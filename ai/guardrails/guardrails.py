"""
Deterministic guardrails around the agent — enforced in code, NOT left to the LLM.
Layered with Azure AI Content Safety + Prompt Shields (configured in infra); this
module is the always-on local backstop and the thing unit-tested in CI.

  * input:  prompt-injection / jailbreak heuristics on the user turn AND on any
            uploaded document text
  * output: PII leakage scan before an answer is returned to the user
  * action: write/mutating intents are routed to human approval, never auto-run

Azure AI Content Safety **Prompt Shields** is called live on the input path when
configured (env PROMPT_SHIELDS_ENABLED truthy + AZURE_CONTENT_SAFETY_ENDPOINT set);
it runs FIRST and, on a detected jailbreak/prompt-injection, blocks the turn. The
deterministic regex below is the always-on fallback: it backstops Prompt Shields
(fail-open on any SDK/network/auth error) and also covers write-action approval and
PII scanning. Flag OFF (default) => regex only, i.e. identical to prior behavior.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

# ---- PII patterns (masked views should prevent these upstream; this catches leaks)
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE = re.compile(r"\+?\d[\d\s().-]{7,}\d")
_PAN = re.compile(r"\b(?:\d[ -]*?){13,19}\b")  # card-like PANs

# ---- prompt-injection / jailbreak heuristics
# The qualifier run is `*`-quantified on purpose: "ignore all previous instructions" is
# the canonical phrasing and stacks three qualifiers. Matching only one silently lets it
# through.
_QUALIFIER = r"(?:all\s+|any\s+|the\s+|your\s+|my\s+|previous\s+|prior\s+|earlier\s+|above\s+|foregoing\s+)*"
_INJECTION = re.compile(
    r"("
    rf"(?:ignore|disregard|forget|override)\s+{_QUALIFIER}"
    r"(instructions?|rules?|directives?|guardrails?|guidelines?|policy|policies|"
    r"system(?:\s+prompt)?|row\s+filters?|filters?|constraints?)"
    rf"|(?:reveal|print|show|repeat|output|dump|leak|expose)\s+{_QUALIFIER}"
    r"(?:full\s+|entire\s+|raw\s+)*(?:system\s+)?(prompt|instructions?|api\s+keys?|credentials?|secrets?)"
    r"|you are now\b|\bas dan\b|\bdo anything now\b|\bdeveloper mode\b"
    r"|(?:bypass|circumvent|evade|get around|turn off|disable)\s+(?:\w+\s+){0,3}?"
    r"(filters?|mask\w*|guardrails?|governance|rls|permissions?|controls?)"
    r"|\braw\s+pii\b|\bunmasked\b"
    r"|drop table|delete from|update .* set|;--|exfiltrat"
    r")",
    re.IGNORECASE,
)

# ---- write/action intents that require human approval
#
# The verb must be an imperative aimed at the system AND close to its object. The old
# pattern was `verb .* noun` with an unbounded gap, so any read-only question containing
# an action word anywhere and a business noun anywhere later was gated — e.g. "Give me an
# update on which venues have low conversion, and what the campaign playbook recommends"
# matched update…campaign and was refused. Two guards against that:
#   * the article lookbehinds reject noun usage ("an update on", "the transfer")
#   * the bounded gap keeps the verb attached to its object
_ACTION = re.compile(
    r"(?<!an\s)(?<!the\s)\b"
    r"(approve|execute|apply|create|update|delete|increase|reduce|send|issue|"
    r"cancel|refund|charge|transfer|remit|disburse|wire|reallocate)\b"
    r"(?:\s+(?:the|a|an|our|this|that|its))?"     # optional determiner
    r"(?:\s+\w+){0,3}?\s+"                        # bounded gap: verb stays near its object
    r"\b(rent|price|pricing|lease|campaign|asset|booking|action|order|payment|funds|"
    r"budget|account|aed|usd)\b",
    re.IGNORECASE,
)


@dataclass
class GuardResult:
    allowed: bool
    status: str                       # PASS / BLOCKED_PII / BLOCKED_INJECTION / NEEDS_APPROVAL
    reasons: list[str] = field(default_factory=list)
    redacted_text: str | None = None


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _prompt_shield(text: str, is_document: bool) -> tuple[bool, str]:
    """Live Azure AI Content Safety Prompt Shields check.

    Returns (attack_detected, reason). Active only when PROMPT_SHIELDS_ENABLED is
    truthy AND AZURE_CONTENT_SAFETY_ENDPOINT is set; otherwise a no-op so the
    flag-off path is byte-for-byte the prior behavior. Any error (missing SDK,
    auth, network, schema drift) fails OPEN to (False, "") so the deterministic
    regex backstop takes over and the request never crashes.
    """
    if not (_env_truthy("PROMPT_SHIELDS_ENABLED")
            and os.environ.get("AZURE_CONTENT_SAFETY_ENDPOINT")):
        return (False, "")

    endpoint = os.environ["AZURE_CONTENT_SAFETY_ENDPOINT"].rstrip("/")
    try:
        # ---- credential (lazy imports; key first, else managed identity)
        key = os.environ.get("AZURE_CONTENT_SAFETY_KEY")
        if key:
            from azure.core.credentials import AzureKeyCredential
            credential = AzureKeyCredential(key)
        else:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()

        user_prompt = "" if is_document else (text or "")
        documents = [text or ""] if is_document else []

        # ---- preferred path: typed SDK client + shield-prompt operation
        try:
            from azure.ai.contentsafety import ContentSafetyClient
            client = ContentSafetyClient(endpoint, credential)
            options = {"userPrompt": user_prompt, "documents": documents}
            shield = getattr(client, "shield_prompt", None)
            if shield is not None:
                resp = shield(options=options)
                return _parse_shield(resp)
            # Some SDK versions expose the operation via a request body model.
            try:
                from azure.ai.contentsafety.models import ShieldPromptOptions
                resp = client.shield_prompt(  # type: ignore[attr-defined]
                    body=ShieldPromptOptions(user_prompt=user_prompt, documents=documents)
                )
                return _parse_shield(resp)
            except Exception:
                pass
        except Exception:
            pass

        # ---- fallback: direct REST call to the Prompt Shields endpoint
        return _shield_via_rest(endpoint, credential, text or "", is_document)
    except Exception:
        return (False, "")


def _parse_shield(resp) -> tuple[bool, str]:
    """Read attackDetected from an SDK response or a plain dict."""
    def _get(obj, name):
        if isinstance(obj, dict):
            return obj.get(name)
        return getattr(obj, name, None)

    up = _get(resp, "userPromptAnalysis") or _get(resp, "user_prompt_analysis")
    if up is not None and (_get(up, "attackDetected") or _get(up, "attack_detected")):
        return (True, "Azure Prompt Shields: jailbreak/prompt-injection detected")

    docs = _get(resp, "documentsAnalysis") or _get(resp, "documents_analysis") or []
    for d in docs:
        if _get(d, "attackDetected") or _get(d, "attack_detected"):
            return (True, "Azure Prompt Shields: jailbreak/prompt-injection detected")
    return (False, "")


def _shield_via_rest(endpoint, credential, text: str, is_document: bool) -> tuple[bool, str]:
    import json
    import urllib.request

    url = f"{endpoint}/contentsafety/text:shieldPrompt?api-version=2024-09-01"
    body = {"documents": [text]} if is_document else {"userPrompt": text}
    headers = {"Content-Type": "application/json"}

    # AzureKeyCredential exposes .key; token credentials expose .get_token().
    key = getattr(credential, "key", None)
    if key:
        headers["Ocp-Apim-Subscription-Key"] = key
    else:
        token = credential.get_token("https://cognitiveservices.azure.com/.default")
        headers["Authorization"] = f"Bearer {token.token}"

    req = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310 (trusted endpoint)
        payload = json.loads(r.read().decode("utf-8"))
    return _parse_shield(payload)


def check_input(text: str, is_uploaded_doc: bool = False) -> GuardResult:
    """Screen a user turn (or uploaded document) before it reaches the model/tools."""
    reasons = []
    attack, shield_reason = _prompt_shield(text or "", is_uploaded_doc)
    if attack:
        return GuardResult(False, "BLOCKED_INJECTION", [shield_reason])
    if _INJECTION.search(text or ""):
        reasons.append("prompt-injection / jailbreak pattern detected"
                       + (" in uploaded document" if is_uploaded_doc else ""))
        return GuardResult(False, "BLOCKED_INJECTION", reasons)
    if _ACTION.search(text or ""):
        reasons.append("request implies a write/business action — human approval required")
        return GuardResult(False, "NEEDS_APPROVAL", reasons)
    return GuardResult(True, "PASS", reasons)


def scan_output(text: str) -> GuardResult:
    """Scan the drafted answer for raw PII before returning it to the user."""
    reasons, redacted = [], text or ""
    if _EMAIL.search(redacted):
        reasons.append("email address present")
        redacted = _EMAIL.sub("[REDACTED_EMAIL]", redacted)
    # PAN before PHONE: the phone pattern also matches a spaced 16-digit card, so running
    # it first would swallow every PAN and mislabel it as a phone number in the audit trail.
    if _PAN.search(redacted):
        reasons.append("card/PAN-like number present")
        redacted = _PAN.sub("[REDACTED_PAN]", redacted)
    if _PHONE.search(redacted):
        reasons.append("phone number present")
        redacted = _PHONE.sub("[REDACTED_PHONE]", redacted)
    if reasons:
        # answers must cite aggregates, not raw identifiers — block and redact
        return GuardResult(False, "BLOCKED_PII", reasons, redacted_text=redacted)
    return GuardResult(True, "PASS", reasons, redacted_text=redacted)


def requires_citation(answer: str, retrieved_docs: list[str]) -> bool:
    """A policy answer must cite at least one retrieved doc (groundedness rule)."""
    return bool(retrieved_docs)
