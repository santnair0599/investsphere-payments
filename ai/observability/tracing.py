"""
OpenTelemetry → Azure Application Insights tracing for the agent runtime.

Feature-flagged and dependency-optional: this module imports cleanly with NO
``azure-monitor-opentelemetry`` / ``opentelemetry`` installed and NO Azure creds.
Distributed tracing here *complements* the Delta trace written by
``ai.observability.recorder`` (which stays the durable, queryable record of runs) —
it adds live spans/latency to Application Insights when the platform is configured.

Enabled only when BOTH are true:
  * ``TRACING_ENABLED`` is truthy (1/true/yes/on)
  * ``APPLICATIONINSIGHTS_CONNECTION_STRING`` is set

Public surface (safe to call unconditionally — no-ops when disabled/missing):
  * ``is_enabled() -> bool``
  * ``span(name, **attributes)`` — context manager yielding a span-like object
  * ``set_attributes(span, **attrs)`` — safe attribute setter
"""
from __future__ import annotations

import contextlib
import os
import time

# Module-level state — configuration happens at most once (idempotent).
_configured = False          # True once configure_azure_monitor has run
_tracer = None               # opentelemetry Tracer, or None when disabled/missing
_setup_attempted = False     # guard so a failed setup is not retried every call


def _flag_on() -> bool:
    return os.environ.get("TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}


def _has_conn_str() -> bool:
    return bool(os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING", "").strip())


def _setup() -> None:
    """Configure Azure Monitor + acquire a tracer, at most once.

    Imports both packages LAZILY here so the module loads without them. Any failure
    (missing package, bad creds) degrades to a no-op tracer — tracing must never break
    the request path.
    """
    global _configured, _tracer, _setup_attempted
    if _setup_attempted:
        return
    _setup_attempted = True

    if not (_flag_on() and _has_conn_str()):
        return
    try:
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry import trace

        if not _configured:
            configure_azure_monitor(
                connection_string=os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"],
            )
            _configured = True
        _tracer = trace.get_tracer("investsphere.agent")
    except Exception:
        # Package missing or misconfigured → stay disabled, never raise.
        _tracer = None


def is_enabled() -> bool:
    """True only when the flag + conn string are set AND the SDK configured a tracer."""
    _setup()
    return _tracer is not None


class _NoopSpan:
    """Span-like object used when tracing is disabled/unavailable. All methods no-op."""

    def set_attribute(self, key, value):  # noqa: D401 - mirror the OTel span API
        return None

    def set_attributes(self, attrs):
        return None

    def record_exception(self, exc):
        return None

    def set_status(self, status):
        return None


_NOOP_SPAN = _NoopSpan()


def set_attributes(span, **attrs) -> None:
    """Set multiple attributes on a span, safely no-op'ing on the dummy span / errors."""
    if span is None:
        return
    setter = getattr(span, "set_attribute", None)
    if setter is None:
        return
    for key, value in attrs.items():
        if value is None:
            continue
        try:
            setter(key, value)
        except Exception:
            pass


@contextlib.contextmanager
def span(name: str, **attributes):
    """Start a span ``name`` with ``attributes``; record latency; on exception set the
    span status to error + record the exception, then re-raise. Yields a span-like
    object exposing ``.set_attribute(k, v)``.

    When tracing is disabled OR the SDK is missing this is a no-op context manager that
    yields a dummy span — so callers can use it unconditionally. Never raises from the
    tracing machinery itself (only genuine exceptions from the wrapped block propagate).
    """
    if not is_enabled() or _tracer is None:
        yield _NOOP_SPAN
        return

    t0 = time.time()
    try:
        from opentelemetry.trace import Status, StatusCode
    except Exception:
        # SDK vanished mid-flight — degrade to no-op rather than break the caller.
        yield _NOOP_SPAN
        return

    cm = None
    try:
        cm = _tracer.start_as_current_span(name)
        otel_span = cm.__enter__()
    except Exception:
        # Failed to open a real span → no-op, do not break the request path.
        yield _NOOP_SPAN
        return

    try:
        set_attributes(otel_span, **attributes)
        yield otel_span
    except Exception as exc:
        try:
            otel_span.record_exception(exc)
            otel_span.set_status(Status(StatusCode.ERROR, str(exc)))
        except Exception:
            pass
        raise
    finally:
        try:
            otel_span.set_attribute("latency_ms", int((time.time() - t0) * 1000))
        except Exception:
            pass
        try:
            cm.__exit__(None, None, None)
        except Exception:
            pass
