"""
Pre-deployment validation — secrets + service-principal preflight.

    python pipelines/validate_deployment.py [-t dev|test|prod]

Checks (never prints a secret VALUE — names/references only):
  * every required source credential is referenced by NAME and resolves to a
    secret *reference* (``{{secrets/scope/key}}``), not an inline value,
  * the bundle's test/prod jobs run as the intended ETL service principal,
  * the bundle declares the secret scope + required job parameters.

Required Key Vault permissions for the deploy/runtime identities are documented
in docs/DATABRICKS_DEPLOYMENT.md. Exits 0 on success, 1 on any failure.
"""
from __future__ import annotations

import json
import os
import sys

import yaml

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))

from payments_platform.config import secrets as SEC               # noqa: E402

BUNDLE = os.path.join(_ROOT, "databricks.yml")

# The source credentials the ingestors require (logical name -> KV key).
REQUIRED_CREDENTIALS = list(SEC.SECRET_KEYS)

REQUIRED_JOB_PARAMS = ["env", "catalog", "run_date", "run_id", "load_mode",
                       "backfill_start_date", "backfill_end_date"]

ETL_SP_VAR = "${var.etl_service_principal}"


def _load_bundle():
    with open(BUNDLE, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def check_secret_references():
    """Resolve every required credential to a secret REFERENCE; assert none is an
    inline value. Returns the list of secret names (safe to print)."""
    names, problems = [], []
    for logical in REQUIRED_CREDENTIALS:
        ref = SEC.get_secret(logical)              # never a real value
        if not SEC.is_reference(ref):
            problems.append("%s did not resolve to a secret reference" % logical)
        names.append(SEC.SECRET_KEYS[logical])
    return names, problems


def check_service_principal(bundle):
    """test + prod jobs must run as the ETL service principal."""
    problems = []
    targets = bundle.get("targets", {})
    for env in ("test", "prod"):
        run_as = (targets.get(env) or {}).get("run_as") or {}
        sp = run_as.get("service_principal_name")
        if sp != ETL_SP_VAR:
            problems.append("target %s run_as is %r (expected %s)"
                            % (env, sp, ETL_SP_VAR))
    # the variable must default to the intended ETL principal
    var_default = ((bundle.get("variables", {})
                    .get("etl_service_principal") or {}).get("default"))
    if var_default != "spn_investsphere_etl":
        problems.append("etl_service_principal default is %r" % var_default)
    return problems


def check_secret_scope_and_params(bundle):
    """The daily job must declare the secret scope param indirectly via the
    secret_scope variable and carry all required job parameters."""
    problems = []
    if "secret_scope" not in bundle.get("variables", {}):
        problems.append("bundle is missing the secret_scope variable")
    job = bundle["resources"]["jobs"]["investsphere_payments_daily_e2e"]
    params = {p["name"] for p in job.get("parameters", [])}
    missing = [p for p in REQUIRED_JOB_PARAMS if p not in params]
    if missing:
        problems.append("daily job missing parameters: %s" % missing)
    return problems


def validate():
    bundle = _load_bundle()
    secret_names, sec_problems = check_secret_references()
    problems = sec_problems
    problems += check_service_principal(bundle)
    problems += check_secret_scope_and_params(bundle)
    return {
        "secret_names": secret_names,          # NAMES only — never values
        "secret_scope": bundle["variables"]["secret_scope"]["default"],
        "etl_service_principal_var": ETL_SP_VAR,
        "problems": problems,
        "passed": not problems,
    }


def main():
    result = validate()
    # print names only; explicitly no values
    print(json.dumps({
        "passed": result["passed"],
        "secret_scope": result["secret_scope"],
        "required_secret_names": result["secret_names"],
        "problems": result["problems"],
    }, indent=2))
    if not result["passed"]:
        sys.exit(1)
    print("\nPREFLIGHT PASSED — secret references + ETL service principal validated "
          "(no secret values printed).")


if __name__ == "__main__":
    main()
