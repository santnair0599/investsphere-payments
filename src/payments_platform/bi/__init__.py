"""
BI / Power BI consumption layer (semantic model as policy-as-code).

A declarative semantic model (:mod:`semantic`) — analyst-safe datasets, business
measures/KPIs, dimensions, and row-level-security mapping — plus a generator
(:mod:`sql_generator`) that emits catalog-parameterised BI **serving views**, and
:mod:`validate` security/consistency checks.

The BI layer never reads raw PII: every dataset sources a Gold **mart**, a
governance **masked view**, or the **non-PII fact** — so Power BI inherits Unity
Catalog masking + the region row filter for the ``analysts`` group automatically.
See docs/POWER_BI.md.
"""
from payments_platform.bi import semantic, validate, sql_generator  # noqa: F401
