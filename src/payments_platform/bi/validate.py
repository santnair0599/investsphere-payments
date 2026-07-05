"""
BI semantic-model validation (policy-as-code).

Each function returns a list of violation strings (empty = pass), so tests assert
the real model is clean AND each check trips on a deliberately broken model — the
same pattern as :mod:`governance.validate`.

Guards:
  * measures / dimensions reference a column that exists in their dataset,
  * no BI dataset exposes a raw (unmasked) PII column,
  * every BI dataset sources a mart / masked view / non-PII fact — never a PII
    base table,
  * RLS region columns line up with the governance region row filter.
"""
from __future__ import annotations

from payments_platform.governance import policy as P
from payments_platform.bi import semantic as S

# PII base tables (schema.name) — forbidden as a BI dataset source.
PII_BASE_OBJECTS = {"%s.%s" % (t["schema"], t["name"]) for t in P.pii_tables()}

ANALYST_SAFE_GROUPS = {"analysts", "pii_approved_users"}


def measures_reference_valid_columns(datasets, measures):
    by_name = {d["name"]: d for d in datasets}
    out = []
    for m in measures:
        ds = by_name.get(m["dataset"])
        if ds is None:
            out.append("measure %s -> unknown dataset %s" % (m["name"], m["dataset"]))
        elif m["column"] not in ds["columns"]:
            out.append("measure %s -> column %s not in dataset %s"
                       % (m["name"], m["column"], m["dataset"]))
    return out


def dimensions_reference_valid_columns(datasets, dimensions):
    by_name = {d["name"]: d for d in datasets}
    out = []
    for dim in dimensions:
        ds = by_name.get(dim["dataset"])
        if ds is None:
            out.append("dimension %s -> unknown dataset %s" % (dim["name"], dim["dataset"]))
        elif dim["column"] not in ds["columns"]:
            out.append("dimension %s -> column %s not in dataset %s"
                       % (dim["name"], dim["column"], dim["dataset"]))
    return out


def no_raw_pii_exposed(datasets):
    """A raw PII column may appear in a dataset ONLY if it is display-masked."""
    out = []
    for d in datasets:
        masked = set(d.get("masked_columns", []))
        for col in d["columns"]:
            if col in S.RAW_PII_COLUMNS and col not in masked:
                out.append("dataset %s exposes raw PII column %s"
                           % (d["name"], col))
    return out


def sources_are_analyst_safe(datasets):
    """No BI dataset may source a PII base table (must be mart/masked/fact)."""
    out = []
    for d in datasets:
        if d["source"] in PII_BASE_OBJECTS:
            out.append("dataset %s sources PII base table %s"
                       % (d["name"], d["source"]))
        bad = [g for g in d["audience"] if g not in ANALYST_SAFE_GROUPS]
        if bad:
            out.append("dataset %s has non-analyst audience %s" % (d["name"], bad))
    return out


def rls_aligned(datasets, rls):
    """RLS region columns must match the governance region row-filter columns."""
    out = []
    region = S.REGION_COLUMNS
    for d in datasets:
        rc = d.get("rls_column")
        if rc is not None and rc not in region:
            out.append("dataset %s rls_column %s is not a governance region column"
                       % (d["name"], rc))
    if set(rls.get("region_columns", [])) != region:
        out.append("RLS region_columns %s != governance region columns %s"
                   % (rls.get("region_columns"), sorted(region)))
    if rls.get("scoped_group") in rls.get("global_groups", []):
        out.append("RLS scoped_group is also a global (bypass) group")
    return out


def validate_all(datasets=None, measures=None, dimensions=None, rls=None):
    datasets = S.DATASETS if datasets is None else datasets
    measures = S.MEASURES if measures is None else measures
    dimensions = S.DIMENSIONS if dimensions is None else dimensions
    rls = S.RLS if rls is None else rls
    out = []
    out += measures_reference_valid_columns(datasets, measures)
    out += dimensions_reference_valid_columns(datasets, dimensions)
    out += no_raw_pii_exposed(datasets)
    out += sources_are_analyst_safe(datasets)
    out += rls_aligned(datasets, rls)
    return out
