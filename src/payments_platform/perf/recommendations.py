"""
Optimization recommendation catalogue.

A declarative set of recommendations, each with a predicate over a *workload
profile*; :func:`recommend` returns the ones that apply. The full catalogue maps
the platform's design choices to the Databricks features that make them perform
and cost-efficiently at scale (see docs/PERFORMANCE_COST.md).
"""
from __future__ import annotations

# id -> {title, feature, rationale, applies(profile) -> bool}
RECOMMENDATIONS = [
    {"id": "auto_loader_for_files",
     "title": "Use Auto Loader for file ingestion",
     "feature": "Databricks Auto Loader (cloudFiles)",
     "rationale": "Incremental file discovery + schema evolution + _rescued_data "
                  "instead of re-listing/re-reading the whole landing zone.",
     "applies": lambda p: p.get("has_file_source", False)},

    {"id": "incremental_over_full_reload",
     "title": "Incremental / CDC extraction instead of full reload",
     "feature": "watermark-driven incremental + AUTO CDC",
     "rationale": "Read only changed rows (watermark / SystemModstamp) instead of "
                  "re-scanning the whole source every run.",
     "applies": lambda p: p.get("has_full_loads", False) or p.get("has_cdc", False)},

    {"id": "dedup_before_merge",
     "title": "Deduplicate before MERGE",
     "feature": "row_number() window / dropDuplicates before Delta MERGE",
     "rationale": "Collapsing duplicate keys before MERGE avoids non-deterministic "
                  "merges and wasted write amplification.",
     "applies": lambda p: p.get("has_duplicates", False)},

    {"id": "hash_change_detection",
     "title": "Hash-based change detection in SCD",
     "feature": "sha2() record hash compare in AUTO CDC / MERGE",
     "rationale": "Only expire-and-insert when the tracked-column hash actually "
                  "changes, so unchanged updates don't rewrite history.",
     "applies": lambda p: p.get("has_cdc", False)},

    {"id": "liquid_clustering_gold",
     "title": "Liquid Clustering / CLUSTER BY AUTO for large Gold tables",
     "feature": "Liquid Clustering (CLUSTER BY [AUTO])",
     "rationale": "Self-tuning clustering on high-cardinality filter/join keys "
                  "beats static partitioning and avoids small-file skew at scale.",
     "applies": lambda p: p.get("large_gold_tables", False)},

    {"id": "predictive_optimization",
     "title": "Predictive Optimization for managed tables",
     "feature": "Predictive Optimization (auto OPTIMIZE/VACUUM/ANALYZE)",
     "rationale": "Let UC schedule OPTIMIZE/VACUUM/ANALYZE on managed tables "
                  "instead of hand-tuned maintenance jobs.",
     "applies": lambda p: p.get("managed_tables", True)},

    {"id": "job_serverless_compute",
     "title": "Job / serverless compute instead of all-purpose clusters",
     "feature": "serverless jobs / job clusters",
     "rationale": "All-purpose clusters are the most expensive per DBU and stay "
                  "warm; job/serverless compute is cheaper and ephemeral.",
     "applies": lambda p: p.get("uses_all_purpose", False)},

    {"id": "autotermination_cost_tags",
     "title": "Auto-termination + cost tags",
     "feature": "cluster policy autotermination + custom_tags",
     "rationale": "Bounded idle time and project/env/cost_center tags make spend "
                  "attributable and stop runaway idle clusters.",
     "applies": lambda p: True},
]

RECOMMENDATION_IDS = [r["id"] for r in RECOMMENDATIONS]


def recommend(profile):
    """Return the applicable recommendations for a workload ``profile`` dict."""
    out = []
    for rec in RECOMMENDATIONS:
        if rec["applies"](profile):
            out.append({k: rec[k] for k in ("id", "title", "feature", "rationale")})
    return out


def recommend_all():
    """The full catalogue (every recommendation, predicates stripped)."""
    return [{k: r[k] for k in ("id", "title", "feature", "rationale")}
            for r in RECOMMENDATIONS]


def profile_from_run(*, has_file_source=True, has_full_loads=True,
                     has_cdc=True, has_duplicates=True, large_gold_tables=True,
                     uses_all_purpose=False, managed_tables=True):
    """Convenience builder for a workload profile (defaults match this platform:
    file + JDBC full/incremental + CDC + dedup + managed Gold; serverless jobs)."""
    return {
        "has_file_source": has_file_source,
        "has_full_loads": has_full_loads,
        "has_cdc": has_cdc,
        "has_duplicates": has_duplicates,
        "large_gold_tables": large_gold_tables,
        "uses_all_purpose": uses_all_purpose,
        "managed_tables": managed_tables,
    }
