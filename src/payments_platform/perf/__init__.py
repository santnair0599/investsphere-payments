"""
Performance, cost & scalability validation (reference).

A deterministic synthetic-data generator (:mod:`synth`), a benchmark runner that
times each medallion stage and writes monitoring rows (:mod:`benchmark`), a
cost-observability reference that estimates spend from duration + compute type
(:mod:`cost`), table-health checks (:mod:`table_health`), and an optimization
recommendation catalogue (:mod:`recommendations`).

Everything is pure-Python and deterministic (seeded RNG, timestamps passed in) so
the *correctness* of scalability behaviour is testable; the *timings* are
informational (tests never assert wall-clock values). See docs/PERFORMANCE_COST.md
for how each local benchmark maps to Databricks system tables / billing / OPTIMIZE
/ Liquid Clustering / serverless.
"""
