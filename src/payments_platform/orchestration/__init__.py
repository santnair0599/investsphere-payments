"""
Orchestration as policy-as-code.

The end-to-end workflow is a declarative DAG (:mod:`dag`) executed by a local
runner (:mod:`runner`) that honours dependencies and gate outcomes. The same
graph is mirrored 1:1 in ``databricks.yml`` as a Lakeflow Job, so the orchestration
logic (parallel Bronze, validation gates blocking downstream, ordering) is proven
by tests locally before it ever runs in the workspace.
"""
