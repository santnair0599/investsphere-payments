"""
Real Databricks (PySpark) execution modules.

These are the production counterparts to the pure-Python reference logic — the
code that actually runs on the Lakeflow job. Each module lazily imports
``pyspark`` **inside** its ``run()`` function, so the package imports cleanly in
CI without Spark (the unit tests assert the module contract, not Spark behaviour).

They reuse the tested library where the logic is portable (allowed-value sets,
audit-column + record-hash contract) so the Spark jobs and the local reference
can't drift. See docs/AZURE_IMPLEMENTATION.md for the step-by-step deployment.
"""
