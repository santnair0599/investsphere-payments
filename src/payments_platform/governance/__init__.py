"""
Unity Catalog governance as policy-as-code.

The governance posture (PII classification, groups, least-privilege grants,
column masks, row filters, masked views, quarantine lockdown) is declared once
in :mod:`policy`, then:

  * :mod:`sql_generator` emits Databricks-ready SQL from the model, and
  * :mod:`validate` runs security checks the test-suite asserts on.

So the same model that *produces* the production SQL is the model the security
tests *prove correct* — governance can't silently drift from its tests.
"""
