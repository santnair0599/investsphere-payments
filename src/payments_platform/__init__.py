"""Payments lakehouse platform — reusable, testable reference library.

Layout mirrors the medallion flow:
  common/   -> hashing and shared helpers
  config/   -> run context, audit columns, control-table records
  bronze/   -> raw ingestion (files, CDC) with audit + corrupt capture
  silver/   -> parse, DQ, quarantine, dedup, CDC/SCD2 apply
"""
