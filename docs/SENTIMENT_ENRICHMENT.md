# Sentiment Enrichment (Azure AI Language)

Turns customer/guest sentiment from a **source-provided label** into a **computed signal**.

## Before → after
- **Before:** the `sentiment` column (`POSITIVE`/`NEUTRAL`/`NEGATIVE`) was assumed to arrive from
  the CRM/review source; the marts (`fact_customer_sentiment`, `mart_hotel_revenue_risk`,
  `mart_declining_customer_segments`) only **aggregated** it.
- **After:** `ai/enrichment/sentiment.py` **derives** that label from **raw review/case text** with
  Azure AI Language, writing the same column the marts already read — so nothing downstream changes.

## How it works (mirrors the other Azure integrations)
- **Azure-native when configured:** Azure AI Language `analyze_sentiment`, enabled when
  `SENTIMENT_ENRICHMENT_ENABLED=true` **and** `AZURE_LANGUAGE_ENDPOINT` is set (auth via
  `AZURE_LANGUAGE_KEY` or managed identity). Batches ≤10 docs/request; maps `mixed`→`NEUTRAL`.
- **Deterministic fallback:** a small lexicon classifier runs when the flag is off, the SDK is
  missing, or the service errors — so the module imports and runs with **no SDK and no
  credentials** (local/CI and on Spark workers without creds), and a value is **always** produced.
  `classify()` never raises.

## API
```python
from ai.enrichment import sentiment
sentiment.classify("The room was spotless and staff were friendly")   # {'sentiment':'POSITIVE','confidence':1.0,'source':'lexicon'}
sentiment.classify_batch([...])                                        # list of the same
sentiment.azure_enabled()                                             # False unless flag+endpoint set
```

## Populate the column on Databricks
```python
from ai.enrichment.sentiment import enrich_table
enrich_table(catalog="investsphere_dev",
             table="silver_hospitality.guest_review_clean",
             text_col="review_text",     # the raw text to classify
             out_col="sentiment")        # + sentiment_confidence, sentiment_source
```
A `pandas_udf` batches rows to `classify_batch` per partition — Azure Language when the cluster has
the flag + creds, else the lexicon fallback on the workers. It overwrites the table with the
enriched `sentiment` (+ confidence + source) columns; the existing marts then aggregate the
**computed** value with no change.

> **Requires a text column.** Enrichment needs raw review text (`text_col`). The synthetic demo ships
> `sentiment` labels directly, so add a `review_text` column upstream (Bronze `sfdc_case`) to exercise
> real classification; without it `enrich_table` raises a clear error.

## Governance
Raw review text is PII-adjacent: store only the **derived label + confidence + source**, keep raw
text access restricted (masked/steward-only), and apply the same guardrails as elsewhere. The label
values are unchanged, so Unity Catalog grants and the marts need no change.

## Config
`SENTIMENT_ENRICHMENT_ENABLED` · `AZURE_LANGUAGE_ENDPOINT` · `AZURE_LANGUAGE_KEY` (see `ai/.env.example`).
Defaults OFF → deterministic lexicon, so nothing here is required for local/CI.

## Status
Module + fallback implemented and verified offline. Live Azure classification and populating a real
`review_text` column require Azure AI Language credentials and a text column in the source.
