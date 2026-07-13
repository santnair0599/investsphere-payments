"""
Sentiment enrichment — derive the POSITIVE/NEUTRAL/NEGATIVE label the marts consume
from RAW review/case text, instead of assuming it from the source.

Turns `gold_customer.fact_customer_sentiment` / `mart_hotel_revenue_risk` from
"aggregates a source-provided label" into "aggregates a *computed* signal".

Design (mirrors the other Azure integrations):
  * Azure-native when configured: **Azure AI Language** `analyze_sentiment`, enabled when
    `SENTIMENT_ENRICHMENT_ENABLED` is truthy AND `AZURE_LANGUAGE_ENDPOINT` is set
    (auth via `AZURE_LANGUAGE_KEY` or `DefaultAzureCredential`).
  * Deterministic fallback otherwise: a small lexicon classifier — so this module
    imports and runs with **no `azure-ai-textanalytics` SDK and no credentials**
    (used in local/CI and on Spark workers without creds), and a value is ALWAYS
    produced. Never raises.

Labels match the values the marts already expect: POSITIVE / NEUTRAL / NEGATIVE.
"""
from __future__ import annotations

import os
import re

LABELS = ("POSITIVE", "NEUTRAL", "NEGATIVE")

# Deterministic lexicon fallback — no deps, no creds. Domain-flavoured (hospitality/CRM).
_POS = {"great", "excellent", "amazing", "clean", "friendly", "comfortable", "recommend",
        "love", "loved", "perfect", "wonderful", "helpful", "spotless", "fantastic",
        "good", "enjoyed", "attentive", "welcoming", "value"}
_NEG = {"dirty", "rude", "broken", "poor", "bad", "terrible", "awful", "cold", "noisy",
        "slow", "disappointing", "worst", "unhelpful", "delay", "delayed", "overpriced",
        "smell", "smelly", "complaint", "unclean", "waited", "refused"}

_WORD = re.compile(r"[a-z']+")


def _truthy(v) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on")


def azure_enabled() -> bool:
    """True only when the flag is set AND the Language endpoint is configured."""
    return _truthy(os.environ.get("SENTIMENT_ENRICHMENT_ENABLED", "")) and bool(
        os.environ.get("AZURE_LANGUAGE_ENDPOINT"))


def _lexicon(text: str) -> dict:
    words = set(_WORD.findall((text or "").lower()))
    pos, neg = len(words & _POS), len(words & _NEG)
    if pos > neg:
        label = "POSITIVE"
    elif neg > pos:
        label = "NEGATIVE"
    else:
        label = "NEUTRAL"
    total = pos + neg
    confidence = round(abs(pos - neg) / total, 3) if total else 0.5
    return {"sentiment": label, "confidence": confidence, "source": "lexicon"}


def _client():
    """Azure AI Language client (lazy import; key or managed identity)."""
    from azure.ai.textanalytics import TextAnalyticsClient
    endpoint = os.environ["AZURE_LANGUAGE_ENDPOINT"]
    key = os.environ.get("AZURE_LANGUAGE_KEY")
    if key:
        from azure.core.credentials import AzureKeyCredential
        credential = AzureKeyCredential(key)
    else:
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
    return TextAnalyticsClient(endpoint=endpoint, credential=credential)


def classify_batch(texts) -> list[dict]:
    """Classify a list of texts → list of {sentiment, confidence, source}.

    Uses Azure AI Language when enabled; falls back to the lexicon per-document on any
    error or when disabled. Never raises.
    """
    texts = [("" if t is None else str(t)) for t in texts]
    if not azure_enabled():
        return [_lexicon(t) for t in texts]
    try:
        client = _client()
        out: list[dict] = []
        for i in range(0, len(texts), 10):           # Azure Language: <=10 docs/request
            chunk = texts[i:i + 10]
            for j, r in enumerate(client.analyze_sentiment(documents=chunk)):
                if getattr(r, "is_error", False):
                    out.append(_lexicon(chunk[j]))
                    continue
                label = r.sentiment.upper()          # positive/neutral/negative/mixed
                if label == "MIXED":
                    label = "NEUTRAL"
                if label not in LABELS:
                    label = "NEUTRAL"
                cs = r.confidence_scores
                confidence = round(max(cs.positive, cs.neutral, cs.negative), 3)
                out.append({"sentiment": label, "confidence": confidence,
                            "source": "azure_language"})
        return out
    except Exception:                                # any SDK/network/auth error → fallback
        return [_lexicon(t) for t in texts]


def classify(text: str) -> dict:
    """Classify a single text → {sentiment, confidence, source}."""
    return classify_batch([text])[0]


# ---- Databricks: populate the sentiment column from a text column -----------
def enrich_table(catalog: str,
                 table: str = "silver_hospitality.guest_review_clean",
                 text_col: str = "review_text",
                 out_col: str = "sentiment") -> str:
    """Read ``{catalog}.{table}``, classify ``text_col``, and write ``out_col`` (+
    ``sentiment_confidence`` / ``sentiment_source``) back. Runs on Databricks (Spark).

    A ``pandas_udf`` batches rows to ``classify_batch`` per partition — Azure Language
    when the cluster has the flag + creds, else the lexicon fallback on the workers.
    ``pyspark`` is imported lazily so this module imports without Spark.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    import pandas as pd

    spark = SparkSession.builder.getOrCreate()
    full = f"{catalog}.{table}"
    df = spark.table(full)
    if text_col not in df.columns:
        raise ValueError(
            f"{full} has no '{text_col}' column to classify — sentiment enrichment needs "
            f"raw review text. Add a text column upstream (Bronze), then re-run.")

    schema = StructType([
        StructField("sentiment", StringType()),
        StructField("confidence", DoubleType()),
        StructField("source", StringType()),
    ])

    @pandas_udf(schema)
    def _sentiment_udf(texts: "pd.Series") -> "pd.DataFrame":
        return pd.DataFrame(classify_batch(texts.fillna("").tolist()))

    enriched = (df
                .withColumn("_s", _sentiment_udf(F.col(text_col)))
                .withColumn(out_col, F.col("_s.sentiment"))
                .withColumn("sentiment_confidence", F.col("_s.confidence"))
                .withColumn("sentiment_source", F.col("_s.source"))
                .drop("_s"))
    (enriched.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(full))
    print(f"sentiment enrichment complete → {full} (out_col={out_col})")
    return full
