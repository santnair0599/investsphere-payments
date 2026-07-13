# InvestSphere — Enterprise Business AI Decision Agent (Dubai Holding-style pivot)

> **Status:** design contract for the pivot from the payments-native lakehouse to a
> diversified holding-company lakehouse + Azure GenAI decision agent.
> Every later phase implements against this document.

## 1. Project story (memorize this)

InvestSphere is a **diversified investment holding company** with **real estate, hospitality,
entertainment, and investment** assets. A Databricks Lakehouse consolidates operational and
financial data across these domains through **six enterprise ingestion patterns**. On top of the
governed Gold layer, an **Azure AI Foundry / LangGraph agent** gives leadership grounded business
recommendations — it does not guess: it calls SQL tools over the marts, RAG's over business
policies, checks pipeline/data-quality trust, and returns structured recommendations with a
confidence level and supporting evidence.

**Positioning:** *Data + AI Platform Engineer / Azure AI Agent Engineer.*

## 2. What is preserved vs changed

| Preserved (engineering foundation — DO NOT break the patterns) | Changed (business content) |
|---|---|
| 6 **ingestion patterns**: Autoloader files, SFTP batch, REST API, JDBC, Salesforce SaaS, Debezium CDC | The **payload/domain** each pattern carries |
| Silver: parse → **DQ gate** → **quarantine (Failed Record Register)** → dedup → **MERGE** | Conformed table schemas (domain columns) |
| **SCD2** history (`effective_from/to`, `is_current`, `is_deleted`) | SCD2 subject: customer/**guest** master |
| `silver_control.*` audit (pipeline_run / task_run / table_load_status), gates, PARTIAL logic | — (reused verbatim by `gold_ops_trust`) |
| Unity Catalog governance, masked views, grants generator | Grants extended to new schemas |
| Monitoring, orchestration DAG, dbt tests, CI | Extended to new tables |

> The Python package stays `payments_platform` internally (renaming 50+ modules is high-churn,
> low-signal). The **story, tables, dbt models, docs, and bundle** all present the diversified
> enterprise. This is a deliberate, documented trade-off.

## 3. Six-source → domain mapping (the crux)

The six patterns are *how data arrives*; the domains are *what it is about*. We keep all six
patterns and repoint their payloads.

| # | Ingestion pattern | New source system | Bronze tables | Feeds Gold domain |
|---|---|---|---|---|
| 1 | **JDBC** (Oracle) | Real-estate PMS | `bronze.oracle_properties`, `oracle_leases`, `oracle_occupancy_daily`, `oracle_maintenance_orders` | `gold_realestate` |
| 2 | **JDBC** (SQL Server) | Investment / treasury system | `bronze.sqlserver_assets`, `sqlserver_asset_performance`, `sqlserver_risk_exposure`, `sqlserver_cashflow` | `gold_investment` |
| 3 | **REST API** | Hospitality booking platform + FX | `bronze.rest_hotel_bookings`, `rest_hotel_revenue`, `rest_fx_rates` | `gold_hospitality` (+ FX for investment valuation) |
| 4 | **SFTP** batch | Entertainment ticketing vendor | `bronze.sftp_ticket_sales`, `sftp_footfall` | `gold_entertainment` |
| 5 | **Autoloader** file drop | Marketing/campaign exports | `bronze.campaign_file` → `bronze.campaign_roi` | `gold_entertainment.fact_campaign_roi` |
| 6 | **Salesforce** SaaS | Enterprise CRM | `bronze.sfdc_account` (segment), `sfdc_contact` (customer/guest), `sfdc_opportunity` (deal pipeline), `sfdc_case` (guest review/sentiment) | `gold_customer` + `gold_hospitality.fact_guest_review` |
| — | **Debezium CDC** | Customer/guest master DB | `bronze.customer_cdc` → `silver_cdc.customer_scd2` | shared `dim_customer` / `dim_guest` (SCD2) |

`gold_ops_trust` needs **no new source** — it is derived from the existing `silver_control.*`,
quarantine, and gate-history tables.

## 4. Target Gold layer (build these — do not overbuild)

```
gold_realestate     dim_property, fact_occupancy, fact_lease, fact_maintenance,
                    mart_property_underperformance
gold_hospitality    dim_hotel, fact_booking, fact_revenue, fact_guest_review,
                    mart_hotel_revenue_risk
gold_entertainment  dim_venue, fact_ticket_sales, fact_footfall, fact_campaign_roi,
                    mart_venue_conversion_risk
gold_investment     dim_asset, fact_asset_performance, fact_risk_exposure, fact_cashflow,
                    mart_investment_risk
gold_customer       dim_customer_segment, fact_customer_activity, fact_customer_sentiment,
                    mart_declining_customer_segments
gold_ops_trust      mart_pipeline_status, mart_dq_gate_status, mart_source_freshness,
                    mart_business_recommendation_trust
```

Each `mart_*` is the business-question answer surface the agent's SQL tools hit.

## 5. Business questions the agent answers

| Question | Mart | Tool |
|---|---|---|
| Which real-estate assets are underperforming? | `mart_property_underperformance` | `get_underperforming_properties(period)` |
| Which hotels have revenue risk? | `mart_hotel_revenue_risk` | `get_hotel_revenue_risk(period)` |
| Which venues have high footfall but low conversion? | `mart_venue_conversion_risk` | `get_venue_conversion_risk(period)` |
| Which investment assets have rising risk exposure? | `mart_investment_risk` | `get_investment_risk_exposure(period)` |
| Which customer segments are declining? | `mart_declining_customer_segments` | `get_declining_customer_segments(period)` |
| What are the top business actions this week? | (aggregator) | `get_top_business_actions(period)` |
| Why did the pipeline become PARTIAL? / Can we trust today's numbers? | `gold_ops_trust.*` | `get_pipeline_status(run_date)`, `get_data_quality_trust_score(run_date)` |
| Policy/KPI grounding | Azure AI Search index | `search_policy_docs(question)` |

## 6. Two deployment planes

```
Databricks plane (Asset Bundles + dbt + Terraform + Unity Catalog)
  6 sources → Bronze → Silver (DQ/SCD2/quarantine/control) → Gold marts → SQL Warehouse views

Azure GenAI plane (GitHub Actions + Bicep/Terraform + azd)
  Azure AI Foundry + Azure OpenAI + Azure AI Search + Container Apps (FastAPI/LangGraph)
  + Key Vault + App Insights + eval gate
        │  read-only, service principal in Key Vault, UC masked views
        ▼
  Databricks SQL Warehouse  ──►  Gold marts + silver_control trust tables
```

The agent **queries governed outputs**; it never runs the pipeline.

## 7. Trust / production layer (the differentiator)

- **Guardrails:** no raw PII (UC masked views), tool-argument validation, bounded SQL result size,
  policy citations required, read-only tools by default, **human approval for write/action tools**,
  prompt-injection check on uploaded docs.
- **Observability (`ai_control.*` Delta):** `agent_runs`, `agent_tool_calls`, `agent_evaluations`,
  `prompt_versions` — capture question, answer, tools_called, retrieved_docs, latency, tokens,
  cost, groundedness_score, safety_status.
- **Eval set:** 30 questions (10 KPI, 10 policy/RAG, 5 tool-calling, 5 safety/PII).
- **CI/CD eval gate:** block deploy if `groundedness < 0.80`, `tool_selection_accuracy < 0.85`,
  `PII_leakage > 0`, or `avg_latency_ms > threshold`.

## 8. Phase plan

1. **P1** — repoint 6 source configs + seed data ← *this is where implementation starts*
2. **P2** — adapt Bronze (config-driven; minimal code)
3. **P3** — Silver conformers per domain (reuse DQ/quarantine/SCD2)
4. **P4** — Gold marts (dbt, 6 schemas) + tests
5. **P5** — bundle jobs, governance grants, monitoring, docs
6. **P6** — GenAI plane (RAG, tools, LangGraph/Foundry agent, evals, guardrails, observability)
7. **P7** — CI/CD eval gate + Azure IaC + interview story

## 9. Non-goals (do not build now)

Fine-tuning, deep-learning training, computer vision, speech bots, generic HR/PDF chatbot,
additional ingestion sources, heavy front-end, forecasting models.
