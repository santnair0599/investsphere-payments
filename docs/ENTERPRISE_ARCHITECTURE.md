# InvestSphere — Enterprise Architecture (diagrams)

Diagrammatic view of the diversified-enterprise lakehouse + Azure AI decision agent.
Renders on GitHub / any Mermaid viewer. See `docs/ENTERPRISE_PIVOT.md` for the design
contract and `docs/GENAI_INTERVIEW_STORY.md` for the narrative.

## 1. System architecture — two planes

Data plane (Databricks) produces governed marts; the GenAI plane (Azure) queries them
through a read-only, PII-masked boundary and never runs the pipeline.

```mermaid
flowchart TB
  subgraph DBX["Databricks Data Plane"]
    direction TB
    subgraph SRC["Six ingestion patterns"]
      direction LR
      S1["JDBC · Oracle<br/>Real-estate PMS"]
      S2["JDBC · SQL Server<br/>Treasury"]
      S3["REST<br/>Booking + FX"]
      S4["SFTP<br/>Ticketing"]
      S5["Autoloader<br/>Campaign"]
      S6["Salesforce + Debezium CDC<br/>CRM + guest master"]
    end
    BRZ["Bronze — raw Delta<br/>audit cols · corrupt capture · run_id"]
    SLV["Silver — conform · DQ gate · quarantine<br/>SCD2 history · control tables"]
    subgraph GOLD["Gold — dbt · 6 domains"]
      direction LR
      G1["gold_realestate"]
      G2["gold_hospitality"]
      G3["gold_entertainment"]
      G4["gold_investment"]
      G5["gold_customer"]
      G6["gold_ops_trust<br/>trust_level + confidence"]
    end
    WH["Databricks SQL Warehouse<br/>governed · read-only · UC masked views"]
    SRC --> BRZ --> SLV --> GOLD --> WH
  end

  WH -. "read-only SP · Key Vault · masked views" .-> AGENT

  subgraph AZ["Azure GenAI Plane"]
    direction TB
    AGENT["Enterprise Decision Agent<br/>LangGraph / FastAPI · Azure OpenAI · Foundry twin"]
    TOOLS["8 SQL tools<br/>read-only over marts"]
    RAG["Azure AI Search<br/>hybrid + semantic RAG"]
    GUARD["Guardrails<br/>PII · injection · approval"]
    TRUST["Trust gate<br/>get_data_quality_trust_score"]
    OBS["ai_control.*<br/>runs · tool_calls · evals"]
    AGENT --> TOOLS & RAG & GUARD & TRUST & OBS
  end

  TOOLS -. queries .-> WH
  TRUST -. reads .-> G6
  AGENT --> OUT["Leadership<br/>grounded recommendation + confidence + citations"]

  subgraph DEP["Deployment"]
    direction LR
    D1["Asset Bundles · Terraform · dbt"]
    D2["GitHub Actions · Bicep + eval gate"]
  end
  D1 -. deploys .-> DBX
  D2 -. deploys .-> AZ

  classDef data fill:#0e2630,stroke:#46c7d6,color:#dbf3f7;
  classDef ai fill:#2e230e,stroke:#f0a850,color:#fbe9cf;
  classDef trust fill:#123222,stroke:#5ec98a,color:#d7f0e0;
  class S1,S2,S3,S4,S5,S6,BRZ,SLV,G1,G2,G3,G4,G5,WH,OUT data;
  class AGENT,TOOLS,RAG,GUARD,OBS ai;
  class G6,TRUST trust;
```

## 2. Live query — one question, end to end

```mermaid
sequenceDiagram
  actor L as Leadership
  participant API as FastAPI /ask
  participant GD as Guardrails
  participant AG as Agent · Azure OpenAI
  participant T as SQL Tools
  participant R as Azure AI Search
  participant WH as SQL Warehouse (marts)
  participant OB as ai_control.*

  L->>API: "Which hotels have revenue risk?"
  API->>GD: screen input
  GD-->>API: PASS · no injection / action
  API->>AG: question + tool specs
  AG->>T: get_data_quality_trust_score
  T->>WH: SELECT mart_business_recommendation_trust
  WH-->>AG: trust_level=MEDIUM, confidence=0.7
  AG->>T: get_hotel_revenue_risk
  T->>WH: SELECT mart_hotel_revenue_risk WHERE is_revenue_risk
  WH-->>AG: rows + risk_reasons
  AG->>R: search_policy_docs · RevPAR policy
  R-->>AG: cited chunks
  AG->>GD: scan output for PII
  GD-->>AG: PASS · redacts if needed
  AG-->>API: grounded answer + confidence + citations
  API->>OB: trace run + tool_calls
  API-->>L: recommendation
```

## 3. Data-quality trust roll-up (`gold_ops_trust`)

```mermaid
flowchart LR
  PR["pipeline_run<br/>SUCCESS / PARTIAL / FAILED"] --> TRUST
  DQ["DQ gate passed?"] --> TRUST
  QR["quarantine rate"] --> TRUST
  FR["source freshness vs SLA"] --> TRUST
  TRUST["mart_business_recommendation_trust"] --> LVL["trust_level<br/>HIGH · MEDIUM · LOW"]
  TRUST --> CONF["confidence_score"]
  TRUST --> RSN["trust_reasons<br/>(cited to leadership)"]

  classDef t fill:#123222,stroke:#5ec98a,color:#d7f0e0;
  class TRUST,LVL,CONF,RSN t;
```
