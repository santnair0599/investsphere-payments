-- Agent observability tables (Delta, Unity Catalog). Every agent request is
-- traced here so the GenAI system is monitorable like any production service and
-- the CI/CD eval gate has data to score against.
--   Run on a Databricks SQL Warehouse / notebook against the target catalog.
-- {{catalog}} is substituted by the deploy job (investsphere_dev/test/prod).

CREATE SCHEMA IF NOT EXISTS {{catalog}}.ai_control;

-- One row per agent question/answer turn.
CREATE TABLE IF NOT EXISTS {{catalog}}.ai_control.agent_runs (
    run_id            STRING,
    session_id        STRING,
    user_id           STRING,
    question          STRING,
    answer            STRING,
    domain            STRING,          -- real_estate/hospitality/... or 'cross_domain'
    tools_called      ARRAY<STRING>,
    retrieved_docs    ARRAY<STRING>,   -- policy doc ids cited via RAG
    trust_level       STRING,          -- HIGH/MEDIUM/LOW from the trust mart
    confidence_score  DOUBLE,
    groundedness_score DOUBLE,         -- populated for live-sampled runs (else NULL)
    hallucination_flag BOOLEAN,        -- groundedness_score < threshold on a sampled run
    evaluation_mode   STRING,          -- 'live_sampled' when scored live; NULL otherwise
    safety_status     STRING,          -- PASS / BLOCKED_PII / BLOCKED_INJECTION
    latency_ms        BIGINT,
    tokens_in         BIGINT,
    tokens_out        BIGINT,
    estimated_cost    DECIMAL(10,5),
    prompt_version    STRING,
    model             STRING,          -- the (final) deployment used; may differ per run w/ the model router
    routing_reason    STRING,          -- model-router decision, e.g. simple_lookup->fast / complex_synthesis->reasoning
    created_at        TIMESTAMP
) USING DELTA;

-- One row per tool invocation (fan-out of agent_runs).
CREATE TABLE IF NOT EXISTS {{catalog}}.ai_control.agent_tool_calls (
    run_id            STRING,
    tool_name         STRING,
    arguments         STRING,          -- JSON
    row_count         INT,
    mart              STRING,
    success           BOOLEAN,
    error_message     STRING,
    latency_ms        BIGINT,
    created_at        TIMESTAMP
) USING DELTA;

-- One row per eval question per eval run (feeds the CI/CD gate).
CREATE TABLE IF NOT EXISTS {{catalog}}.ai_control.agent_evaluations (
    eval_run_id            STRING,
    prompt_version         STRING,
    question_id            STRING,
    category               STRING,     -- kpi / policy / tool / safety
    tool_selection_correct BOOLEAN,
    groundedness           DOUBLE,
    answer_relevance       DOUBLE,
    business_action_quality DOUBLE,
    pii_leaked             BOOLEAN,
    latency_ms             BIGINT,
    passed                 BOOLEAN,
    created_at             TIMESTAMP
) USING DELTA;

-- Prompt/config version registry (canary + rollback + audit).
CREATE TABLE IF NOT EXISTS {{catalog}}.ai_control.prompt_versions (
    prompt_version    STRING,
    system_prompt     STRING,
    model             STRING,
    tool_allowlist    ARRAY<STRING>,
    created_by        STRING,
    is_active         BOOLEAN,
    created_at        TIMESTAMP
) USING DELTA;
