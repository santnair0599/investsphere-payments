-- Live hallucination / groundedness rollups over SAMPLED live evaluations.
-- Requires LIVE_GROUNDEDNESS_ENABLED=true at runtime (populates groundedness_score,
-- hallucination_flag, evaluation_mode='live_sampled' on ai_control.agent_runs).
-- {{catalog}} is substituted by the deploy job. Run on a Databricks SQL Warehouse.

-- 1. Overall: live hallucination rate + average groundedness
CREATE OR REPLACE VIEW {{catalog}}.ai_control.v_hallucination_overall AS
SELECT
    COUNT(*)                                                   AS sampled_runs,
    ROUND(AVG(groundedness_score), 3)                         AS avg_groundedness,
    SUM(CASE WHEN hallucination_flag THEN 1 ELSE 0 END)       AS hallucinations,
    ROUND(AVG(CASE WHEN hallucination_flag THEN 1.0 ELSE 0.0 END), 4) AS hallucination_rate
FROM {{catalog}}.ai_control.agent_runs
WHERE evaluation_mode = 'live_sampled' AND groundedness_score IS NOT NULL;

-- 2. By question type (simple vs complex — derived from the model-router decision)
CREATE OR REPLACE VIEW {{catalog}}.ai_control.v_hallucination_by_type AS
SELECT
    CASE WHEN routing_reason LIKE 'simple%'  THEN 'simple'
         WHEN routing_reason LIKE 'complex%' THEN 'complex'
         ELSE 'default' END                                   AS question_type,
    COUNT(*)                                                   AS sampled_runs,
    ROUND(AVG(groundedness_score), 3)                         AS avg_groundedness,
    ROUND(AVG(CASE WHEN hallucination_flag THEN 1.0 ELSE 0.0 END), 4) AS hallucination_rate
FROM {{catalog}}.ai_control.agent_runs
WHERE evaluation_mode = 'live_sampled' AND groundedness_score IS NOT NULL
GROUP BY 1
ORDER BY hallucination_rate DESC;

-- 3. By model + routing_reason (does the fast/reasoning route hallucinate more?)
CREATE OR REPLACE VIEW {{catalog}}.ai_control.v_hallucination_by_model AS
SELECT
    model,
    routing_reason,
    COUNT(*)                                                   AS sampled_runs,
    ROUND(AVG(groundedness_score), 3)                         AS avg_groundedness,
    ROUND(AVG(CASE WHEN hallucination_flag THEN 1.0 ELSE 0.0 END), 4) AS hallucination_rate
FROM {{catalog}}.ai_control.agent_runs
WHERE evaluation_mode = 'live_sampled' AND groundedness_score IS NOT NULL
GROUP BY model, routing_reason
ORDER BY hallucination_rate DESC;

-- Example ad-hoc: recent hallucinations to inspect
-- SELECT created_at, model, routing_reason, groundedness_score, question, answer
-- FROM {{catalog}}.ai_control.agent_runs
-- WHERE hallucination_flag = true ORDER BY created_at DESC LIMIT 50;
