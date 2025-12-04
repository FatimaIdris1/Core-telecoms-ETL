{{ config(
    materialized = 'table',
    alias = 'dim_agents'
) }}

SELECT
    id AS agent_id,
    name AS agent_name,
    experience,
    state
FROM CORETELECOM_DB.CORETELECOM_STAGING.AGENTS
