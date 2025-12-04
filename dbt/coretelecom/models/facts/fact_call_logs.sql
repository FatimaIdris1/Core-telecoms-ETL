{{ config(materialized='table', alias = 'fact_call_logs') }}

SELECT
    cl.call_id,
    cl.customer_id,
    cl.agent_id,
    cl.complaint_category,
    cl.call_start_time,
    cl.call_end_time,
    cl.resolution_status,
    cl.call_logs_generation_date
FROM CORETELECOM_DB.CORETELECOM_STAGING.CALL_LOGS_CLEANED cl
INNER JOIN {{ ref('dim_customers') }} c
    ON cl.customer_id = c.customer_id
INNER JOIN {{ ref('dim_agents') }} a
    ON cl.agent_id = a.agent_id
