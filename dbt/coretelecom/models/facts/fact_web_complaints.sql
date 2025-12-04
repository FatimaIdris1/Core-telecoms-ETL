{{ config(materialized='table', alias = 'fact_web_complaint') }}

SELECT
    w.column_1,
    w.request_id,
    w.customer_id,
    w.complaint_category,
    w.agent_id,
    w.resolution_status,
    w.request_date,
    w.resolution_date,
    w.web_form_generation_date
FROM CORETELECOM_DB.CORETELECOM_STAGING.WEB_CUSTOMER_COMPLAINTS w
INNER JOIN {{ ref('dim_customers') }} c
    ON w.customer_id = c.customer_id
INNER JOIN {{ ref('dim_agents') }} a
    ON w.agent_id = a.agent_id
