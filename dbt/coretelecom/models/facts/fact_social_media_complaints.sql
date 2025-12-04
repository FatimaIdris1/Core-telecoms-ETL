{{ config(materialized='table', alias = 'fact_social_media_complaint') }}

SELECT
    s.complaint_id,
    s.customer_id,
    s.complaint_category,
    s.agent_id,
    s.resolution_status,
    s.request_date,
    s.resolution_date,
    s.media_channel,
    s.media_complaint_generation_date
FROM CORETELECOM_DB.CORETELECOM_STAGING.SOCIAL_MEDIA_COMPLAINT s
INNER JOIN {{ ref('dim_customers') }} c
    ON s.customer_id = c.customer_id
INNER JOIN {{ ref('dim_agents') }} a
    ON s.agent_id = a.agent_id
