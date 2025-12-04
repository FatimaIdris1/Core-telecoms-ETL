{{ config(materialized='view', alias = 'staging_agents') }}

SELECT *
FROM CORETELECOM_DB.CORETELECOM_STAGING.AGENTS
