{{ config(materialized='view', alias = 'staging_customers') }}

SELECT *
FROM CORETELECOM_DB.CORETELECOM_STAGING.CUSTOMERS
