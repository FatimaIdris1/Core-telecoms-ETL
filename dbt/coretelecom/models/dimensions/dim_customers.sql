{{ config(materialized='table',
alias = 'dim_customers') }}

SELECT
    customer_id,
    name AS customer_name,
    gender,
    date_of_birth,
    signup_date,
    email,
    address
FROM CORETELECOM_DB.CORETELECOM_STAGING.CUSTOMERS
