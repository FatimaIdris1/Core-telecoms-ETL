# Coretelecoms ETL Data Pipeline

## Overview

CoreTelecoms, a major telecom provider in the United States, is facing increasing customer churn due to unresolved and poorly managed complaints. The organization receives thousands of complaints daily from multiple sources such as call logs, social media, website forms, and customer care centers. However, due to inconsistent formats, siloed teams, and manual reporting processes, insights are delayed and customer retention continues to decline.

This project implements a unified data engineering pipeline that automates ingestion, storage, and processing of all customer complaint sources into Snowflake, enabling analytics and reporting from a single trusted dataset. The pipeline eliminates manual bottlenecks and creates a foundation for real-time monitoring, customer service optimization, and churn reduction.

---

## Data Sources

| Source                  | Description                                                                     | Format        | Location           | Frequency |
| ----------------------- | ------------------------------------------------------------------------------- | ------------- | ------------------ | --------- |
| Customers               | Customer information: ID, name, contact, location, etc.                         | CSV           | AWS S3             | Static    |
| Agents                  | Customer care agent lookup table                                                | Google Sheets | Google Drive       | Static    |
| Call Center Logs        | Daily logs with complaint type, agent handling, resolution status, and duration | CSV           | AWS S3             | Daily     |
| Social Media            | Online complaints containing customer/agent-related metadata                    | JSON          | AWS S3             | Daily     |
| Website Complaint Forms | Customer-submitted forms containing complaint records                           | Table         | AWS Postgres (RDS) | Daily     |

### Processing Behavior

Daily datasets uploaded to S3 may contain multiple files per source. Files are merged using append logic and then loaded into Snowflake. Static dataset sources (Google Sheets and Customers CSV) are loaded once via dedicated tasks in Airflow.

---

## Technologies Used

### Cloud and Storage

* AWS S3
* AWS IAM
* AWS SSM Parameter Store
* AWS Postgres (Transactional DB)

### Data Warehouse

* Snowflake

### Data Orchestration

* Apache Airflow

### Infrastructure Automation

* Terraform

### Programming and Libraries

* Python
* boto3
* pandas
* pyarrow
* snowflake-connector-python

---

## Pipeline Features

* Automatic folder-level processing of S3 data
* Daily scheduled ingestion orchestrated with Airflow
* Incremental loading and prevention of duplicate loads
* Append logic for merging daily batch files
* Multi-source extraction (S3, Google Sheets, Postgres)
* Automated table creation in Snowflake
* Schema inference for dynamic loading
* Fully automated IAM and Snowflake configuration with Terraform
* Logging and monitoring using Airflow
* Email notifications for pipeline success and failure using Gmail SMTP

---

## Architecture Summary

The pipeline currently follows the sequence below:

1. Terraform provisions AWS infrastructure including:

   * IAM roles
   * S3 buckets
   * Security policies
   * Snowflake storage integration, database, and schema
2. Airflow orchestrates ETL tasks:

   * Extracts data from S3, Google Sheets, and Postgres
   * Applies incremental append logic
   * Detects and skips previously processed files
   * Loads curated data into Snowflake staging
   * Sends email notifications on failure or pipeline completion
3. dbt transforms raw data into facts and dimensions for analytics:

   * Generates Star Schema in Snowflake
   * Provides clean, analysis-ready tables

---

### Architecture Diagram

```mermaid
flowchart LR
    %% Define styles
    classDef aws fill:#FFFAE3,stroke:#F7C948,stroke-width:2px,color:#333;
    classDef airflow fill:#E3F2FF,stroke:#3399FF,stroke-width:2px,color:#333;
    classDef dbt fill:#E6FFE6,stroke:#33CC33,stroke-width:2px,color:#333;

    %% AWS Infrastructure
    subgraph Terraform_AWS["Terraform + AWS Infrastructure"]
        class Terraform_AWS aws
        IAM[IAM Roles]:::aws
        S3_Raw[S3 Bucket: Raw Data]:::aws
        S3_Curated[S3 Bucket: Curated Data]:::aws
        Security[Security Policies]:::aws
        Snowflake[Snowflake DB & Schema]:::aws
    end

    %% Airflow Orchestration
    subgraph Airflow["Airflow Orchestration"]
        class Airflow airflow
        Extract[Extract Data from S3, Google Sheets, Postgres]:::airflow
        Incremental[Apply Incremental Append Logic]:::airflow
        Skip[Skip Previously Processed Files]:::airflow
        Load[Load Curated Data into Snowflake Staging]:::airflow
        Email[Send Email Notifications]:::airflow
    end

    %% dbt Transformations
    subgraph dbt["dbt Data Transformations"]
        class dbt dbt
        Transform[Transform Staged Data]:::dbt
        StarSchema[Generate Star Schema in Snowflake]:::dbt
        Analytics[Analytics-Ready Tables]:::dbt
    end

    %% Connections (Left-to-Right)
    IAM --> S3_Raw --> Extract
    IAM --> S3_Curated --> Extract
    IAM --> Snowflake
    Extract --> Incremental --> Skip --> Load --> Transform --> StarSchema --> Analytics
    Load --> Email
```

---

## Snowflake Configuration

Deployed through Terraform:

* **Database:** `CORETELECOM_DB`
* **Schema:** `CORETELECOM_STAGING`
* **Warehouse:** `COMPUTE_WH`
* **Storage Integration:** `integration`
* **AWS IAM Role:** `snowflake_user_role`

All Snowflake stages, roles, and schema configurations are automatically generated by Terraform within `aws_infrastructure/snowflake.tf`.

---

## Airflow Data Pipeline

### DAG Name

`core_etl_pipeline`

### Primary Tasks

```
single_load
incremental_load
transformations
snowflake_load
```

### Email Alerts

Airflow DAGs are configured to send email notifications using Gmail SMTP:
For the failure of the dag at any point an email will be sent with the failure logs, while after a successful run, at the end of the process, an email will be sent to confirm the success

* **Failure Notification**

```python
from airflow.providers.smtp.operators.smtp import EmailOperator

failure_email = EmailOperator(
    task_id='notify_failure',
    to='your_email@gmail.com',
    subject='CoreTelecoms ETL Pipeline — FAILED',
    html_content='The ETL pipeline has failed. Please check the Airflow logs for details.'
)
```

* **Success Notification**

```python
success_email = EmailOperator(
    task_id='notify_success',
    to='your_email@gmail.com',
    subject='CoreTelecoms ETL Pipeline — SUCCESS',
    html_content='All pipeline tasks completed successfully.'
)
```

SMTP configuration in `airflow.cfg` or via Airflow connections:

```
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = your_email@gmail.com
smtp_password = your_app_password
smtp_port = 587
smtp_mail_from = your_email@gmail.com
```

---

### Directory Structure

```
Airflow/ dags/
│  └ pipeline.py
│
│── plugins/
│   ├── google_sheet_load.py
│   ├── customer_load.py
│   ├── postgres_extract.py
│   ├── s3_incremental_copy.py
│   ├── s3_to_snowflake.py
│   └── utilities.py
```

---

## Terraform Infrastructure Layout

```
aws_infrastructure/
│── main.tf
│── iam.tf
│── s3_buckets.tf
│── secret_manager.tf
│── security_groups.tf
│── subnets.tf
│── subnet_groups.tf
│── snowflake.tf
```

This provisions the entire environment including networking, IAM roles, secret storage, S3 buckets, Postgres access, and Snowflake cloud resources.

---

## dbt Models (Facts and Dimensions)

The analytics layer transforms raw data from Snowflake staging into clean, analysis-ready tables under `CORETELECOM_TRANSFORMED`.

### Fact Tables

| File                               | Resulting Table                | Description                                                              |
| ---------------------------------- | ------------------------------ | ------------------------------------------------------------------------ |
| `fact_call_center_logs.sql`        | `fact_call_center_logs`        | Phone-based customer complaints including agent handling and resolution. |
| `fact_social_media_complaints.sql` | `fact_social_media_complaints` | Complaints from social platforms with sentiment and user details.        |
| `fact_web_complaints.sql`          | `fact_web_complaints`          | Complaints submitted via website forms stored in Postgres.               |

### Dimension Tables

| File                | Resulting Table | Description                                                |
| ------------------- | --------------- | ---------------------------------------------------------- |
| `dim_customers.sql` | `dim_customers` | Standardized customer demographics and identifiers.        |
| `dim_agents.sql`    | `dim_agents`    | Standardized customer care agent information and metadata. |

---

## CI/CD Pipeline

This project follows a **DataOps CI/CD approach** for infrastructure, ETL, and analytics models.

| Component                         | CI (Checks Before Merge)                  | CD (Deployment)                           |
| --------------------------------- | ----------------------------------------- | ----------------------------------------- |
| Terraform (AWS + Snowflake Infra) | Validate configuration (`terraform plan`) | Deploy infrastructure (`terraform apply`) |
| Airflow (DAGs + Plugins)          | Lint and test Python DAGs                 | Upload DAGs to MWAA S3 bucket             |
| dbt (Warehouse Models)            | Validate SQL (`dbt test` & `dbt compile`) | Build models in Snowflake (`dbt run`)     |

CI ensures only validated DAGs, Terraform plans, and dbt models are merged. CD deploys infrastructure, runs ETL pipelines, and updates warehouse models automatically.

---

## Docker & Docker Compose Overview

This setup uses a custom Docker image (`coretelecoms-etl:latest`) and docker-compose to provide a full ETL and data workflow environment:

1. **Terraform container**

   * Provisions AWS infrastructure
   * Commands: `terraform plan` and `terraform apply`
2. **Airflow container**

   * Runs DAGs to orchestrate ETL pipelines
   * Sends email notifications
3. **dbt container**

   * Transforms Snowflake staging data into facts and dimensions
   * Commands: `dbt run --project-dir /opt/project/dbt`

Volumes sync local project files with containers to maintain up-to-date code.

---

## Usage

Start services:

```bash
docker-compose up -d
```

Access Airflow UI:

```
http://localhost:8080
```

Run dbt models:

```bash
docker-compose exec dbt dbt run --project-dir /opt/project/dbt
```

Run Terraform commands:

```bash
docker-compose exec terraform terraform plan
docker-compose exec terraform terraform apply
```

---

## Summary
This project delivers a **comprehensive, end-to-end ETL pipeline** for processing telecom customer complaints from multiple sources into a unified Snowflake data warehouse. The system integrates multiple technologies to automate extraction, transformation, and loading of raw data into a structured, analysis-ready format.

Key highlights:

* **Automated Multi-Source Ingestion:** Extracts data from S3, Google Sheets, and Postgres, handling both static and incremental datasets.
* **Incremental and Duplicate-Safe Loading:** Uses append logic and file tracking to prevent redundant loads.
* **Airflow Orchestration:** DAGs handle task dependencies, scheduling, and error handling, including **email notifications** for failures and successful pipeline completion.
* **Terraform Infrastructure Automation:** Automatically provisions AWS resources (IAM roles, S3 buckets, security groups) and Snowflake cloud infrastructure (databases, schemas, storage integration).
* **Snowflake Data Warehouse:** Centralized staging and modeled tables, enabling fast analytics and reporting.
* **dbt Analytics Layer:** Transforms raw staging data into facts and dimensions, following a star schema model for robust analysis.
* **CI/CD Integration:** Validates Terraform, Airflow DAGs, and dbt models before merging, ensuring high-quality, production-ready deployments.
* **Containerized Workflow:** Docker Compose provides a reproducible environment for development, testing, and deployment, keeping Airflow, dbt, and Terraform isolated but synchronized with local code.

The pipeline ensures **scalable, reliable, and traceable data processing**, providing a solid foundation for telecom analytics, operational insights, and customer retention strategies.

---

## Future Improvements

While the current implementation is production-ready, several enhancements can further improve scalability, efficiency, and analytics capabilities:

1. **Enhanced Monitoring & Alerting:**

   * Integrate Airflow with monitoring tools such as **Prometheus**, or **CloudWatch** to provide real-time visibility on DAG performance and resource usage.
   * Include Slack or Teams notifications alongside email for faster incident response.

2. **Data Quality Checks:**

   * Implement automated **data validation** and anomaly detection within the ETL process (e.g., schema validation, null value checks, or outlier detection).

3. **Dynamic Source Handling:**

   * Introduce modular extraction operators to support new sources easily.

4. **Incremental dbt Models:**

   * Implement **incremental models** for large fact tables to improve transformation performance and reduce processing time.

5. **Scalability Improvements:**

   * Split Airflow workloads into multiple workers for parallel execution.
   * Use Snowflake clustering or materialized views to improve query performance on large datasets.

