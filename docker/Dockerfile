# Use the official Apache Airflow image (Airflow 3.1.3)
FROM apache/airflow:3.1.3


# Set ARGs for versions
ARG TERRAFORM_VERSION=1.14.0
ARG DBT_CORE_VERSION=1.10.13
ARG DBT_SNOWFLAKE_VERSION=1.10.3


USER root


# Install system deps required by terraform, snowflake connector and build tools
RUN apt-get update \
&& apt-get install -y --no-install-recommends \
wget \
unzip \
git \
build-essential \
libssl-dev \
libffi-dev \
libpq-dev \
ca-certificates \
locales \
&& rm -rf /var/lib/apt/lists/*


# Install Terraform
RUN wget --progress=dot:giga https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip -O /tmp/terraform.zip \
&& unzip /tmp/terraform.zip -d /usr/local/bin \
&& chmod +x /usr/local/bin/terraform \
&& rm /tmp/terraform.zip


# Upgrade pip and install python deps: airflow extras already present in base image
RUN pip install --no-cache-dir --upgrade pip setuptools


# Copy your airflow requirements and install them (will include boto3, snowflake connector, pyarrow, etc.)
COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt


# Install dbt and dbt adapters
RUN pip install --no-cache-dir dbt-core==${DBT_CORE_VERSION} dbt-snowflake==${DBT_SNOWFLAKE_VERSION}


# Copy the rest of the repo into the image
# Keep repo at /opt/coretelecom (non-root for Airflow best practices is 'airflow' user)
COPY . /opt/coretelecom
WORKDIR /opt/coretelecom


# Make terraform and helper scripts executable
RUN chmod +x /opt/coretelecom/docker/terraform-run.sh || true


# Switch back to airflow user
USER airflow


# Expose Airflow UI
EXPOSE 8080