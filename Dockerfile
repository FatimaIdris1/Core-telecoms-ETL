# Base image with Airflow + Python
FROM apache/airflow:2.9.0-python3.10

# Set Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Switch to root to install packages
USER root

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy entire project into container
COPY . /opt/project
WORKDIR /opt/project

# Copy Airflow DAGs and plugins to Airflow directory
COPY airflow/dags /opt/airflow/dags
COPY airflow/dags/plugins /opt/airflow/plugins

# Switch back to airflow user
USER airflow
