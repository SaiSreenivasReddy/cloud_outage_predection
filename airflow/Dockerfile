FROM apache/airflow:3.0.0-python3.12

USER root
# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy init script
COPY airflow/init.sh /entrypoints/init.sh
RUN chmod +x /entrypoints/init.sh

USER airflow

# Install DAG-specific Python packages inside the Airflow image
COPY dags/requirements.txt /tmp/dags-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dags-requirements.txt
