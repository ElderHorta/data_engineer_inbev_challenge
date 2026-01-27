# Dockerfile for Airflow with PySpark support
FROM apache/airflow:2.8.0-python3.11

USER root

# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Create directories for data and logs
RUN mkdir -p /data/bronze /data/silver /data/gold /logs && \
    chown -R airflow:root /data /logs && \
    chmod -R 775 /data /logs

USER airflow

# Copy requirements and install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy source code
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dags/ /opt/airflow/dags/

# Add src to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Healthcheck
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8080/health || exit 1

WORKDIR /opt/airflow
