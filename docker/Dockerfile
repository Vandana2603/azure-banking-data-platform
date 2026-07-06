# =============================================================================
# Dockerfile - local development / CI environment for the
# Azure Banking Data Platform (PySpark + Python 3.10 + OpenJDK for Spark)
# =============================================================================
FROM python:3.10-slim

# Spark needs a JVM
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
        unixodbc-dev \
        gcc \
        g++ && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/raw data/bronze data/silver data/gold logs docs/data_quality_reports

# Default command runs the full local pipeline end to end
CMD ["bash", "docker/run_pipeline.sh"]
