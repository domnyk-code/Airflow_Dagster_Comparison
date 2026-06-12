# Dockerfile
# ──────────────────────────────────────────────────────
# Extends the official Airflow image with the extra packages
# needed for this project (Cosmos, dbt-postgres, pandas, etc.)
#
# Build context: airflow/
# Built automatically by docker compose (see docker-compose.yml)

FROM apache/airflow:2.9.1-python3.11

# Switch to root only for any system-level deps, then back to airflow user
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Back to the unprivileged airflow user — pip must run as this user
USER airflow

# Copy and install Python requirements.
COPY requirements.txt /requirements.txt
RUN pip install protobuf==4.25.3
RUN pip install --no-cache-dir dbt-core
RUN pip install --no-cache-dir dbt-postgres
RUN pip install --no-cache-dir astronomer-cosmos
RUN pip install --no-cache-dir -r /requirements.txt
