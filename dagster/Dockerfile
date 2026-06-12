FROM python:3.11-slim

WORKDIR /opt/dagster/app

# System deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]" 2>/dev/null || pip install --no-cache-dir -e .

COPY . .

# Dagster home (stores run storage when using filesystem backend)
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

COPY dagster.yaml $DAGSTER_HOME/dagster.yaml
