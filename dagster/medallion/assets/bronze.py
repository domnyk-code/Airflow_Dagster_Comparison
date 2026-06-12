from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

import dagster as dg


def _get_engine():
    conn = (
        f"postgresql+psycopg2://"
        f"{os.environ.get('DBT_USER', 'postgres')}:"
        f"{os.environ.get('DBT_PASSWORD', 'postgres')}@"
        f"{os.environ.get('DBT_HOST', 'localhost')}:5432/"
        f"{os.environ.get('DBT_DBNAME', 'medallion_db')}"
    )
    return create_engine(conn)

@dg.asset(
    name="raw_orders",
    key_prefix="bronze",
    group_name="bronze_layer",
    description=(
        "Raw orders loaded from CSV into the raw.orders Postgres table. "
        "Adds ingestion metadata (_ingested_at, _source_file). "
        "This is the entry point of the medallion pipeline."
    ),
)
def raw_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:

    source_path = Path("/opt/dagster/data/yellow_tripdata_2024-03.parquet")

    context.log.info("Reading source file: %s", source_path)
    df = pd.read_parquet(source_path)

    # Medallion convention: add ingestion audit columns
    df["_ingested_at"] = datetime.now()
    df["_source_file"] = str(source_path)

    engine = _get_engine()
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()

    df.to_sql(
        name="taxi_trips",
        schema="raw",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=10_000,
        method="multi",
    )

    rows = len(df)
    context.log.info("Loaded %d rows into raw.taxi_trips", rows)

    return dg.MaterializeResult(
        metadata={
            "rows_loaded":   dg.MetadataValue.int(rows),
            "source_file":   dg.MetadataValue.path(str(source_path)),
            "columns":       dg.MetadataValue.json(list(df.columns)),
            "ingested_at":   dg.MetadataValue.text(str(datetime.utcnow())),
            "preview":       dg.MetadataValue.md(df.head(5).to_markdown()),
        }
    )
