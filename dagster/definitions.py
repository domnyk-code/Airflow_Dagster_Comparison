
import os

import dagster as dg
from dagster_dbt import DbtCliResource

from medallion.assets.bronze import raw_orders
from medallion.assets.dbt_assets import (
    bronze_dbt_models,
    medallion_dbt_assets,
    dbt_project,
)

all_assets = [
    raw_orders,           # the raw data
    bronze_dbt_models,    # transferring raw data into bronze layer
    medallion_dbt_assets, # the actual pipeline bronze > silver > gold
]

resources = {
    "dbt": DbtCliResource(
        project_dir=dbt_project,
        target="dev",
    ),
}

full_pipeline_job = dg.define_asset_job(
    name="full_medallion_pipeline",
    selection=dg.AssetSelection.all(),
    description="Materialise every layer: bronze > silver > gold",
)

daily_schedule = dg.ScheduleDefinition(
    name="medallion_daily",
    job=full_pipeline_job,
    cron_schedule="0 0 * * *",   # midnight UTC
)

@dg.sensor(
    job=full_pipeline_job,
    minimum_interval_seconds=30,
    description="Triggers the pipeline when a new orders CSV is detected",
)
def new_file_sensor(context: dg.SensorEvaluationContext):
    data_dir = os.environ.get("SOURCE_DATA_PATH", "/opt/dagster/data/")
    import pathlib

    path = pathlib.Path(data_dir)
    if not path.exists():
        return dg.SkipReason(f"File not found: {path}")

    stat = path.stat()
    fingerprint = f"{stat.st_mtime}:{stat.st_size}"
    cursor = context.cursor

    if cursor == fingerprint:
        return dg.SkipReason("File unchanged since last run")

    context.update_cursor(fingerprint)
    context.log.info("New file detected — triggering pipeline")
    return dg.RunRequest(
        run_key=fingerprint,
        run_config={},
    )

defs = dg.Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[full_pipeline_job],
    schedules=[daily_schedule],
    sensors=[new_file_sensor],
)
