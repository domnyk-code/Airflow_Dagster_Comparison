from __future__ import annotations

import os
from pathlib import Path

from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

import dagster as dg


DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_PATH"))

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project)

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="silver gold",
    name="medallion_dbt_assets",
    dagster_dbt_translator=None,
)
def medallion_dbt_assets(
    context: dg.AssetExecutionContext,
    dbt: DbtCliResource,
):
    yield from dbt.cli(["build"], context=context).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="bronze",
    name="bronze_dbt_models",
)
def bronze_dbt_models(
    context: dg.AssetExecutionContext,
    dbt: DbtCliResource,
):
    """DBT bronze models — run after Python ingestion asset."""
    yield from dbt.cli(["build", "--select", "bronze"], context=context).stream()
