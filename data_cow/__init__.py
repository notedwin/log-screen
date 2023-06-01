from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets
from .resources import PostgresResource

all_assets = load_assets_from_modules([assets])

logs = define_asset_job("logs", selection=AssetSelection.all())

logs_schedule = ScheduleDefinition(
    job=logs,
    cron_schedule="0 * * * *",  # every hour
)

defs = Definitions(
    assets=all_assets,
    schedules=[logs_schedule],
    resources={"postgres": PostgresResource()},
)