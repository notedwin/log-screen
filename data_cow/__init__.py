# from dagster import Definitions, EnvVar
# from .assets import fake, postgres_io_manager, read_asset, latest_row

# defs = Definitions(
#     assets=[read_asset, latest_row],
#     jobs=[fake],
#     resources={"pg_io_manager": postgres_io_manager()},
# )

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

all_assets = load_assets_from_modules([assets])

logs = define_asset_job("logs", selection=AssetSelection.all())

logs_schedule = ScheduleDefinition(
    job=logs,
    cron_schedule="0 * * * *",  # every hour
)

defs = Definitions(
    assets=all_assets,
    schedules=[logs_schedule],
    resources={"postgres": assets.PostgresResource()},
)