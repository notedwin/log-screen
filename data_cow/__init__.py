from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets
from .resources import PostgresResource, RemoteSQLiteResource

all_assets = load_assets_from_modules([assets])

logs = define_asset_job("logs", selection=AssetSelection.keys("CaddyLogsParsing").downstream())
personal = define_asset_job("personal", selection=AssetSelection.keys("st2Postgres", "aw2Postgres"))

logs_schedule = ScheduleDefinition(
    job=logs,
    cron_schedule="0 10,22 * * *",
)

personal_schedule = ScheduleDefinition(
    job=personal,
    cron_schedule="0 17 * * *",
)

defs = Definitions(
    assets=all_assets,
    schedules=[logs_schedule, personal_schedule],
    resources={
        "postgres": PostgresResource(),
        "screentime": RemoteSQLiteResource(remote_path="/Users/edwinzamudio/Library/Application Support/Knowledge/knowledgeC.db"),
        "activitywatch": RemoteSQLiteResource(remote_path="/Users/edwinzamudio/Library/Application Support/activitywatch/aw-server/peewee-sqlite.v2.db"),
    },
)
