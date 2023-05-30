from dagster import (
    Config,
    Definitions,
    RunConfig,
    in_process_executor,
    job,
    op,
    get_dagster_logger,
    EnvVar,
    IOManager,
    io_manager
)
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.types import Integer
import os


class PostgresIOManager(IOManager):
    """IOManager for postgres databases."""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self._engine = create_engine(self.db_url)
        self._inspector = inspect(self._engine)

    def load_input(self, context) -> pd.DataFrame:
        """Load the contents of a table as a pandas DataFrame."""
        table_name = context.sql
        return pd.read_sql_query(table_name, self._engine)

    def handle_output(self, context, obj):
        table_name = context.name
        schema = context.schema
        if isinstance(obj, pd.DataFrame):
            return obj.to_sql(table_name, self._engine, dtype=schema, if_exists="append", index=False)
        elif obj is None:
            # dbt has already written the data to this table
            pass
        else:
            raise ValueError(f"Unsupported object type {type(obj)} for PGIOManager.")


@io_manager(config_schema={"db_url": str})
def postgres_io_manager(context):
    return PostgresIOManager(context.resource_config["db_url"])


class TableConfig(Config):
    number: int


@op
def get_last_row(context, config: TableConfig) -> pd.DataFrame:
    """Get the last row processed from metadata table corresponding to the table_name"""
    get_dagster_logger().info(f"number: {config.number}")
    return pd.DataFrame({"num_row": [config.number]})


default_config = RunConfig(
    ops={"get_last_row": TableConfig(number=2)}
)

@job(config=default_config, resource_defs={"postgres_io_manager": postgres_io_manager})
def fake():
    get_last_row().persist_to_table("postgres_io_manager", sql="yes")

class PostgresIOManager(IOManager):
    """IOManager for postgres databases.
    """

    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        # logs the url
        # get_dagster_logger().error(self.db_url)
        self._engine = create_engine(self.db_url)
        self._inspector = inspect(self._engine)

    def load_input(self) -> pd.DataFrame:
        """Load the contents of a table as a pandas DataFrame."""
        sql = context.sql
        return pd.read_sql_query(sql, self._engine)

    def handle_output(self,context, obj):
        get_dagster_logger().error(f"context: {context.config}")
        if isinstance(obj, pd.DataFrame):
            tbl = context.config.get("table","didntwork")
            rows = obj.to_sql(tbl, self._engine, if_exists="append", index=False)
            return AssetMaterialization(asset_key=tbl, description=f"Inserted {rows} rows")
        elif obj is None:
            # dbt has already written the data to this table
            pass
        else:
            raise ValueError(f"Unsupported object type {type(obj)} for PGIOManager.")


@io_manager
def postgres_io_manager():
    return PostgresIOManager()


def get_metadata(
	name: str,
	non_argument_deps: set[str] = {},
	io_manager_key: str = "io_manager",
) -> AssetsDefinition:
	"""Factory for creating assets that run SQL statements."""

	@op(required_resource_keys={io_manager_key})
	def get_last_row(context) -> pandas.DataFrame:
		"""Get the last row processed from metadata table corresponding to the table_name"""
		df = context.resources.io_manager_key.load_input()
		context.log.info(f"df: {df}")
		return df

	return get_last_row