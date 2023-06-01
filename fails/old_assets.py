from dagster import (
	Config,
	Definitions,
	RunConfig,
	in_process_executor,
	job,
	op,
	asset,
	get_dagster_logger,
	IOManager,
	io_manager,
	Output,
	Out,
	AssetMaterialization,
	EnvVar,
	AssetsDefinition,
	SourceAsset,
	AssetKey,
	AssetIn,
	In,
)
from contextlib import contextmanager
import pandas
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.types import BIGINT, DATE
from typing import Iterator, Optional, Sequence, Callable

from dagster import (
	ConfigurableIOManager,
	InputContext,
	OutputContext,
)
import os

# notes 5/29
# right now the probelm is that when I get the source df using SQL sometimes the tables does not exists
# or really just the first time it runs, I can create the table and set the value to 0 for the first run
# what I did before was default to 0 if the table did not exist


@contextmanager
def connect_postgresql(config) -> Iterator:
	url = config["url"]
	conn = None
	try:
		conn = create_engine(url).connect()
		yield conn
	finally:
		if conn:
			conn.close()


class PostgreSQLPandasIOManager(ConfigurableIOManager):
	"""This IOManager will take in a pandas dataframe and store it in postgresql."""

	url: str = os.getenv("DATABASE_URL")

	@property
	def _config(self):
		return self.dict()

	def get_metadata_create_not_exists(self, context):
		tbl_name = context.metadata.get("tbl", None)
		if tbl_name is None:
			raise Exception("tbl_name not provided in metadata")
		latest_row = 0
		with connect_postgresql(config=self._config) as con:
			result = con.execute(
				f"SELECT MAX(num_row) AS num_row FROM {tbl_name}_metadata"
			)
			row = result.fetchone()
			if row is not None and row["num_row"] is not None:
				latest_row = row["num_row"]

		context.log.info(f"Latest row processed for {tbl_name} was: {latest_row}")
		return latest_row

	def update_metadata(self, context, latest_row: int):
		tbl_name = context.metadata.get("table_name", None)
		if tbl_name is None:
			raise Exception("table_name not provided in metadata")

		schema = {"num_row": BIGINT, "date": DATE}
		with connect_postgresql(config=self._config) as con:
			date = pandas.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")
			# insert last row processed into metadata table
			metadata = pandas.DataFrame({"num_row": [latest_row], "date": [date]})
			num_rows = metadata.to_sql(
				f"{tbl_name}_metadata",
				con=con,
				dtype=schema,
				if_exists="append",
				index=False,
				chunksize=1000,
			)
			context.log.info(
				f"Last row processed was: {latest_row}, Inserted {num_rows} rows into metadata table"
			)

	def handle_output(self, context: OutputContext, obj):
		if isinstance(obj, pandas.DataFrame):
			tbl_name = context.metadata.get("table_name", None)
			if tbl_name is None:
				raise Exception("table_name not provided in metadata")
			# schema = context.metadata.get("schema", None)
			# if schema is None:
			# raise Exception("schema not provided in metadata")

			with connect_postgresql(config=self._config) as con:
				rows = obj.to_sql(
					con=con,
					name=tbl_name,
					# dtype=schema,
					if_exists="append",
					chunksize=1000,
				)
				context.log.info(f"Rows added to {tbl_name}: {rows}")
		elif isinstance(obj, int):
			self.update_metadata(context, obj)

		else:
			raise Exception(f"Outputs of type {type(obj)} not supported.")

	def load_input(self, context):
		# context.log.info(f"asset_key: {context.asset_key}")
		# context.log.info(f"metadata: {context.metadata}")

		is_metadata_tbl = context.metadata.get("md_tbl", False)
		with connect_postgresql(config=self._config) as con:
			if is_metadata_tbl:
				return self.get_metadata_create_not_exists(context)

			sql = context.metadata.get("sql", None)
			if sql is None:
				raise Exception("sql not provided in metadata")

			df = pandas.read_sql(
				sql=sql,
				con=con,
			)
			context.log.info(f"df: {df}")
			return df

	def simple_read(self, context):
		with connect_postgresql(config=self._config) as con:
			pass


@io_manager
def postgres_io_manager():
	return PostgreSQLPandasIOManager()


class OpConfig(Config):
	tbl: str


latest_row = SourceAsset(
	"latest_row",
	io_manager_key="io_manager",
	metadata={"tbl": "bruh", "md_tbl": True},
)


@asset(
	# input table name
	# read from metadata table called caddy_fct_metadata
	ins={
		"latest_row": AssetIn(
			asset_key="latest_row",
			metadata={"tbl": "bruh", "md_tbl": True},
		)
	},
	io_manager_key="pg_io_manager",
	# ouput to bruh where there is one column with num_row
	# the schema isnt being used rn
	metadata={"table_name": "bruh"},
)
def read_asset(latest_row):
	get_dagster_logger().info(f"df: {latest_row}")
	# read_sql_query simple
	# raw_data = []
	# json_normalize(raw_data["json_message"])

	# use a graph asset if I am going to use multiple ops or use assets
	# one op to update the metadata table
	# one op to update create the asset

	return latest_row


@op
# ins={
# 	"latest_row": In(
# 		asset_key="latest_row",
# 		metadata={"tbl": "bruh", "md_tbl": True},
# 	)
# },
# out=Out(
#     io_manager_key="io_manager",
#     metadata={"table_name": "bruh", "schema": {"num_row": BIGINT}},
# )
def get_latest_row(context, config: OpConfig):
	"""Get the last row processed from metadata table corresponding to the table_name"""
	context.log.info(config)
	tbl_name = config.tbl
	if tbl_name is None:
		raise Exception("tbl_name not provided in metadata")
	latest_row = 0
	with connect_postgresql(config={"url": os.getenv("DATABASE_URL")}) as con:
		result = con.execute(f"SELECT MAX(num_row) AS num_row FROM {tbl_name}_metadata")
		row = result.fetchone()
		if row is not None and row["num_row"] is not None:
			latest_row = row["num_row"]

	context.log.info(f"Latest row processed for {tbl_name} was: {latest_row}")
	return latest_row

	# return pandas.DataFrame({"num_row": [latest_row]})


@op
def update_latest_row(context, latest_row, config: OpConfig):
	context.log.info(config)
	tbl_name = config.tbl
	schema = {"num_row": BIGINT, "date": DATE}
	with connect_postgresql(config={"url": os.getenv("DATABASE_URL")}) as con:
		date = pandas.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")
		# insert last row processed into metadata table
		metadata = pandas.DataFrame({"num_row": [latest_row], "date": [date]})
		num_rows = metadata.to_sql(
			f"{tbl_name}_metadata",
			con=con,
			dtype=schema,
			if_exists="append",
			index=False,
			chunksize=1000,
		)
		context.log.info(
			f"Last row processed was: {latest_row}, Inserted {num_rows} rows into metadata table"
		)

	context.log.info(f"df: {latest_row}")


default_config = RunConfig(
	ops={
		"get_latest_row": OpConfig(
			tbl="bruh",
		),
		"update_latest_row": OpConfig(
			tbl="bruh",
		),
	}
)


@job(config=default_config)
def fake():
	# source_df
	# get_dagster_logger().info(f"df: {source_df}")
	# sql = metadata_sql("caddy_fct")
	# resource_defs.io_manager.load_input("name", )
	# df = get_metadata("caddy_fct")
	# get_dagster_logger().info(f"df: {df}")
	# log.info(f"df: {df}")
	update_latest_row(get_latest_row())

	#     last_processed_state = get_last_processed_state()  # Implement the logic to fetch the last processed state

	# # Fetch new or modified data from the SQL table based on the last processed state
	# data_to_process = fetch_new_or_modified_data(last_processed_state)

	# # Perform the desired transformations on the data_to_process
	# transformed_data = ...

	# # Update the metadata storage with the new last processed state
	# update_last_processed_state(new_state)  # Implement the logic to update the last processed state

	# # Return the transformed data as the output
	# return transformed_data


# fake.execute_in_process(run_config=RunConfig({""}))
