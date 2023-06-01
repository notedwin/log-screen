from contextlib import contextmanager
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from typing import Iterator, Optional, Sequence

from dagster import (
	ConfigurableIOManager,
	InputContext,
	OutputContext,
)

@contextmanager
def connect_postgresql(config, schema="public") -> Iterator:
	url = config["url"]
	conn = None
	try:
		conn = create_engine(url).connect()
		yield conn
	finally:
		if conn:
			conn.close()

class PostgreSQLPandasIOManager(ConfigurableIOManager):
	"""This IOManager will take in a pandas dataframe and store it in postgresql.
	"""

	url: Optional[str]

	@property
	def _config(self):
		return self.dict()

	def handle_output(self, context: OutputContext, obj: pandas.DataFrame):
		get_dagster_logger().error(f"context: {context.asset_key}")
		# schema, table = self._get_schema_table(context.asset_key)
		
		# if isinstance(obj, pandas.DataFrame):
		# 	row_count = len(obj)
		# 	context.log.info(f"Row count: {row_count}")
		# 	# TODO make chunksize configurable
		# 	with connect_postgresql(config=self._config) as con:
		# 		obj.to_sql(con=con, 
		# 			name=table, 
		# 			schema=schema, 
		# 			if_exists='replace', 
		# 			chunksize=500
		# 		)
		# else:
		# 	raise Exception(f"Outputs of type {type(obj)} not supported.")

	# def load_input(self, context: InputContext) -> geopandas.GeoDataFrame:
	# 	schema, table = self._get_schema_table(context.asset_key)
	# 	with connect_postgresql(config=self._config) as con:
	# 		columns = (context.metadata or {}).get("columns")
	# 		return self._load_input(con, table, schema, columns, context)
			
	# def _load_input(
	# 	self, 
	# 	con: Connection,
	# 	table: str, 
	# 	schema: str, 
	# 	columns: Optional[Sequence[str]],
	# 	context: InputContext
	# ) -> pandas.DataFrame:
	# 	df = pandas.read_sql(
	# 		sql="""
	# 			SELECT * FROM caddy_fct LIMIT 10
	# 		""",
	# 		con=con,
	# 	)
	# 	return df