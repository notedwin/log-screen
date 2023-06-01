import os
from contextlib import contextmanager
from typing import Iterator

import pandas as pd
from dagster import ConfigurableResource
from sqlalchemy import create_engine
from datetime import datetime

@contextmanager
def connect_pg(url) -> Iterator:
	conn = None
	try:
		conn = create_engine(url).connect()
		yield conn
	finally:
		if conn:
			conn.close()


class PostgresResource(ConfigurableResource):
	url: str = os.getenv("DATABASE_URL")

	def get_client(self):
		return connect_pg(self.url)

	def get_latest_row(self, table_name: str):
		latest_row = 0
		with connect_pg(self.url) as con:
			result = con.execute(
				f"SELECT MAX(last_row) AS num_row FROM {table_name}_metadata"
			)
			row = result.fetchone()
			if row is not None and row["num_row"] is not None:
				latest_row = row["num_row"]
		return latest_row

	def update_latest_row(self, table_name: str, latest_row: int):
		with connect_pg(self.url) as con:
			date = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
			query = (
				f"INSERT INTO {table_name}_metadata (last_row, date) VALUES (%s, %s)"
			)
			con.execute(query, (latest_row, date))

	def execute_query(self, sql: str):
		with connect_pg(self.url) as con:
			return pd.read_sql_query(sql, con=con)

	def insert_df(self, df: pd.DataFrame, table_name, schema=None) -> int:
		with connect_pg(self.url) as con:
			return df.to_sql(
				table_name,
				con=con,
				if_exists="append",
				index=False,
				chunksize=1000,
				method="multi",
			)