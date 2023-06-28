import os
from contextlib import contextmanager
from typing import Iterator

import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger
from sqlalchemy import create_engine
from datetime import datetime
from sqlite3 import connect 

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
	
	def check_table_exists(self, table_name: str, con):
		return con.execute(
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
			"WHERE table_name = '{}')".format(table_name)
		).scalar()


	def get_latest_row(self, table_name: str):
		latest_row = 0
		with connect_pg(self.url) as con:
			if self.check_table_exists(table_name + "_metadata", con):
			# check if table exists
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
			if not self.check_table_exists(table_name + "_metadata", con):
				query = (
					f"CREATE TABLE {table_name}_metadata (last_row integer, date timestamp)"
				)
				con.execute(query)
			
			query = (
				f"INSERT INTO {table_name}_metadata (last_row, date) VALUES (%s, %s)"
			)
			con.execute(query, (latest_row, date))

	def execute_query(self, sql: str):
		with connect_pg(self.url) as con:
			return pd.read_sql_query(sql, con=con)

	def insert_df(self, df: pd.DataFrame, table_name, schema=None):
		with connect_pg(self.url) as con:
			df.to_sql(
				table_name,
				con=con,
				if_exists="append",
				index=False,
				chunksize=1000,
				method="multi",
			)

	def insert_df_update(self, df: pd.DataFrame, table_name, schema=None, pk="id") -> int:
		self.insert_df(df, table_name)
		last_processed = df[pk].max() if df.shape[0] > 0 else 0
		self.update_latest_row(table_name, int(last_processed))
		return int(last_processed)

@contextmanager
def connect_sqlite(url) -> Iterator:
	conn = None
	try:
		conn = connect(url)
		yield conn
	finally:
		if conn:
			conn.close()

class RemoteSQLiteResource(ConfigurableResource):
	remote_path: str
	local_path: str = "./temp.db"
	# when adding new env, add to dagster.yaml too
	# when using tailscale ssh, give the tailscaled daemon full disk access
	host: str = os.getenv("MAC_HOST")

	def get_remote_file(self):
		from fabric import Connection
		from paramiko import RSAKey

		get_dagster_logger().info(f"Getting remote file from {self.host}")

		# paramiko needs password or pkey, but we are using tail scale ssh so use random key
		with Connection(self.host, connect_kwargs={"pkey": RSAKey.generate(2048)}) as c:
			c.get(self.remote_path, self.local_path)

	def get_client(self):
		self.get_remote_file()
		return connect_sqlite(self.local_path)
	
	def execute_query(self, sql: str):
		with self.get_client() as con:
			return pd.read_sql_query(sql, con=con)