from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.types import BIGINT, DATE
from typing import Iterator, Optional, Sequence, Callable

from dagster import (
	op,
	job,
	asset,
	ConfigurableResource,
	Config,
	RunConfig,
	get_dagster_logger,
	Output
)
import os
import datetime
import pandas as pd
from pandas import json_normalize

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
			date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
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


@asset
def CaddyLogsParsing(context, postgres: PostgresResource):
	table_name = "caddy_fct"
	json_column = "json_message"
	last_row = postgres.get_latest_row(table_name)
	context.log.info(f"latest row:{last_row}")
	sql = """
	SELECT id,
		message,
		CAST(
			SUBSTRING(
				SUBSTRING(
					message
					FROM 1 FOR POSITION('{' IN message) - 1
				)
				FROM 1 FOR 24
			) AS TIMESTAMP
		) AS time_reported,
		regexp_replace(
			SUBSTRING(
				message
				FROM POSITION('{' IN message)
			),
			'("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):\s*\[[^]]*\]',
			'\\1:[]',
			'g'
		)::JSON AS json_message
	FROM dockerlogs
	WHERE message LIKE '%%http.log.access.log0%%' AND id > """ + str(last_row)
	raw_data = postgres.execute_query(sql)
	normal_df = json_normalize(raw_data[json_column])
	normal_df = pd.concat([raw_data, normal_df], axis=1)
	normal_df = normal_df.drop(columns=[json_column])
	row_inserted = postgres.insert_df(normal_df, table_name)
	context.log.info(f"Inserted {row_inserted} rows into {table_name} table")
	last_row = raw_data["id"].max() if raw_data.shape[0] > 0 else 0
	postgres.update_latest_row(table_name, int(last_row))
	return Output(
		normal_df[:5],
		metadata={
			"Table Modified": table_name,
			"Rows Inserted": row_inserted,
			"Last Row": last_row
		}
	)
	

	# some things to do is to make some test enviroment resources
	# Only parsing the data we want to and make a parition