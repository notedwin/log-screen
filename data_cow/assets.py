from dagster import (
	asset,
	Output
)
import pandas as pd
from pandas import json_normalize

from .resources import PostgresResource

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
	WHERE message LIKE '%%http.log.access.log0%%' 
	AND is_valid_json(regexp_replace(
			SUBSTRING(
				message
				FROM POSITION('{' IN message)
			),
			'("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):[\s]*\[[^]]*\]',
			'\\1:[]',
			'g'
		)::varchar) AND id > """ + str(last_row)
	# ignore the lines I can't parse with regex, there is only 2 of them
	raw_data = postgres.execute_query(sql)
	normal_df = json_normalize(raw_data[json_column])
	normal_df = pd.concat([raw_data, normal_df], axis=1)
	normal_df = normal_df.drop(columns=[json_column])
	last_processed = postgres.insert_df_update(normal_df, table_name)
	return Output(
		None,
		metadata={
			"Table Modified": table_name,
			"Last row Processed": last_processed,
			"Num Rows Inserted": int(raw_data.shape[0]),
		}
	)

@asset
def ActivityWatch2Postgres(context, postgres: PostgresResource):
	from fabric import Connection
	from paramiko import RSAKey
	from os import getenv
	from sqlite3 import connect

	table_name = "eventmodel"
	# when adding new env, add to dagster.yaml too
	host = getenv("MAC_HOST")
	path = "/Users/edwinzamudio/Library/Application Support/activitywatch/aw-server/peewee-sqlite.v2.db"

	# paramiko needs password or pkey, but we are using tail scale ssh so use random key
	with Connection(host, connect_kwargs={"pkey": RSAKey.generate(2048)}) as c:
		result = c.get(path, "./aw.db")
		print("Uploaded {0.local} to {0.remote}".format(result))
	
	sqlite = connect("./aw.db")
	last_row = postgres.get_latest_row(table_name)
	context.log.info(f"latest row:{last_row}")
	sql = f"SELECT * FROM {table_name} WHERE id > {last_row}"
	data = pd.read_sql_query(sql, sqlite)
	context.log.info(f"Read {data.shape[0]} rows from raw table")
	last_processed = postgres.insert_df_update(data, table_name)
	context.log.info(f"Last row processed: {last_processed}")

	return Output(
		None,
		metadata={
			"Table Modified": table_name,
			"Last row Processed": last_processed,
			"Num Rows Inserted": int(data.shape[0]),
		}
	)

	# some things to do is to make some test enviroment resources
	# Only parsing the data we want to and make a parition