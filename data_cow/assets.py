from dagster import (
	asset,
	Output
)
import pandas as pd
from pandas import json_normalize

from .resources import PostgresResource, RemoteSQLiteResource

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
def IP_data(context, postgres: PostgresResource, CaddyLogsParsing):
	import requests
	import time
	df = postgres.execute_query("""
	SELECT DISTINCT SUBSTR("request.headers.Cf-Connecting-Ip", 2, LENGTH("request.headers.Cf-Connecting-Ip") - 2) AS IP FROM caddy_fct
    EXCEPT
    SELECT DISTINCT query FROM ip_data
	""")  # noqa: E501
	context.log.info(f"Found {df.shape[0]} new IPs")
	partitions = [df[i:i + 100] for i in range(0, df.shape[0], 100)]
	context.log.info(f"Split into {len(partitions)} partitions")
	for partition in partitions:
		partition = partition.dropna()

		json_data = partition["ip"].to_json(orient="values")
		context.log.info(json_data)
		context.log.info(f"Sending {len(partition)} IPs to API")
		response = requests.post("http://ip-api.com/batch/", data=json_data, timeout=12)
		context.log.info(f"status code: {response.status_code}")
		if response.status_code == 200:
			context.log.info("Got response from API")
			data = pd.DataFrame(response.json())
			# remove message column
			if "message" in data.columns:
				data = data.drop(columns=["message"])

			postgres.insert_df(data, "ip_data")
			context.log.info(f"Inserted {data.shape[0]} rows")
			time.sleep(1)

	return Output(
		None,
		metadata={
			"Table Modified": "ip_data",
			"Num Rows Inserted": int(df.shape[0]),
		}
	)

@asset
def aw2Postgres(context, postgres: PostgresResource, activitywatch: RemoteSQLiteResource):
	table_name = "eventmodel"
	
	last_row = postgres.get_latest_row(table_name)
	df = activitywatch.execute_query(f"SELECT * FROM {table_name} WHERE id > {last_row}")
	context.log.info(f"Read {df.shape[0]} rows from raw table")
	last_processed = postgres.insert_df_update(df, table_name)

	return Output(
		None,
		metadata={
			"Table Modified": table_name,
			"Last row Processed": last_processed,
			"Num Rows Inserted": int(df.shape[0]),
		}
	)

@asset
def st2Postgres(context, postgres: PostgresResource, screentime: RemoteSQLiteResource):
	table_name = "zobject"
	last_row = postgres.get_latest_row(table_name)
	df = screentime.execute_query(f"SELECT * FROM {table_name} WHERE Z_PK > {last_row}")
	context.log.info(f"Read {df.shape[0]} rows from raw table")
	last_processed = postgres.insert_df_update(df, table_name, pk="Z_PK")

	return Output(
		None,
		metadata={
			"Table Modified": table_name,
			"Last row Processed": last_processed,
			"Num Rows Inserted": int(df.shape[0]),
		}
	)
