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
	

	# some things to do is to make some test enviroment resources
	# Only parsing the data we want to and make a parition