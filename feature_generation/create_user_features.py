import argparse

from kafka import KafkaConsumer
from pyflink.table import EnvironmentSettings, TableEnvironment

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch


parser = argparse.ArgumentParser()
parser.add_argument("--interval_in_minutes", default=60)
parser.add_argument("--local", action="store_true")
args = parser.parse_args()
local = args.local
interval_in_minutes = args.interval_in_minutes

# 1. Establish ibis flink backend connection
# create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
# write all the data to one file
table_env.get_config().set("parallelism.default", "1")

# The `flink` backend does not create `TableEnvironment` objects; pass
# the `TableEnvironment` object created above to `ibis.flink.connect`.
connection = ibis.flink.connect(table_env)

# Flink’s streaming connectors aren't part of the binary distribution.
# Link the Kafka connector for cluster execution by adding a JAR file.
connection._exec_sql("ADD JAR 'flink-sql-connector-kafka-3.0.2-1.18.jar'")

# 2. Create source table
source_topic_name = "transaction"
kafka_offset = "earliest-offset"
source_schema = sch.Schema(
    {
        "user_id": dt.int64,
        "trans_date_trans_time": dt.timestamp(scale=3),
        "cc_num": dt.int64,
        "amt": dt.float64,
        "trans_num": dt.str,
        "merchant": dt.str,
        "category": dt.str ,      
        "is_fraud": dt.int32,
        "first": dt.str,
        "last": dt.str,
        "dob": dt.str,
        "zipcode": dt.str,
    }
)

# Configure the source table with Kafka connector properties.
source_configs = {
    "connector": "kafka",
    "topic": source_topic_name,
    "properties.bootstrap.servers": "localhost:9092" if local else "kafka:29092",
    "properties.group.id": "test",
    "scan.startup.mode": kafka_offset,
    "format": "json",
}

# Create the source table using the defined schema, Kafka connector properties,
# and set watermarking for real-time processing with a 15-second delay.
source_table = connection.create_table(
    source_topic_name,
    schema=source_schema,
    tbl_properties=source_configs,
    watermark=ibis.watermark(
        time_col="trans_date_trans_time", allowed_delay=ibis.interval(seconds=15)
    ),
)

# 3. Feature Generation using Flink backend
# Define a window specification for different aggregations over the last X minutes.
# The aggregation is partitioned by user_id and ordered by trans_date_trans_time.
# The window range is set to the interval from X minutes ago to the current time.

user_trans_amt_agg = source_table[
    source_table.user_id,
    # Calculate the maximum transaction amount over the specified window.
    source_table.amt.max().over(
        ibis.window(
            group_by=source_table.user_id,
            order_by=source_table.trans_date_trans_time,
            range=(-ibis.interval(minutes=interval_in_minutes), 0),
        )
    ).name(f"user_max_trans_amt_last_{interval_in_minutes}min"),
    # Calculate the min transaction amount over the specified window.
    source_table.amt.min().over(
        ibis.window(
            group_by=source_table.user_id,
            order_by=source_table.trans_date_trans_time,
            range=(-ibis.interval(minutes=interval_in_minutes), 0),
        )
    ).name(f"user_min_trans_amt_last_{interval_in_minutes}min"),
    # Calculate the average transaction amount over the specified window.
    source_table.amt.mean().over(
        ibis.window(
            group_by=source_table.user_id,
            order_by=source_table.trans_date_trans_time,
            range=(-ibis.interval(minutes=interval_in_minutes), 0),
        )
    ).name(f"user_mean_trans_amt_last_{interval_in_minutes}min"),
    # Calculate the number of transaction count over the specified window.
    source_table.amt.count().over(
        ibis.window(
            group_by=source_table.user_id,
            order_by=source_table.trans_date_trans_time,
            range=(-ibis.interval(minutes=interval_in_minutes), 0),
        )
    ).name(f"user_trans_count_last_{interval_in_minutes}min"),
    source_table.trans_date_trans_time
]

# 3.1 Alternative ways to calculate windowed aggregations
''' 
Different window options 
    1. tumble(): a fixed size and do not overlap
    2. hop(): Hopping windows have a fixed size and can be overlapping if the slide is smaller than the window size 
    3. cumulate(): Cumulate windows don't have a fixed size and do overlap
'''
windowed_stream =  source_table.window_by(
        time_col=source_table.trans_date_trans_time,
    ).tumble(
        window_size=ibis.interval(minutes=360)
    )


user_trans_amt_last_360m_agg_windowed_stream = windowed_stream.group_by(
        ["window_start", "window_end", "user_id"]
    ).agg(
        user_max_trans_amt_last_360min=windowed_stream.amt.max(),
        user_min_trans_amt_last_360min=windowed_stream.amt.min(),
        user_mean_trans_amt_last_360min=windowed_stream.amt.mean(),
    )

# 4. Creat Sink
sink_topic_name = f"user_trans_amt_last_{interval_in_minutes}min"
sink_schema = sch.Schema(
    {
        "user_id": dt.int64,
        f"user_max_trans_amt_last_{interval_in_minutes}min": dt.float64,
        f"user_min_trans_amt_last_{interval_in_minutes}min": dt.float64,
        f"user_mean_trans_amt_last_{interval_in_minutes}min": dt.float64,
        f"user_trans_count_last_{interval_in_minutes}min": dt.int64,
        "trans_date_trans_time": dt.timestamp(scale=3), # used for future temporal join
    }
)

# Configure the sink table with Kafka connector properties for writing results.
sink_configs = {
    "connector": "kafka",
    "topic": sink_topic_name,
    "properties.bootstrap.servers": "localhost:9092" if local else "kafka:29092",
    "format": "debezium-json", # "debezium-json" is needed for future temporal join.
}

connection.create_table(
    sink_topic_name, schema=sink_schema, tbl_properties=sink_configs
)

# 5. Emit query result to sink table
connection.insert(sink_topic_name, user_trans_amt_agg)

if local:
    # Use the Kafka Python client to stream records from the sink topic.
    # Otherwise, the mini cluster will shut down upon script completion.
    consumer = KafkaConsumer(sink_topic_name, bootstrap_servers="localhost:9092" if local else "kafka:29092")
    for msg in zip(consumer):
        print(msg)
