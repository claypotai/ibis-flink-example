import sys

from kafka import KafkaConsumer
from pyflink.table import EnvironmentSettings, TableEnvironment

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch

local = len(sys.argv) > 1 and sys.argv[1] == "local"

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
connection._exec_sql("ADD JAR '../qflink-sql-connector-kafka-3.0.2-1.18.jar'")

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
# Define a window specification for aggregating maximum transaction amount over the last 5 minutes.
# The aggregation is partitioned by user_id and ordered by trans_date_trans_time.
# The window range is set to the interval from 5 minutes ago to the current time.
user_max_trans_amt_last_5min = source_table[
    source_table.user_id,
    # Calculate the maximum transaction amount over the specified window.
    source_table.amt.max().over(
        ibis.window(
            group_by=source_table.user_id,
            order_by=source_table.trans_date_trans_time,
            range=(-ibis.interval(minutes=5), 0),
        )
    ).name("user_max_trans_amt_last_5min"),
    source_table.trans_date_trans_time
]

# 4. Creat Sink
sink_topic_name = "user_max_trans_amt_last_5min"
sink_schema = sch.Schema(
    {
        "user_id": dt.int64,
        "user_max_trans_amt_last_5min": dt.float64,
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
connection.insert(sink_topic_name, user_max_trans_amt_last_5min)

if local:
    # Use the Kafka Python client to stream records from the sink topic.
    # Otherwise, the mini cluster will shut down upon script completion.
    consumer = KafkaConsumer(sink_topic_name)
    for msg in zip(consumer):
        print(msg)