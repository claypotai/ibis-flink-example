import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from kafka import KafkaConsumer
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. create a TableEnvironment
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

# 2. create source Table
source_schema = sch.Schema(
    {
        "createTime": dt.timestamp(scale=3),
        "orderId": dt.int64,
        "payAmount": dt.float64,
        "payPlatform": dt.int32,
        "provinceId": dt.int32,
    }
)

source_configs = {
    "connector": "kafka",
    "topic": "payment_msg",
    "properties.bootstrap.servers": "localhost:9092",
    "properties.group.id": "test_3",
    "scan.startup.mode": "earliest-offset",
    "format": "json",
}

t = connection.create_table(
    "payment_msg",
    schema=source_schema,
    tbl_properties=source_configs,
    watermark=ibis.watermark(
        time_col="createTime", allowed_delay=ibis.interval(seconds=15)
    ),
)

# 3. create sink Table
sink_schema = sch.Schema(
    {
        "province_id": dt.int32,
        "pay_amount": dt.float64,
    }
)

sink_configs = {
    "connector": "kafka",
    "topic": "sink",
    "properties.bootstrap.servers": "localhost:9092",
    "format": "json",
}

connection.create_table(
    "total_amount_by_province_id", schema=sink_schema, tbl_properties=sink_configs
)

# 4. query from source table and perform calculations
agged = t[
    t.provinceId.name("province_id"),
    t.payAmount.sum()
    .over(
        range=(-ibis.interval(seconds=10), 0),
        group_by=t.provinceId,
        order_by=t.createTime,
    )
    .name("pay_amount"),
]

# 5. emit query result to sink table
connection.insert("total_amount_by_province_id", agged)

# Use the Kafka Python client to print some records from the sink topic.
consumer = KafkaConsumer("sink")
for _, msg in zip(range(10), consumer):
    print(msg)
