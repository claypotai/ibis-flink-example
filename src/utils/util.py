import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch

def create_and_connect_kafka_table(connection,
                           topic_name,
                           schema,
                           time_col,
                           primary_key=None,
                           kafka_data_format="debezium-json",
                           local=True
):
    """
    Connects to a Kafka topic and creates a table with the specified schema.

    Args:
        connection (ibis.client.Client): Ibis client for connecting to the backend.
        topic_name (str): Name of the Kafka topic.
        schema (dict): Dictionary defining the schema for the table.
        time_col (str): Column representing the timestamp for watermarking.
        primary_key (str, optional): Primary key column for the table. Default is None.
        kafka_data_format (str, optional): Kafka data format. Default is "debezium-json".
        local (bool, optional): Flag indicating whether the Kafka server is local. Default is True.

    Returns:
        ibis.expr.table.Table: Created table connected to the specified Kafka topic.
     """
     
    print(f"local in util = {local}")
    sink_configs = {
        "connector": "kafka",
        "topic": topic_name,
        "properties.bootstrap.servers": "localhost:9092" if local else "kafka:29092",
        "scan.startup.mode": "earliest-offset",
        "format": kafka_data_format,
    }

    t = connection.create_table(
        topic_name,
        schema=sch.Schema(schema),
        tbl_properties=sink_configs,
        watermark=ibis.watermark(
            time_col=time_col, allowed_delay=ibis.interval(seconds=15)
        ),
        #  primary_key = primary_key,
        overwrite=True
    )

    return t

def create_schema(name_prefix, primary_keys, agg_func, time_col, interval_in_minutes):
    """
    Creates a schema for a table.

    Args:
        name_prefix (str): Prefix for the names of schema columns.
        primary_keys (list): List of dictionaries containing 'name' and 'type' for primary key columns.
        agg_func (str): Aggregation function to be applied to the data.
        time_col (str): Column representing the timestamp in the schema.
        interval_in_minutes (int): Interval for the aggregation in minutes.

    Returns:
        dict: A dictionary defining the schema for the table.
    """
    schema = {
        key_name_type['name']: eval(key_name_type['type']) for key_name_type in primary_keys
    }
    schema[f'{name_prefix}_{agg_func}_{interval_in_minutes}m'] = dt.float64
    schema[time_col] = dt.timestamp(scale=3)
    return schema