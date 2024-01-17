import ibis
from kafka import KafkaConsumer, TopicPartition
from pyflink.table import EnvironmentSettings, TableEnvironment
import yaml
import argparse
import json

from utils.util import create_and_connect_kafka_table, create_schema
from schemas.source_schema import CreditCardTransaction


def compute_streaming_feature(t, connection, yaml_path, local, print_examples=True):
    """
    Computes streaming features based on the configuration specified in a YAML file.

    Args:
        t (ibis.expr.table.Table): Ibis table representing the source data.
        connection (ibis.client.Client): Ibis client for connecting to the backend.
        path (str): Path to the YAML file containing feature configuration.
        print_examples (bool, optional): Flag to print examples of created Kafka topics. Default is True.

    Returns:
        list: List of dictionaries containing information about the created Kafka topics.
    """
    features_config_list = []
    bootstrap_servers = "localhost:9092" if local else "kafka:29092"
    # Load feature configuration from the YAML file.
    with open(yaml_path, 'r', encoding='utf-8') as file:
        features_config = yaml.load_all(file, Loader=yaml.FullLoader)
        for conf in features_config:
            features_config_list.append(conf)

    created_topics = []
    # Iterate over each feature configuration.
    for feature in features_config_list:
        for name_prefix, conf in feature.items():
            # Iterate over specified aggregation functions.
            for agg_func in conf['agg_func']:
                primary_keys_and_types = conf['primary_keys']
                agg_primary_key = [keys['name'] for keys in primary_keys_and_types][0]
                agg_col = conf['agg_col']
                time_col = conf['time_col']
                created_topic = {}

                # Iterate over specified intervals for the feature.
                for interval_in_minutes in conf['interval_in_minutes']:              
                    topic_name = f"{name_prefix}_{agg_func}_{interval_in_minutes}m"
                    
                    # Print information about the feature being created.
                    print(f"\n\n***********Creating feature = {topic_name}************************")
                    
                    # Create schema for the feature.
                    schema = create_schema(name_prefix, primary_keys_and_types, agg_func, time_col, interval_in_minutes)
                    # Connect table to Kafka and insert aggregated data.
                    f = create_and_connect_kafka_table(connection=connection,
                                                       topic_name=topic_name,
                                                       schema=schema,
                                                       time_col=time_col,
                                                       primary_key=agg_primary_key,
                                                       kafka_data_format="json",
                                                       local=local )
                    print("sink")
                    print(f)
                    data = t[
                        t[agg_primary_key],
                        getattr(t[agg_col], agg_func)().over(
                            ibis.window(
                                group_by=agg_primary_key,
                                order_by=time_col,
                                range=(-ibis.interval(minutes=interval_in_minutes), 0)
                            )
                        ).name(topic_name),
                        t[time_col]
                    ]

                    # Emit aggregated data to kafka topic.
                    connection.insert(topic_name, data)

                    # Record information about the created Kafka topic.
                    created_topic["topic_name"] = topic_name
                    created_topic["primary_key"] = agg_primary_key
                    created_topic["time_col"] = time_col              
                    created_topics.append(created_topic)

                    # Print examples of the created Kafka topic if specified.
                    if print_examples:
                        print("--------------------Print 2 examples------------------")
                        # consumer = KafkaConsumer("transaction", bootstrap_servers=bootstrap_servers)
                        # print(consumer.poll(200000))
                        print("ddd")
                        consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)
                        # print(consumer.poll(200000))
                        print("ccc")
                        for _, msg in zip(range(2), consumer):
                            print(msg)
                            
    return created_topics



def main(feature_path, local, print_examples):

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
    connection._exec_sql("ADD JAR '../../flink-sql-connector-kafka-3.0.2-1.18.jar'")

    # # Specify the topic and partition
    # bootstrap_servers = "localhost:9092" if local else "kafka:29092"
    # topic = 'transaction'
    # partition = 0  # Replace with the partition number you want to set the offset for
    # print("transaction")
    # # Create a KafkaConsumer
    # print(bootstrap_servers)
    # consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers) # bootstrap_servers='kafka:29092'
    # print(consumer.topics())
    # consumer.assign([TopicPartition(topic, partition)])
    # # Set the offset to the earliest available offset
    # consumer.seek_to_beginning(TopicPartition(topic, partition))
    # print(consumer.poll(200000))

    t = create_and_connect_kafka_table(
        connection=connection,
        topic_name="transaction",
        schema=CreditCardTransaction.__annotations__,
        time_col="trans_date_trans_time",
        kafka_data_format="json",
        local=local
    )

    print(t)

    created_topics = compute_streaming_feature(t, connection, feature_path, local, print_examples)

    # Write created_topics to a file
    with open("./created_topics.json", 'w', encoding='utf-8') as outfile:
        json.dump(created_topics, outfile)

    print("#############################################")
    print("------------------Done----------------------")
    print("#############################################")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature_path", default="../../features/features.yaml")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--print_examples", action="store_true")
    args = parser.parse_args()
    main(args.feature_path, args.local, args.print_examples)
