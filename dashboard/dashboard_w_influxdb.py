import collections
import dash
import kafka
import json
import logging
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import signal
import sys
import threading
import time

from dash import dcc, html
from dash.dependencies import Input, Output
from datetime import datetime, timedelta, timezone
from dotenv import dotenv_values
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from plotly import subplots

import config


print(
    "Env variables: \n"
    f"DASHBOARD_PORT= {config.DASHBOARD_PORT} \n"
    f"KAFKA_SOURCE_TOPIC= {config.KAFKA_SOURCE_TOPIC} \n"
    f"KAFKA_FEATURE_TOPIC= {config.KAFKA_FEATURE_TOPIC} \n"
    f"KAFKA_SERVICE= {config.KAFKA_SERVICE} \n"
    f"KAFKA_BOOTSTRAP_PORT= {config.KAFKA_BOOTSTRAP_PORT} \n"
    f"INFLUXDB_SERVICE= {config.INFLUXDB_SERVICE} \n"
    f"INFLUXDB_PORT= {config.INFLUXDB_PORT} \n"
)

## Kafka
TOPIC_TO_CONSUMER_MAP = {
    config.KAFKA_SOURCE_TOPIC: None,
    config.KAFKA_FEATURE_TOPIC: None,
}
LOCK = threading.Lock()
# CONDITION = threading.Condition(lock=LOCK)
CONDITION = threading.Condition()
KAFKA_CONSUMERS_READY = False

## Dash
app = dash.Dash(title="Ibis/Flink demo dashboard", update_title=None)
flask_server = app.server

# The topic that will determine the x-axis for all graphs.
# Ther rest of the topics are treated as followers of the leader.
# Plots for the follower topics will have the same x-axis as
# the leader topic for every x-field. All follower x-topics must
# be managed by the leader topic.
LEADER_KAFKA_TOPIC = config.KAFKA_FEATURE_TOPIC
print(f"LEADER_KAFKA_TOPIC= {LEADER_KAFKA_TOPIC}")


class TopicManager:
    def __init__(
        self,
        topic: str,
        field_pairs: list[tuple[str, str]],
    ):
        self.topic = topic
        self.field_pairs = field_pairs

        self.field_set = set()
        for x, y in field_pairs:
            self.field_set.add(x)
            self.field_set.add(y)


class LeaderTopicManager(TopicManager):
    def __init__(
        self,
        topic: str,
        field_pairs: list[tuple[str, str]],
        max_length: int = config.MAX_TIME_SERIES_LENGTH,
    ):
        super().__init__(topic=topic, field_pairs=field_pairs)

        self.field_to_queue = {
            field: collections.deque(maxlen=max_length)
            for field in self.field_set
        }

    def append(self, field: str, value):
        self.field_to_queue[field].append(value)

    def get_x_y_values(self, x_field: str, y_field: str) -> tuple[list, list]:
        return (
            list(self.field_to_queue[x_field]),
            list(self.field_to_queue[y_field]),
        )

class FollowerTopicManager(TopicManager):
    def __init__(
        self,
        topic: str,
        field_pairs: list[tuple[str, str]],
        leader_topic_manager: LeaderTopicManager,
    ):
        super().__init__(topic=topic, field_pairs=field_pairs)
        self.leader_topic_manager = leader_topic_manager

        for x_field, _ in field_pairs:
            if x_field not in leader_topic_manager.field_set:
                raise ValueError(
                    f"Follower topic manager requires `x_field` {x_field} "
                    "which is not managed by the leader topic manager."
                )

    def append(self, x_field: str, y_field: str, x_value, y_value):
        point = (
            Point("measurement_name")
            .time(x_value)
            .tag("topic", self.topic)
            .tag("x_field", x_field)
            .tag("y_field", y_field)
            .field("y_value", float(y_value))
        )
        DB_WRITE_API.write(bucket=DB_BUCKET, record=point)

    def get_x_y_values(self, x_field: str, y_field: str) -> tuple[list, list]:
        # TODO: This might lead to querying db multiple times for the same x-axis,
        # which is redundant. Leaving it this way for simplicity.

        leader_x_q = self.leader_topic_manager.field_to_queue[x_field]
        if len(leader_x_q) == 0:
            return [], []

        x_min = leader_x_q[0]
        x_min = get_datetime_from_str(x_min).isoformat() + "Z"
        x_max = leader_x_q[-1]
        x_max = get_datetime_from_str(x_max).isoformat() + "Z"
        # x_max = (get_datetime_from_str(x_max) + timedelta(hours=1)).isoformat() + "Z"
        # print(f"x_field= {x_field}, y_field= {y_field}")
        # print(f"x_min= {x_min}, x_max= {x_max}")
        if x_max == x_min:
            return [], []

        # |> range(start: -1h)'
        # |> range(start: 2021-05-22T23:30:00Z, stop: 2021-05-23T00:00:00Z)'
        query = f'from(bucket:"{DB_BUCKET}")\
        |> range(start: {x_min}, stop: {x_max})\
        |> filter(fn:(r) => r.topic == "{self.topic}")\
        |> filter(fn:(r) => r.x_field == "{x_field}")\
        |> filter(fn:(r) => r.y_field == "{y_field}")'
        # print(f"query= {query}")
        tables = DB_QUERY_API.query(query)
        if not tables:
            # print(f"`tables` is empty")
            return [], []

        # print(f"num_results= {len(tables[0].records)}")
        x_values, y_values = [], []
        for row in tables[0].records:
            x_values.append(row["_time"])
            y_values.append(row["_value"])

        return x_values, y_values


class FollowerTopicManagerPool:
    def __init__(self, leader_topic_manager: LeaderTopicManager):
        self.leader_topic_manager = leader_topic_manager

        self.topic_to_manager_map = {}

    def add(self, topic: str, field_pairs: list[tuple[str, str]]):
        self.topic_to_manager_map[topic] = FollowerTopicManager(
            topic=topic,
            field_pairs=field_pairs,
            leader_topic_manager=self.leader_topic_manager,
        )

    def append(self, topic: str, x_field: str, y_field: str, x_value: str, y_value):
        # dt = get_datetime_from_str(x_value)
        # dt = datetime_from_iso8601(x_value)
        self.topic_to_manager_map[topic].append(
            x_field=x_field, y_field=y_field, x_value=x_value, y_value=y_value
        )

    def get_x_y_values(self, topic: str, x_field: str, y_field: str) -> tuple[list, list]:
        return self.topic_to_manager_map[topic].get_x_y_values(x_field, y_field)


def get_datetime_from_str(s: str):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def datetime_from_iso8601(iso8601_str):
    if iso8601_str[-1].upper() == "Z":
        iso8601_str = f"{iso8601_str[:-1]}+00:00"

    dt = datetime.fromisoformat(iso8601_str)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


DB_BUCKET = "dashboard"
DB_ORG = "dashboard"
DB_TOKEN = "dashboard"
DB_CLIENT = InfluxDBClient(url=f"http://{config.INFLUXDB_SERVICE}:{config.INFLUXDB_PORT}", token=DB_TOKEN, org=DB_ORG)
DB_WRITE_API = DB_CLIENT.write_api(write_options=SYNCHRONOUS)
DB_QUERY_API = DB_CLIENT.query_api()
print("InfluxDBClient is ready.")

# Init managers for leader and follower topics.
LEADER_TOPIC_MANAGER = LeaderTopicManager(
    topic=LEADER_KAFKA_TOPIC,
    field_pairs=config.TOPIC_TO_FIELD_PAIRS_TO_PLOT[LEADER_KAFKA_TOPIC],
)

FOLLOWER_TOPIC_MANAGER_POOL = FollowerTopicManagerPool(
    leader_topic_manager=LEADER_TOPIC_MANAGER,
)
for topic, field_pairs in config.TOPIC_TO_FIELD_PAIRS_TO_PLOT.items():
    if topic != LEADER_KAFKA_TOPIC:
        FOLLOWER_TOPIC_MANAGER_POOL.add(topic, field_pairs)


app.layout = html.Div([
    html.H1(children="Features Over Time", style={"textAlign": "center"}),
    html.Br(),
    html.Div(
        children=[
            dcc.Graph(id="real-time-plot"),
            dcc.Interval(
                id="interval-component",
                # interval=1 * 100,  # in milliseconds
                # interval=1 * 200,  # in milliseconds
                interval=1 * 300,  # in milliseconds
                # interval=1 * 1000,  # in milliseconds
                # interval=1 * 10000000,  # in milliseconds
                n_intervals=0,
            )
        ]
    ),
])


@app.callback(
    Output("real-time-plot", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_plots(n):
    global FOLLOWER_TOPIC_MANAGER_POOL, KAFKA_CONSUMERS_READY, LEADER_TOPIC_MANAGER, TOPIC_TO_CONSUMER_MAP

    with LOCK:
        consumers_ready = KAFKA_CONSUMERS_READY

    # Pull from leader topic
    if consumers_ready:
        consumer = TOPIC_TO_CONSUMER_MAP[LEADER_KAFKA_TOPIC]
        data_dict = consumer.poll(1.0, max_records=1)
        # print(f"update_plots:: pulled from topic= {LEADER_KAFKA_TOPIC}")
        if data_dict:
            for topic_partition, record_list in data_dict.items():
                for record in record_list:
                    record_as_dict = json.loads(record.value.decode("utf-8"))
                    # print(f"record_as_dict= \n{record_as_dict}")

                    for i, field in enumerate(LEADER_TOPIC_MANAGER.field_set):
                        if LEADER_KAFKA_TOPIC == config.KAFKA_FEATURE_TOPIC:
                            value = record_as_dict["after"][field]
                        else:
                            value = record_as_dict[field]

                        LEADER_TOPIC_MANAGER.append(field=field, value=value)

    # Construct the graphs
    figure = subplots.make_subplots(rows=config.NUM_ROWS_IN_DASHBOARD)
    i = 0
    for topic, field_pairs in config.TOPIC_TO_FIELD_PAIRS_TO_PLOT.items():
        # print(f"> topic= {topic}")

        for x_field, y_field in field_pairs:
            row = i + 1
            col = 1

            if topic == LEADER_KAFKA_TOPIC:
                x_values, y_values = LEADER_TOPIC_MANAGER.get_x_y_values(
                    x_field=x_field, y_field=y_field
                )
            else:
                x_values, y_values = FOLLOWER_TOPIC_MANAGER_POOL.get_x_y_values(
                    topic=topic, x_field=x_field, y_field=y_field
                )

            figure.add_trace(
                go.Scatter(
                    x=x_values,
                    y=y_values,
                    mode="lines+markers",
                    marker=dict(
                        symbol="square-open",
                        size=8,
                        line=dict(width=2),
                        color=config.PLOT_COLOR_LIST[i]
                    ),
                ),
                row=row,
                col=col,
            )
            figure.update_xaxes(title_text=x_field, row=row, col=col)
            figure.update_yaxes(title_text=y_field, row=row, col=col)

            i += 1

    figure.update_layout(height=config.DASHBOARD_HEIGHT, width=config.DASHBOARD_WIDTH)
    return figure


def boot_kafka_consumers():
    global CONDITION, KAFKA_CONSUMERS_READY, TOPIC_TO_CONSUMER_MAP

    print("boot_kafka_consumers:: started")

    # Create the consumers
    consumers_created = False
    while not consumers_created:
        consumers_created = True
        for topic, consumer in TOPIC_TO_CONSUMER_MAP.items():
            if consumer:
                continue

            try:
                consumer = kafka.KafkaConsumer(
                    # topic,
                    # client_id=topic,
                    bootstrap_servers=f"{config.KAFKA_SERVICE}:{config.KAFKA_BOOTSTRAP_PORT}",
                    # group_id="topic",
                )
            except kafka.errors.NoBrokersAvailable:
                print(f"boot_kafka_consumers:: No Kafka broker available for topic= {topic}")
                consumers_created = False
                continue

            try:
                consumer.assign([kafka.TopicPartition(topic, 0)])
                consumer.seek_to_beginning()
            except AssertionError:
                print(f"boot_kafka_consumers:: No partitions are currently assigned for topic= {topic}")
                consumers_created = False
                continue

            TOPIC_TO_CONSUMER_MAP[topic] = consumer
            print(f"boot_kafka_consumers:: Created Kafka consumer for topic= {topic}")

        if not consumers_created:
            print("boot_kafka_consumers:: Will sleep for 2 sec and try again...")
            time.sleep(2)
    print("boot_kafka_consumers:: Created the consumers for all topics.")

    with LOCK:
        KAFKA_CONSUMERS_READY = True
    with CONDITION:
        CONDITION.notify_all()


def consume_from_follower_topics(follower_topic_manager_pool: FollowerTopicManagerPool):
    global CONDITION, KAFKA_CONSUMERS_READY

    print("consume_from_follower_topics:: Will wait for Kafka consumers...")
    with CONDITION:
        CONDITION.wait_for(lambda: KAFKA_CONSUMERS_READY)
    print("consume_from_follower_topics:: Kafka consumers are ready.")

    while True:
        time.sleep(0.001)
        for topic, consumer in TOPIC_TO_CONSUMER_MAP.items():
            if topic == LEADER_KAFKA_TOPIC:
                continue

            consumer = TOPIC_TO_CONSUMER_MAP[topic]

            data_dict = consumer.poll(1.0, max_records=1)
            # print(f"Pulled from topic= {topic}")
            if not data_dict:
                continue

            for topic_partition, record_list in data_dict.items():
                for record in record_list:
                    record_as_dict = json.loads(record.value.decode("utf-8"))
                    # print(f"topic= {topic}, record_as_dict= \n{record_as_dict}")

                    for i, (x_field, y_field) in enumerate(config.TOPIC_TO_FIELD_PAIRS_TO_PLOT[topic]):
                        if topic == config.KAFKA_FEATURE_TOPIC:
                            x_value = record_as_dict["after"][x_field]
                            y_value = record_as_dict["after"][y_field]
                        else:
                            x_value = record_as_dict[x_field]
                            y_value = record_as_dict[y_field]

                        follower_topic_manager_pool.append(
                            topic=topic, x_field=x_field, y_field=y_field, x_value=x_value, y_value=y_value
                        )

    print("consume_from_follower_topics:: done")


if __name__ == "__main__":
    # Clear DB
    start = "1970-01-01T00:00:00Z"
    stop = "2025-01-01T00:00:00Z"
    delete_api = DB_CLIENT.delete_api()
    print(f"Clearing InfluxDB bucket {DB_BUCKET}...")
    delete_api.delete(start, stop, predicate="", bucket=DB_BUCKET, org=DB_ORG)
    print(f"Cleared InfluxDB bucket {DB_BUCKET}")

    # Start `boot_kafka_consumers`
    threading.Thread(target=boot_kafka_consumers, daemon=True).start()

    # Start `consume_from_follower_topics`
    threading.Thread(
        target=consume_from_follower_topics,
        args=[FOLLOWER_TOPIC_MANAGER_POOL],
        daemon=True,
    ).start()

    # Start app
    app.run_server(host="0.0.0.0", debug=True, port=config.DASHBOARD_PORT, dev_tools_silence_routes_logging=False)
    # app.run_server(host="0.0.0.0", debug=True, port=config.DASHBOARD_PORT)

    # Clean up connections
    for consumer in TOPIC_TO_CONSUMER_MAP.values():
        consumer.close()

    DB_CLIENT.close()
    print("Done")
