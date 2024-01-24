import collections
import dash
import kafka
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import threading
import time

from dash import dcc, html
from dash.dependencies import Input, Output
from datetime import datetime
from plotly import subplots

import config


print(
    "Env variables: \n"
    f"DASHBOARD_PORT= {config.DASHBOARD_PORT} \n"
    f"KAFKA_SOURCE_TOPIC= {config.KAFKA_SOURCE_TOPIC} \n"
    f"KAFKA_FEATURE_TOPIC= {config.KAFKA_FEATURE_TOPIC} \n"
    f"KAFKA_SERVICE= {config.KAFKA_SERVICE} \n"
    f"KAFKA_BOOTSTRAP_PORT= {config.KAFKA_BOOTSTRAP_PORT} \n"
)

## Kafka
TOPIC_TO_CONSUMER_MAP = {
    config.KAFKA_SOURCE_TOPIC: None,
    config.KAFKA_FEATURE_TOPIC: None,
}
LOCK = threading.Lock()
KAFKA_CONSUMERS_READY = False


## Dash
app = dash.Dash(title="Ibis/Flink demo dashboard", update_title=None)
flask_server = app.server


# Plot data
class TimeSeries:
    def __init__(
        self,
        x_label: str,
        y_label: str,
        max_length: int = config.MAX_TIME_SERIES_LENGTH,
    ):
        self.x_label = x_label
        self.y_label = y_label

        self._x_q = collections.deque(maxlen=max_length)
        self._y_q = collections.deque(maxlen=max_length)

    def append(self, x, y):
        self._x_q.append(x)
        self._y_q.append(y)

    def x_list(self):
        return list(self._x_q)

    def y_list(self):
        return list(self._y_q)


TOPIC_TO_TIMESERIES_LIST = {
    topic : [
        TimeSeries(x_label=x, y_label=y)
        for x, y in field_pairs
    ]
    for topic, field_pairs in config.TOPIC_TO_FIELD_PAIRS_TO_PLOT.items()
}

app.layout = html.Div([
    html.H1(children="Features Over Time", style={"textAlign": "center"}),
    html.Br(),
    html.Div(
        children=[
            dcc.Graph(id="real-time-plot"),
            dcc.Interval(
                id="interval-component",
                interval=1 * 200,  # in milliseconds
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
    global TOPIC_TO_CONSUMER_MAP, TOPIC_TO_TIMESERIES_LIST

    # print("update_plots:: started")

    # Do not pull from any topic unless the consumers for all topics
    # have been constructed. This is to align the x-axes across graphs.

    with LOCK:
        consumers_ready = KAFKA_CONSUMERS_READY

    if consumers_ready:
        for topic, consumer in TOPIC_TO_CONSUMER_MAP.items():
            # print(f"> topic= {topic}")

            data_dict = consumer.poll(1.0, max_records=1)
            if data_dict:
                for topic_partition, record_list in data_dict.items():
                    for record in record_list:
                        record_as_dict = json.loads(record.value.decode("utf-8"))
                        # print(f"topic= {topic}, record_as_dict= \n{record_as_dict}")

                        for i, (x_feature, y_feature) in enumerate(config.TOPIC_TO_FIELD_PAIRS_TO_PLOT[topic]):
                            if topic == config.KAFKA_FEATURE_TOPIC:
                                x = record_as_dict["after"][x_feature]
                                y = record_as_dict["after"][y_feature]
                            else:
                                x = record_as_dict[x_feature]
                                y = record_as_dict[y_feature]

                            TOPIC_TO_TIMESERIES_LIST[topic][i].append(x, y)

    figure = subplots.make_subplots(rows=config.NUM_ROWS_IN_DASHBOARD)
    i = 0
    for topic, timeseries_list in TOPIC_TO_TIMESERIES_LIST.items():
        for timeseries in timeseries_list:
            row = i + 1
            col = 1
            figure.add_trace(
                go.Scatter(
                    x=timeseries.x_list(),
                    y=timeseries.y_list(),
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
            figure.update_xaxes(title_text=timeseries.x_label, row=row, col=col)
            figure.update_yaxes(title_text=timeseries.y_label, row=row, col=col)

            i += 1

    figure.update_layout(height=config.DASHBOARD_HEIGHT, width=config.DASHBOARD_WIDTH)
    return figure


def boot_kafka_consumers():
    global KAFKA_CONSUMERS_READY, TOPIC_TO_CONSUMER_MAP

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
                print(f"No Kafka broker available for topic= {topic}")
                consumers_created = False
                continue

            try:
                consumer.assign([kafka.TopicPartition(topic, 0)])
                consumer.seek_to_beginning()
            except AssertionError:
                print(f"No partitions are currently assigned for topic= {topic}")
                consumers_created = False
                continue

            TOPIC_TO_CONSUMER_MAP[topic] = consumer
            print(f"Created Kafka consumer for topic= {topic}")

        if not consumers_created:
            print("boot_kafka_consumers:: Will sleep for 2 sec and try again...")
            time.sleep(2)
    print("boot_kafka_consumers:: Created the consumers for all topics.")

    # Wait until each topic is non-empty
    empty_topic_set = set(TOPIC_TO_CONSUMER_MAP.keys())
    while empty_topic_set:
        topic = next(iter(empty_topic_set))

        data_dict = TOPIC_TO_CONSUMER_MAP[topic].poll(1.0, max_records=1)
        if data_dict:
            empty_topic_set.remove(topic)

        if empty_topic_set:
            print("boot_kafka_consumers:: Will sleep for 2 sec and try again...")
            time.sleep(2)
    print("boot_kafka_consumers:: All topics are non-empty.")

    with LOCK:
        KAFKA_CONSUMERS_READY = True


if __name__ == "__main__":
    # boot_kafka_consumers()
    threading.Thread(target=boot_kafka_consumers, daemon=True).start()

    # Start dash app
    # app.run_server(host="0.0.0.0", debug=True, port=config.DASHBOARD_PORT, dev_tools_silence_routes_logging=False)
    app.run_server(host="0.0.0.0", debug=True, port=config.DASHBOARD_PORT)

    # Clean up connections
    for consumer in TOPIC_TO_CONSUMER_MAP.values():
        consumer.close()
    print("Done")
