import dotenv
import os


# Default values of env variables are used while running locally.
ENV_CONFIG = dotenv.dotenv_values(".env")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", default=ENV_CONFIG["DASHBOARD_PORT"]))
KAFKA_SOURCE_TOPIC = os.environ.get("KAFKA_SOURCE_TOPIC", default=ENV_CONFIG["KAFKA_SOURCE_TOPIC"])
KAFKA_FEATURE_TOPIC = os.environ.get("KAFKA_FEATURE_TOPIC", default=ENV_CONFIG["KAFKA_FEATURE_TOPIC"])
KAFKA_SERVICE = os.environ.get("KAFKA_SERVICE", default="localhost")
KAFKA_BOOTSTRAP_PORT = os.environ.get("KAFKA_BOOTSTRAP_PORT", default=f"{ENV_CONFIG['KAFKA_BOOTSTRAP_PORT']}")
INFLUXDB_SERVICE = os.environ.get("INFLUXDB_SERVICE", default="localhost")
INFLUXDB_PORT = os.environ.get("INFLUXDB_PORT", default=f"{ENV_CONFIG['INFLUXDB_PORT']}")

## Plot config
# List of time series graphs. Each time series is defined by the
# features (x, y) plotted against each other.
TOPIC_TO_FIELD_PAIRS_TO_PLOT = {
    KAFKA_SOURCE_TOPIC: [
        ("trans_date_trans_time", "amt"),
        # ("trans_date_trans_time", "cc_num"),
    ],
    KAFKA_FEATURE_TOPIC: [
        ("trans_date_trans_time", "user_max_trans_amt_last_60min"),
        ("trans_date_trans_time", "user_mean_trans_amt_last_60min"),
        # ("trans_date_trans_time", "user_trans_count_last_60min"),
    ]
}

NUM_ROWS_IN_DASHBOARD = sum(
    len(field_pairs)
    for field_pairs in TOPIC_TO_FIELD_PAIRS_TO_PLOT.values()
)
print(f"NUM_ROWS_IN_DASHBOARD= {NUM_ROWS_IN_DASHBOARD}")
DASHBOARD_HEIGHT = 300 * NUM_ROWS_IN_DASHBOARD
DASHBOARD_WIDTH = 1500

# Max number of (x, y) points plotted on each time series.
MAX_TIME_SERIES_LENGTH = 100
print(
    "Plot config: \n"
    f"TOPIC_TO_FIELD_PAIRS_TO_PLOT= \n{TOPIC_TO_FIELD_PAIRS_TO_PLOT} \n"
    f"MAX_TIME_SERIES_LENGTH= {MAX_TIME_SERIES_LENGTH} \n"
)


# Ref: https://developer.mozilla.org/en-US/docs/Web/CSS/named-color
PLOT_COLOR_LIST = [
    "blueviolet",
    "darksalmon",
    "gold",
    "darkseagreen",
    "dodgerblue",
    "lightseagreen",
    "black",
    "orangered"
]
