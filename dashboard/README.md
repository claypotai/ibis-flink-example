# Dashboard: Feature values over time

Basic `dash` app that plots the values for a given set of features over time.
It displays a separate graph for each feature.
It reads the feature values from the records pulled from Kafka.

**Note**: File directories in this README start with `/`, which denotes the
root of the demo repo, e.g., `/dashboard` refers to this folder.

## How to run

As every other service in the demo, dashboard app runs in a container.
It is defined as `dashboard` in `/compose.yaml`.
The following command will build the necessary container images and start the `dashboard` app together with the other services:
```bash
$ docker-compose up --build
```

To clean up, the services can be brought down with
```bash
$ docker-compose down
```

## How to configure

Dashboard app takes the configuration information through env (environment) variables.
Env variables for all services are defined in `/.env`, and passed to the corresponding services in `/compose.yaml`.
Description for the env variables is given with comments in `/.env`.
Under normal circumstances, values for the env variables should not be changed.

Graphs displayed on the dashboard are defined and configured in `dashboard/dashboard.py`.
To learn how to configure the graphs, refer to the global variables defined in `dashboard/dashboard.py` and `dashboard/config.py`, and the inlined comments added for them.


## Aligning time windows across the graphs

### Restarting the dashboard

Dashboard is a basic app that pulls records from given Kafka topics, reads the specified feature values, and plots them on separate plots.
This simplicity is desired to keep the resource requirements of dashboard low enough for a "smooth" run on a laptop.
However, due to this simplicity, the x-axes between the plots might be misaligned.
For instance, `user_max_trans_amt_last_60min` is defined as the maximum `amt` in the last `60` mins.
However, when the plots for `user_max_trans_amt_last_60min` and `amt` are not aligned on the same on the same time window (given in terms of `trans_date_trans_time`),
it would not be possible to see that `user_max_trans_amt_last_60min` values are indeed the max over past `amt` values.

If the time windows happen to be non-overlapping when you start the dashboard, you can restart the dashboard by following the steps below.
Restarting the dashboard resets the read pointers to the beginning of the Kafka topics that contains the feature values.

1. Kill the existing dashboard container
```bash
$ docker-compose down dashboard
```

2. Re-create the dashboard container
```bash
$ docker-compose down dashboard
```

3. Visit `http://127.0.0.1:8050/` on your browser and see the plots restarted from the earliest records available in the Kafka topics.


### Switch to the dashboard with `InfluxDB`

Aligning the time window acrosss all the graphs requires an intermediate database.
This option is implemented in `dashboard/dashboard_w_influxdb.py`. As the file name suggests, it relies on an `InfluxDB` instance.
This intermediate timeseries database enables the dashboard app to
(1) store the feature values as needed and
(2) execute time range queries against the database to align multiple plots in their x-axes.

There two downsides of this option:
- The additional database increases the resource requirement, especially in terms of memory.
According to our experience on a laptop (M2 Mac), at least 12 GB of memory should be allocated to the Docker environment for a smooth run.

- The app needs to run a query against the database every time the plots are regenerated . Thus the latency of the query is directly reflected on the graph refresh rate.
This naturally limits the rate in which the plots can progress over the x-axis.

Here are the steps needed to run `dashboard_w_influxdb`:
1. Start the required docker services including `dashboard_w_influxdb`:
```bash
$ docker-compose -f compose_w_influxdb.yaml up
```

2. Generate the features using one of these options: (1) Run the demo on the notebook, (2) Run the generator with the following command
```bash
$ python feature_generation/create_user_features.py --local
```

4. Visit `http://127.0.0.1:8050/` on your browser and see the graphs.
