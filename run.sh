#!/bin/bash

# Utility script for running individual demo components.
# Meant to be used ONLY for debugging purposes.

if [ $1 = "dl" ]; then
  # python3 dashboard/dashboard.py
  python3 dashboard/dashboard_w_influxdb.py

elif [ $1 = "dd" ]; then
  docker-compose up dashboard --build

elif [ $1 = "diu" ]; then
  # docker-compose -f compose_w_influxdb.yaml up dashboard_w_influxdb --build
  docker-compose -f compose_w_influxdb.yaml up --build

elif [ $1 = "did" ]; then
  docker-compose -f compose_w_influxdb.yaml down

elif [ $1 = "ci" ]; then
  docker exec -it influxdb sleep 2 && influx config rm dashboard_config && influx setup --name dashboard_config --username dashboard --password dashboard --token dashboard --org dashboard --bucket dashboard --force

elif [ $1 = "f" ]; then
  python feature_generation/create_user_features.py --local

elif [ $1 = "s" ]; then
  influx config rm dashboard_config
  influx setup \
	 --name dashboard_config \
	 --username dashboard \
	 --password dashboard \
	 --token dashboard \
	 --org dashboard \
	 --bucket dashboard \
	 --force

else
  echo "Unexpected arg= $1"
fi
