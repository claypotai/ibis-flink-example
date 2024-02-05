FROM python:3.10.13

ARG DASHBOARD_PORT

WORKDIR /app

COPY dashboard .env /app

RUN pip install --upgrade pip
RUN pip install --no-cache-dir \
dash \
gunicorn \
kafka-python \
pandas \
plotly \
python-dotenv \
influxdb-client

EXPOSE ${DASHBOARD_PORT}

# Note: Gunicorn redirects the logs to a file by default
# CMD ["sh", "-c", "gunicorn dashboard_w_influxdb:flask_server --bind=0.0.0.0:${DASHBOARD_PORT}"]
CMD ["python", "-u", "dashboard_w_influxdb.py"]
