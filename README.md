# Ibis Flink backend example

This repository contains a simple, self-contained example using the Apache Flink backend for Ibis.

## Installation prerequisites

* **Docker Compose:** This tutorial uses Docker Compose to manage an Apache Kafka environment (including sample data generation) and a Flink cluster (for [remote execution](#remote-execution)). You can [download and install Docker Compose from the official website](https://docs.docker.com/compose/install/).
* **JDK 11:** Flink requires Java 11. If you don't already have JDK 11 installed, you can [get the appropriate Eclipse Temurin release](https://adoptium.net/temurin/releases/?package=jdk&version=11).
* **Python:** To follow along, you need Python 3.9 or 3.10.

## Installing the Flink backend for Ibis

We strongly recommend creating a virtual environment for your project. In your virtual environment, install the dependencies from the requirements file:

```bash
python -m pip install -r requirements.txt
```

> [!CAUTION]
> The Flink backend for Ibis is unreleased. Please check back in early 2024 if you feel more comfortable using a released version.

## Spinning up the services using Docker Compose

From your project directory, run `docker compose up` to create Kafka topics, generate sample data, and launch a Flink cluster.

> [!TIP]
> If you don't intend to try [remote execution](#remote-execution), you can start only the Kafka-related services with `docker compose up kafka init-kafka data-generator`.

After a few seconds, you should see messages indicating your Kafka environment is ready:

```bash
ibis-flink-example-init-kafka-1      | Successfully created the following topics:
ibis-flink-example-init-kafka-1      | payment_msg
ibis-flink-example-init-kafka-1      | sink
ibis-flink-example-init-kafka-1 exited with code 0
ibis-flink-example-data-generator-1  | Connected to Kafka
ibis-flink-example-data-generator-1  | Producing 20000 records to Kafka topic payment_msg
```

The `payment_msg` Kafka topic contains messages in the following format:

```json
{
    "createTime": "2023-09-20 22:19:02.224",
    "orderId": 1695248388,
    "payAmount": 88694.71922270155,
    "payPlatform": 0,
    "provinceId": 6
}
```

In a separate terminal, we can explore what these messages look like:

```pycon
>>> from kafka import KafkaConsumer
>>>
>>> consumer = KafkaConsumer("payment_msg")
>>> for _, msg in zip(range(3), consumer):
...     print(msg)
... 
ConsumerRecord(topic='payment_msg', partition=0, offset=628, timestamp=1702073942808, timestamp_type=0, key=None, value=b'{"createTime": "2023-12-08 22:19:02.808", "orderId": 1702074256, "payAmount": 79901.88673289565, "payPlatform": 1, "provinceId": 1}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=131, serialized_header_size=-1)
ConsumerRecord(topic='payment_msg', partition=0, offset=629, timestamp=1702073943310, timestamp_type=0, key=None, value=b'{"createTime": "2023-12-08 22:19:03.309", "orderId": 1702074257, "payAmount": 34777.62234573957, "payPlatform": 0, "provinceId": 3}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=131, serialized_header_size=-1)
ConsumerRecord(topic='payment_msg', partition=0, offset=630, timestamp=1702073943811, timestamp_type=0, key=None, value=b'{"createTime": "2023-12-08 22:19:03.810", "orderId": 1702074258, "payAmount": 17101.347666982423, "payPlatform": 0, "provinceId": 2}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=132, serialized_header_size=-1)
```

## Running the example

The [included window aggregation example](window_aggregation.py) uses the Flink backend for Ibis to process the aforementioned payment messages, computing the total pay amount per province in the past 10 seconds (as of each message, for the province in the incoming message).

### Local execution

You can run the example using the Flink mini cluster from your project directory:

```bash
python window_aggregation.py local
```

Within a few seconds, you should see messages from the `sink` topic (containing the results of your computation) flooding the screen.

> [!NOTE]
> The mini cluster shuts down as soon as the Python session ends, so we print the Kafka messages until the process is cancelled (e.g. with <kbd>Ctrl</kbd>+<kbd>C</kbd>).

### Remote execution

You can also submit the example to the [remote cluster started using Docker Compose](#spinning-up-the-services-using-docker-compose). We will [use the method described in the official Flink documentation](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/deployment/cli/#submitting-pyflink-jobs).

> [!TIP]
> You can find the `./bin/flink` executable with the following command:
> 
> ```bash
> python -c'from pathlib import Path; import pyflink; print(Path(pyflink.__spec__.origin).parent / "bin" / "flink")'
> ```

My full command looks like this:

```bash
/opt/miniconda3/envs/ibis-dev/lib/python3.10/site-packages/pyflink/bin/flink run --jobmanager localhost:8081 --python window_aggregation.py
```

The command will exit after displaying a submission message:

```bash
Job has been submitted with JobID b816faaf5ef9126ea5b9b6a37012cf56
```

Similar to how we viewed messages in the `payment_msg` topic, we can print results from the `sink` topic:

```pycon
>>> from kafka import KafkaConsumer
>>> 
>>> consumer = KafkaConsumer("sink")
>>> for _, msg in zip(range(10), consumer):
...     print(msg)
... 
ConsumerRecord(topic='sink', partition=0, offset=8264, timestamp=1702076548075, timestamp_type=0, key=None, value=b'{"province_id":1,"pay_amount":102381.88254099473}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=49, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8265, timestamp=1702076548480, timestamp_type=0, key=None, value=b'{"province_id":1,"pay_amount":114103.59313794877}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=49, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8266, timestamp=1702076549085, timestamp_type=0, key=None, value=b'{"province_id":5,"pay_amount":65711.48588438489}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=48, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8267, timestamp=1702076549488, timestamp_type=0, key=None, value=b'{"province_id":3,"pay_amount":388965.01567530684}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=49, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8268, timestamp=1702076550098, timestamp_type=0, key=None, value=b'{"province_id":4,"pay_amount":151524.24311058817}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=49, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8269, timestamp=1702076550502, timestamp_type=0, key=None, value=b'{"province_id":2,"pay_amount":290018.422116076}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=47, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8270, timestamp=1702076550910, timestamp_type=0, key=None, value=b'{"province_id":5,"pay_amount":47098.24626524143}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=48, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8271, timestamp=1702076551516, timestamp_type=0, key=None, value=b'{"province_id":4,"pay_amount":155309.68873659955}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=49, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8272, timestamp=1702076551926, timestamp_type=0, key=None, value=b'{"province_id":2,"pay_amount":367397.8759861871}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=48, serialized_header_size=-1)
ConsumerRecord(topic='sink', partition=0, offset=8273, timestamp=1702076552530, timestamp_type=0, key=None, value=b'{"province_id":3,"pay_amount":182191.45302431137}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=49, serialized_header_size=-1)
```

## Shutting down the Compose environment

Press <kbd>Ctrl</kbd>+<kbd>C</kbd> to stop the Docker Compose containers. Once stopped, run `docker compose down` to remove the services created for this tutorial.
