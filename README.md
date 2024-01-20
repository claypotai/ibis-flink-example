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
ibis-flink-example-init-kafka-1      | transaction
ibis-flink-example-init-kafka-1 exited with code 0
ibis-flink-example-data-generator-1  | Connected to Kafka
ibis-flink-example-data-generator-1  | send 1000 rows to kafka
ibis-flink-example-data-generator-1  | send 1000 rows to kafka
...
```

The `transaction` Kafka topic contains messages in the following format:

```json
 {
        "user_id": dt.int64,
        "trans_date_trans_time": dt.timestamp(scale=3),
        "cc_num": dt.int64,
        "amt": dt.float64,
        "trans_num": dt.str,
        "merchant": dt.str,
        "category": dt.str ,      
        "is_fraud": dt.int32,
        "first": dt.str,
        "last": dt.str,
        "dob": dt.str,
        "zipcode": dt.str,
    }
```

In a separate terminal, we can explore what these messages look like:

```pycon
>>> from kafka import KafkaConsumer
>>>
>>> consumer = KafkaConsumer("transaction")
>>> for _, msg in zip(range(3), consumer):
...     print(msg)
... 
0, ConsumerRecord(topic='transaction', partition=0, offset=90000, timestamp=1705686143809, timestamp_type=0, key=None, value=b'{"trans_date_trans_time": "2012-02-23 00:10:01", "cc_num": 4428780000000000000, "merchant": "fraud_Olson, Becker and Koch", "category": "gas_transport", "amt": 82.55, "first": "Richard", "last": "Waters", "zipcode": "53186", "dob": "1/2/46", "trans_num": "dbf31d83eebdfe96d2fa213df2043586", "is_fraud": 0, "user_id": 7109464218691269943}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=337, serialized_header_size=-1))
(1, ConsumerRecord(topic='transaction', partition=0, offset=90001, timestamp=1705686143809, timestamp_type=0, key=None, value=b'{"trans_date_trans_time": "2012-02-23 00:12:05", "cc_num": 6011490000000000, "merchant": "fraud_Schmitt Inc", "category": "gas_transport", "amt": 77.47, "first": "Gary", "last": "Barnes", "zipcode": "71762", "dob": "6/11/86", "trans_num": "8e2ab99602a3bc2ca943609b74b64871", "is_fraud": 0, "user_id": -3502299427506550148}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=322, serialized_header_size=-1))
(2, ConsumerRecord(topic='transaction', partition=0, offset=90002, timestamp=1705686143809, timestamp_type=0, key=None, value=b'{"trans_date_trans_time": "2012-02-23 00:14:11", "cc_num": 4826660000000000, "merchant": "fraud_Reichert, Rowe and Mraz", "category": "shopping_net", "amt": 7.12, "first": "Tami", "last": "Forbes", "zipcode": "2630", "dob": "12/29/63", "trans_num": "34f0167e9b52e6cd21911de0908a0e5e", "is_fraud": 0, "user_id": -6454535437007309845}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=332, serialized_header_size=-1))
```

## Running the example

The [included window aggregation example](create_user_features.py and create_user_features.ipynb) uses the Flink backend for Ibis to process the aforementioned credict card transaction, computing different aggregations over the last x minutes for each user.

### Local execution

You can run the example using the Flink mini cluster from your project directory:

```bash
python feature_generation/create_user_features.py --local
```

Within a few seconds, you should see messages from the `user_trans_amt_last_Xmin` topic (containing the results of your computation) flooding the screen.

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
/opt/miniconda3/envs/ibis-dev/lib/python3.10/site-packages/pyflink/bin/flink run --jobmanager localhost:8081 --python feature_generation/create_user_features.py
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
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498016, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":7584046863221534119,"user_max_trans_amt_last_360min":9.28,"user_min_trans_amt_last_360min":9.28,"user_mean_trans_amt_last_360min":9.28,"user_trans_count_last_360min":1,"trans_date_trans_time":"2012-03-05 10:02:42"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=258, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498017, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":-227411383219759975,"user_max_trans_amt_last_360min":238.98,"user_min_trans_amt_last_360min":129.62,"user_mean_trans_amt_last_360min":184.3,"user_trans_count_last_360min":2,"trans_date_trans_time":"2012-03-05 10:05:20"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=263, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498018, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":5540113790583634747,"user_max_trans_amt_last_360min":321.13,"user_min_trans_amt_last_360min":85.0,"user_mean_trans_amt_last_360min":175.16666666666663,"user_trans_count_last_360min":3,"trans_date_trans_time":"2012-03-05 10:06:44"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=274, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498019, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":5473941431289561122,"user_max_trans_amt_last_360min":65.1,"user_min_trans_amt_last_360min":65.1,"user_mean_trans_amt_last_360min":65.1,"user_trans_count_last_360min":1,"trans_date_trans_time":"2012-03-05 10:07:04"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=258, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498020, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":-8002661054777632739,"user_max_trans_amt_last_360min":100.47,"user_min_trans_amt_last_360min":100.47,"user_mean_trans_amt_last_360min":100.47,"user_trans_count_last_360min":1,"trans_date_trans_time":"2012-03-05 10:08:10"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=265, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498021, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":4533977973587865842,"user_max_trans_amt_last_360min":32.6,"user_min_trans_amt_last_360min":4.37,"user_mean_trans_amt_last_360min":18.484999999999985,"user_trans_count_last_360min":2,"trans_date_trans_time":"2012-03-05 10:09:37"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=272, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498022, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":3481231273808269436,"user_max_trans_amt_last_360min":151.31,"user_min_trans_amt_last_360min":110.31,"user_mean_trans_amt_last_360min":130.81,"user_trans_count_last_360min":2,"trans_date_trans_time":"2012-03-05 10:09:46"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=264, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498023, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":-8934990280846121883,"user_max_trans_amt_last_360min":59.72,"user_min_trans_amt_last_360min":59.72,"user_mean_trans_amt_last_360min":59.72,"user_trans_count_last_360min":1,"trans_date_trans_time":"2012-03-05 10:10:13"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=262, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498024, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":-4445344990296535100,"user_max_trans_amt_last_360min":74.49,"user_min_trans_amt_last_360min":60.65,"user_mean_trans_amt_last_360min":67.57,"user_trans_count_last_360min":2,"trans_date_trans_time":"2012-03-05 10:10:49"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=262, serialized_header_size=-1)
ConsumerRecord(topic='user_trans_amt_last_360min', partition=0, offset=498025, timestamp=1705623329919, timestamp_type=0, key=None, value=b'{"before":null,"after":{"user_id":1953379549727975020,"user_max_trans_amt_last_360min":55.26,"user_min_trans_amt_last_360min":55.26,"user_mean_trans_amt_last_360min":55.26,"user_trans_count_last_360min":1,"trans_date_trans_time":"2012-03-05 10:11:32"},"op":"c"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=261, serialized_header_size=-1)
```

## Shutting down the Compose environment

Press <kbd>Ctrl</kbd>+<kbd>C</kbd> to stop the Docker Compose containers. Once stopped, run `docker compose down` to remove the services created for this tutorial.
