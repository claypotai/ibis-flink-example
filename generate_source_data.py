# import calendar
import random
# import time
from datetime import datetime
from json import dumps
# from random import randint
from time import sleep
import csv
from io import StringIO
import requests

from kafka import KafkaProducer, errors


# def write_data(producer):
#     data_cnt = 20000
#     order_id = calendar.timegm(time.gmtime())
#     max_price = 100000
#     topic = "payment_msg"

#     print(f"Producing {data_cnt} records to Kafka topic {topic}")
#     for _ in range(data_cnt):
#         ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
#         rd = random.random()
#         order_id += 1
#         pay_amount = max_price * rd
#         pay_platform = 0 if random.random() < 0.9 else 1
#         province_id = randint(0, 6)
#         cur_data = {
#             "createTime": ts,
#             "orderId": order_id,
#             "payAmount": pay_amount,
#             "payPlatform": pay_platform,
#             "provinceId": province_id,
#         }
#         producer.send(topic, value=cur_data)
#         sleep(0.5)
#     print("done")

def write_fraud_detection_data_from_s3(producer):

    topic = "transaction"
    batch_size = 1000

    bucket_name = "claypot-fraud-detection"
    object_key = "FraudTransactions.csv"

    # Generate the S3 URL
    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"

    # Send an HTTP GET request to download the file
    response = requests.get(s3_url)
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Now, response.text contains the content of the CSV file in memory
        csv_data = response.text
        # You can use csv_data as needed in your script
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
    print("Loaded data from s3")

    # Use StringIO to treat the string data as a file-like object
    csv_file = StringIO(csv_data)

    # Read the CSV file line by line, skipping the first row
    reader = csv.reader(csv_file)
    keys = next(reader)

    print(f"Send records to Kafka topic {topic}")
    cnt = 0
    for values in reader:
        # values = next(reader)
        data_dict = dict(zip(keys, values))
        data_dict['trans_date_trans_time'] = datetime.utcfromtimestamp(int(data_dict['unix_time'])//1000).strftime('%Y-%m-%d %H:%M:%S')
        int_variables = ['cc_num', 'city_pop', 'unix_time', 'is_fraud']
        float_vaibles = ['amt', 'latitude', 'longitude', 'merch_lat', 'merch_long']
        for var in int_variables:
            data_dict[var] = int(data_dict[var])
        for var in float_vaibles:
            data_dict[var] = float(data_dict[var])
        data_dict['user_id'] = hash(data_dict['last'] + data_dict['first'] + data_dict['dob'])
        used = ['user_id', 'trans_date_trans_time', 'cc_num', 'amt', 'trans_num', 'merchant', 'category', 'is_fraud', 'first', 'last', 'dob', 'zipcode']
        data_dict = {key: value for key, value in data_dict.items() if key in used}

        producer.send(topic, value=data_dict)
        cnt += 1
        if cnt == batch_size:
            producer.flush()
            print(f"send {cnt} rows to kafka")
            cnt = 0
            sleep(1)
    if cnt > 0:
        producer.flush()
    producer.close()


def create_producer():
    print("Connecting to Kafka brokers")
    for _i in range(6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka:29092"],
                value_serializer=lambda x: dumps(x).encode("utf-8"),
            )
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(10)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")


if __name__ == "__main__":
    producer = create_producer()
    # write_data(producer)
    # fraud detection dataset
    write_fraud_detection_data_from_s3(producer)
