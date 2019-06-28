from time import sleep
from kafka import KafkaProducer
import boto3
import json
import datetime as dt

# This version reads file from local

# this needs a timestamp
# and also a try/except for JSON DECODE ERROR (if the topic gets thrown bad stuff it will stop the consumer)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

s3 = boto3.resource('s3')

with open('random_accounts.json') as f:
    streamer_names = json.load(f)

platform = 'twitch'
initial_file_timestamp = '2000-01-01_00-00-00_'
time_format = '%Y-%m-%d_%H-%M-%S_'
initial_producer_timestamp = dt.datetime.now().strftime(time_format)
second_counter = 0
while True:
    current_file_timestamp = dt.datetime.strftime(dt.datetime.strptime(initial_file_timestamp, time_format)
                                                  + dt.timedelta(seconds=second_counter), time_format)

    current_producer_timestamp = dt.datetime.strftime(dt.datetime.strptime(initial_producer_timestamp, time_format)
                                                      + dt.timedelta(seconds=second_counter), time_format)
    second_counter += 1

    # Add a try/except here if the S3 file doesn't exist
    for streamer_name in streamer_names.keys():
        content_object = s3.Object('insight-api-dumps', current_file_timestamp + platform + '_' + streamer_name + '.json')
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        producer.send(platform+'-topic', value={current_producer_timestamp + streamer_name: json_content['total']})
