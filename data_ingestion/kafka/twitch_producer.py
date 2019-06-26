from time import sleep
from kafka import KafkaProducer
import boto3
import json
import datetime as dt

# This version reads file from local

# this needs a timestamp
# and also a try/except for JSON DECODE ERROR (if the topic gets thrown bad stuff it will stop the consumer)

s3 = boto3.resource('s3')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

folder_path = './simulated_data/'
streamer_name = 'ninja'
platform = 'twitch'
initial_timestamp = '2019-06-25_13-21-33_'
time_format = '%Y-%m-%d_%H-%M-%S_'
second_counter = 0
while True:

    current_timestamp = dt.datetime.strftime(dt.datetime.strptime(initial_timestamp, time_format) +
                                             dt.timedelta(seconds=second_counter), time_format)

    content_object = s3.Object('insight-api-dumps', current_timestamp + platform + streamer_name + '.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    # with open(folder_path + current_timestamp + platform + streamer_name + '.json') as f:
        # json_content = json.load(f)
    producer.send('twitch-topic', value={current_timestamp + streamer_name: json_content['total']})
