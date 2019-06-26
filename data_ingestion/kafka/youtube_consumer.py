from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from cassandra.cluster import Cluster
import datetime
import time
from json import loads

cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
session = cluster.connect('')

prepared_query = session.prepare("insert into insight.youtube_live (streamer, timestamp, follower_count) values (?,?,?)");

print("After connecting to kafka")


def insert(consumed_message):
    for key, value in consumed_message.value.items():
        return session.execute(prepared_query, [key[:19], key[20:], value])


consumer = KafkaConsumer(
        'youtube-topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='youtube-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))


for message in consumer:
    insert(message)
