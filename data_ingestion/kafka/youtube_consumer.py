from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import datetime
import time
from json import loads

cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
session = cluster.connect('')

user_insert_stmt = session.prepare("insert into insight.youtube_live (timestamp_name, subscriber_count) values (?,?)");


print("After connecting to kafka")


def insert(message):
    print(message.value)
    for key, value in message.value.items():
        return session.execute(user_insert_stmt, [key, value])


consumer = KafkaConsumer(
        'youtube-topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='youtube-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))


for message in consumer:
    insert(message)
