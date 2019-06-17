from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import datetime
import time
from json import loads

cluster = Cluster(['10.0.0.4', '10.0.0.5', '10.0.0.6', '10.0.0.14'])

#cluster = Cluster()
session = cluster.connect('tutorialspoint')

user_insert_stmt = session.prepare("insert into kafkatest (name,subs) values (?,?)");
 
#kafka = KafkaClient("localhost:9092")
 
print("After connecting to kafka")
 
#consumer = SimpleConsumer(kafka, "test-consumer-group", "my-topic")
 
consumer = KafkaConsumer(
        'my-topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=None,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

def insert(message):
    print (message.value)
    for key, value in message.value.items():    
        return session.execute(user_insert_stmt, [key,value])
 
for message in consumer:
    insert (message)

