from cassandra.cluster import Cluster
import pulsar
from json import loads

cass_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
cass_session = cass_cluster.connect('')

pulsar_client = pulsar.Client('pulsar://localhost:6650')

consumer = pulsar_client.subscribe('test_cassandra', 'my-subscription')

user_insert_stmt = cass_session.prepare("insert into insight.twitch_live (timestamp_name, follower_count) values (?,?)");

print("After connecting to pulsar")


def insert(message):
    print(message)
    for key, value in message.items():
        return cass_session.execute(user_insert_stmt, [key, value])


while True:
    msg = consumer.receive()
    print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
    insert(loads(msg.data().decode('utf-8')))
    consumer.acknowledge(msg)

# consumer = KafkaConsumer(
#         'twitch-topic',
#         bootstrap_servers=['localhost:9092'],
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         group_id=None,
#         value_deserializer=lambda x: loads(x.decode('utf-8')))

#
# for message in consumer:
#     insert(message)
