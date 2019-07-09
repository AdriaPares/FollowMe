from kafka.consumer import KafkaConsumer
from cassandra.cluster import Cluster
from json import loads


def insert(consumed_message):
    for key, value in consumed_message.value.items():
        return session.execute(prepared_query, [key[:19], key[20:], value])


def get_kafka_consumer(topic: str, consumer_group: str) -> KafkaConsumer:
    return KafkaConsumer(topic,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='latest',
                         enable_auto_commit=True,
                         group_id=consumer_group,
                         value_deserializer=lambda x: loads(x.decode('utf-8')))


if __name__ == '__main__':
    cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    session = cluster.connect('')

    prepared_query = session.prepare(
        "insert into insight.twitch_live (timestamp, streamer, follower_count) values (?,?,?)")

    consumer = get_kafka_consumer('twitch-topic', 'twitch-group')
    for message in consumer:
        insert(message)


