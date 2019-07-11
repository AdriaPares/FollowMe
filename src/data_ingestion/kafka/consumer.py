from kafka.consumer import KafkaConsumer
from cassandra.cluster import Cluster
from json import loads
import sys


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


def get_platform(admitted_platforms: list = ['twitch', 'twitter', 'youtube']):
    # sys.argv[0] is the name of the script, always present
    if len(sys.argv) == 1 or len(sys.argv) > 2:
        print('Producer requires exactly one argument. Options available:')
        for plat in admitted_platforms:
            print(plat)
        sys.exit()
    else:
        platform_passed = str(sys.argv[1])
        if platform_passed not in admitted_platforms:
            print('Unrecognized platform' + platform_passed + '. Options available:')
            for plat in admitted_platforms:
                print(plat)
            sys.exit()
        else:
            return platform_passed


if __name__ == '__main__':

    platform = get_platform()
    cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    session = cluster.connect('')

    prepared_query = session.prepare(
        "insert into insight." + platform + "_live (timestamp, streamer, follower_count) values (?,?,?)")

    consumer = get_kafka_consumer(platform + '-topic', platform + '-group')
    for message in consumer:
        insert(message)
