from kafka import KafkaProducer
import json
import datetime as dt
import random
import time
import sys


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def get_streamer_names(file_name: str) -> dict:
    with open(file_name) as f:
        streamers = json.load(f)
    return streamers


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
    time_format = '%Y-%m-%d_%H-%M-%S_'
    current_producer_timestamp = dt.datetime.now().strftime(time_format)

    streamer_names = get_streamer_names('accounts_info.json')
    producer = get_kafka_producer()

    t = time.time()
    while True:
        for streamer_name, platform_data in streamer_names.items():
            choice = random.uniform(0.99, 1.01)
            platform_data['platform_data'][platform]['total_followers'] *= choice
            producer.send(platform+'-topic', value={current_producer_timestamp + streamer_name:
                                                    int(platform_data['platform_data'][platform]['total_followers'])})

        current_producer_timestamp = dt.datetime.strftime(dt.datetime.strptime(current_producer_timestamp, time_format)
                                                          + dt.timedelta(seconds=1), time_format)
        print(time.time() - t)
        t = time.time()
        time.sleep(0.95)  # This only remains while we scale up the data
