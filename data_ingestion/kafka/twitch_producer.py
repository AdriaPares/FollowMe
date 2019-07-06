from kafka import KafkaProducer
import boto3
import json
import datetime as dt


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def get_streamer_names(file_name: str) -> dict:
    with open(file_name) as f:
        streamers = json.load(f)
    return streamers


if __name__ == '__main__':

    s3 = boto3.resource('s3')
    platform = 'twitch'
    initial_file_timestamp = '2000-01-01_00-00-00_'
    time_format = '%Y-%m-%d_%H-%M-%S_'
    initial_producer_timestamp = dt.datetime.now().strftime(time_format)
    second_counter = 0

    streamer_names = get_streamer_names('random_accounts.json')
    producer = get_kafka_producer()

    while True:
        current_file_timestamp = dt.datetime.strftime(dt.datetime.strptime(initial_file_timestamp, time_format)
                                                      + dt.timedelta(seconds=second_counter), time_format)

        current_producer_timestamp = dt.datetime.strftime(dt.datetime.strptime(initial_producer_timestamp, time_format)
                                                          + dt.timedelta(seconds=second_counter), time_format)
        second_counter += 1

        try:
            for streamer_name in streamer_names.keys():
                content_object = s3.Object('insight-api-dumps',
                                           current_file_timestamp + platform + '_' + streamer_name + '.json')
                file_content = content_object.get()['Body'].read().decode('utf-8')
                json_content = json.loads(file_content)
                producer.send(platform+'-topic', value={current_producer_timestamp + streamer_name: json_content['total']})
        except:
            print('Unknown Error in S3')
