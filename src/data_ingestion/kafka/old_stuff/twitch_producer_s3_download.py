from time import sleep
from kafka import KafkaProducer
import boto3
import json
import datetime as dt
import time


def get_simulation_timestamps(file_name='_twitch_testing.json',
                              sim_number=86400, time_format='%Y-%m-%d_%H-%M-%S'):
    initial_time = '2019-06-16_00-00-00'
    for i in range(sim_number):
        yield (i, (dt.datetime.strptime(initial_time, time_format) + dt.timedelta(seconds=i + 1))
               .strftime(time_format) + file_name)


s3 = boto3.resource('s3')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))
init = time.time()
for counter, s3_file in get_simulation_timestamps(sim_number=1000):
    content_object = s3.Object('insight-api-dumps', 'dummy_batch_dump/twitch_dummy/' + s3_file)
    file_content = content_object.download_file('/tmp/' + s3_file)
    with open('/tmp/' + s3_file) as f:
        json_content = json.load(f)
    producer.send('twitch_topic', value={'total_twitch_followers_' + str(counter): json_content['total']})
print(init - time.time())
# this needs a timestamp


print('Done')
sleep(50)
# for e in range(1000):
#     data = {'number': e}
#     producer.send('twitter_topic', value=data)
#     sleep(5)
