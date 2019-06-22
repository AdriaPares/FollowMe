from time import sleep
from kafka import KafkaProducer
import boto3
import json

s3 = boto3.resource('s3')

content_object = s3.Object('insight-api-dumps', 'youtube_dump/youtube_72hrs.json')
file_content = content_object.get()['Body'].read().decode('utf-8')
json_content = json.loads(file_content)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

producer.send('youtube-topic',
              value={'total_youtube_subscribers': int(json_content['items'][0]['statistics']['subscriberCount'])})
# this needs a timestamp


print('Done')
sleep(50)
# for e in range(1000):
#     data = {'number': e}
#     producer.send('twitter_topic', value=data)
#     sleep(5)
