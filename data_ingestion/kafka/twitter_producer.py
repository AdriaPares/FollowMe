from time import sleep
from kafka import KafkaProducer
import boto3
import json

s3 = boto3.resource('s3')

content_object = s3.Object('insight-api-dumps', 'twitter_dump/twitter_72hrs.json')
file_content = content_object.get()['Body'].read().decode('utf-8')
json_content = json.loads(file_content)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

producer.send('twitter-topic', value={'total_twitter_followers': json_content['followers_count']})
# this needs a timestamp


print('Done')
# for e in range(1000):
#     data = {'number': e}
#     producer.send('twitter_topic', value=data)
#     sleep(5)
