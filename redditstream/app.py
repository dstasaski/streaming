import praw
import os
import json

from time import sleep

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


producer = None

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka Broker...")
        sleep(5)


reddit = praw.Reddit(client_id=os.environ['client_id'],
                     client_secret=os.environ['client_secret'],
                     user_agent='Insights App')

subreddit = reddit.subreddit('all')
for comment in subreddit.stream.comments(skip_existing=True):
    producer.send('reddit', {'body': comment.body})
    print("we made it!")



