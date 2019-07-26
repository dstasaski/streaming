import praw
import os
import json

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

reddit = praw.Reddit(client_id=os.environ['client_id'],
                     client_secret=os.environ['client_secret'],
                     user_agent='Insights App')

subreddit = reddit.subreddit('all')
for comment in subreddit.stream.comments(skip_existing=True):
    producer.send('reddit', {'body': comment.body})



