import os
import json

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from time import sleep
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


auth = OAuthHandler(os.environ['consumer_key'], os.environ['consumer_secret'])
auth.set_access_token(os.environ['access_token'], os.environ['access_token_secret'])

producer = None
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                                 value_serializer=lambda m: json.dumps(m).encode('ascii'))
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka Broker...")
        sleep(1)


class KafkaStream(StreamListener):
    def on_data(self, data):
        producer.send('twitter', {'tweet_body': data})
        return True

    def on_error(self, status):
        print(status)


kafkaStream = KafkaStream()
stream = Stream(auth, kafkaStream)
stream.sample()
