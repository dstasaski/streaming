import json

from time import sleep
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


consumer = None
while True:
    try:
        consumer = KafkaConsumer(topic='reddit', group_id='redditconsumer',
                                 bootstrap_servers=['kafka:9092'],
                                 value_serializer=lambda m: json.dumps(m).encode('ascii'))
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka Broker...")
        sleep(1)

# TODO logic
