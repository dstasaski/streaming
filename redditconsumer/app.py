import json

from time import sleep
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


consumer = None
while True:
    try:
        consumer = KafkaConsumer('reddit', group_id='redditconsumer',
                                 bootstrap_servers=['kafka:9092'],
                                 value_deserializer=lambda m: json.dumps(m).encode('ascii'))
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka Broker...")
        sleep(1)

print('FOUND KAFKA')

for message in consumer:
    print(message.value)
