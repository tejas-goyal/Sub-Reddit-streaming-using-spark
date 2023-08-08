import json
from kafka import KafkaConsumer

# Kafka Consumer configuration
consumer = KafkaConsumer('reddit_stream',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('ascii')),
                         auto_offset_reset='latest')

# Start consuming messages
for message in consumer:
    print(message.value)
