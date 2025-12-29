# kafka/producer_json.py
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('/opt/project/sample-data/orders.json') as f:
    orders = json.load(f)

for order in orders:
    producer.send('orders', order)
    print("Sent:", order)

producer.flush()
producer.close()
