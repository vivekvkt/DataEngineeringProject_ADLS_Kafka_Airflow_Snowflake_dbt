from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    #bootstrap_servers='localhost:9092',
    bootstrap_servers='data-platform-poc-kafka-1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

from datetime import datetime
orders = [
    {
        "order_id": 1,
        "customer": "Amit",
        "amount": 1200,
        "event_time": datetime.utcnow().isoformat()
    },
    {
        "order_id": 2,
        "customer": "Neha",
        "amount": 850,
        "event_time": datetime.utcnow().isoformat()
    },
    {
        "order_id": 3,
        "customer": "Ravi",
        "amount": 420,
        "event_time": datetime.utcnow().isoformat()
    },
    {
        "order_id": 4,
        "customer": "vivek",
        "amount": 450,
        "event_time": datetime.utcnow().isoformat()
    },
    {
        "order_id": 5,
        "customer": "Anup",
        "amount": 460,
        "event_time": datetime.utcnow().isoformat()
    },
     {
        "order_id": 6,
        "customer": "Anup",
        "amount": 1200,
        "event_time": datetime.utcnow().isoformat()
    }
]

for order in orders:
    producer.send('orders', order)
    print("Sent:", order)
    time.sleep(1)

producer.flush()
