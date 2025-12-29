import csv
import json
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('/opt/project/sample-data/orders.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        row['amount'] = float(row['amount'])
        row['event_time'] = row['event_time'] or datetime.utcnow().isoformat()

        producer.send('orders', row)
        print("Sent:", row)

producer.flush()
producer.close()
