from azure.storage.filedatalake import DataLakeServiceClient
from kafka import KafkaProducer
import json
import csv
import io
import os

# -----------------------------
# CONFIG
# -----------------------------
ADLS_ACCOUNT_URL = "https://myadlsaccount.dfs.core.windows.net"
FILE_SYSTEM = "raw-data"
FILE_PATH = "orders/orders_2025_12_29.csv"   # can be .csv or .json
KAFKA_TOPIC = "orders"
KAFKA_BROKER = "kafka:9092"

# -----------------------------
# ADLS CONNECTION
# -----------------------------
service_client = DataLakeServiceClient(
    account_url=ADLS_ACCOUNT_URL,
    credential={
        "tenant_id": "TENANT_ID",
        "client_id": "CLIENT_ID",
        "client_secret": "CLIENT_SECRET"
    }
)

fs_client = service_client.get_file_system_client(FILE_SYSTEM)
file_client = fs_client.get_file_client(FILE_PATH)

# -----------------------------
# DOWNLOAD FILE
# -----------------------------
content = file_client.download_file().readall().decode("utf-8")

# -----------------------------
# PARSE FILE (CSV / JSON)
# -----------------------------
if FILE_PATH.endswith(".csv"):
    records = list(csv.DictReader(io.StringIO(content)))
elif FILE_PATH.endswith(".json"):
    records = json.loads(content)
else:
    raise ValueError("Unsupported file format")

# -----------------------------
# KAFKA PRODUCER (SAFE CONFIG)
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",        # wait for broker acknowledgment
    retries=3          # retry on transient failures
)

source_file = os.path.basename(FILE_PATH)

# -----------------------------
# SEND TO KAFKA
# -----------------------------
for record in records:
    record["source_file"] = source_file  # 🔑 lineage / dedup support
    producer.send(KAFKA_TOPIC, record)

producer.flush()
producer.close()

print(f"Sent {len(records)} records from {source_file} to Kafka topic '{KAFKA_TOPIC}'")
