from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
import snowflake.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='kafka_to_snowflake',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# -----------------------------
# Kafka → Snowflake task
# -----------------------------
def consume_kafka_to_snowflake():
    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=10000,  # 🔑 stops consumer after idle time
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    ctx = snowflake.connector.connect(
        user='',
        password='',
        account='',
        warehouse='COMPUTE_WH',
        database='DEMO_DB',
        schema='PUBLIC'
    )

    cs = ctx.cursor()

    inserted_rows = 0
    for message in consumer:
        order = message.value
        cs.execute(
            """
            INSERT INTO DEMO_DB.PUBLIC.RAW_ORDERS
            (ORDER_ID, CUSTOMER, AMOUNT, EVENT_TIME)
            VALUES (%s, %s, %s, %s)
            """,
            (
                order['order_id'],
                order['customer'],
                order['amount'],
                order['event_time']
            )
        )
        inserted_rows += 1

    print(f"Inserted {inserted_rows} rows into RAW_ORDERS")

    cs.close()
    ctx.close()


consume_kafka = PythonOperator(
    task_id='consume_kafka',
    python_callable=consume_kafka_to_snowflake,
    dag=dag
)

# -----------------------------
# dbt run task
# -----------------------------
run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command="""
    cd /opt/project/dbt &&
    export DBT_NO_GIT=1 &&
    dbt run --profiles-dir .
    """,
    dag=dag
)

# -----------------------------
# Task dependency
# -----------------------------
consume_kafka >> run_dbt
