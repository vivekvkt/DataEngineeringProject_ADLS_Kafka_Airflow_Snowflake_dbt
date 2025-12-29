1️⃣ START DOCKER SERVICES

From your project root (data-platform-poc/):

docker compose up -d


✅ This will start:

Zookeeper

Kafka

Airflow

Check status:

docker compose ps


You should see Up for all services.

2️⃣ CHECK AIRFLOW

Open browser → http://localhost:8080

Login: admin / admin

You should see Airflow UI running.

3️⃣ CREATE SNOWFLAKE TABLES

Login to Snowflake UI or SnowSQL CLI

Run snowflake/init.sql:

CREATE DATABASE DEMO_DB;
USE DATABASE DEMO_DB;

CREATE SCHEMA PUBLIC;

CREATE TABLE RAW_ORDERS (
    ORDER_ID INT,
    CUSTOMER STRING,
    AMOUNT FLOAT,
    EVENT_TIME TIMESTAMP
);


✅ This creates the raw table for incoming Kafka events.

4️⃣ RUN KAFKA PRODUCER

From your project root:

python kafka/producer.py


This will produce sample JSON events into Kafka topic orders.
You should see output like:

Sent: {'order_id': 1, 'customer': 'Amit', 'amount': 1200}
Sent: {'order_id': 2, 'customer': 'Neha', 'amount': 850}
Sent: {'order_id': 3, 'customer': 'Ravi', 'amount': 420}

5️⃣ RUN AIRFLOW DAG

In Airflow UI → DAGs → kafka_to_snowflake

Trigger DAG manually (click “Trigger DAG”)

DAG will:

Consume Kafka messages

Load into Snowflake RAW_ORDERS table

Check logs → you should see rows inserted.

6️⃣ RUN DBT TRANSFORMATIONS

From your dbt/ folder:

dbt run


This will:

Create stg_orders (staging)

Create fct_orders (fact table)

Check Snowflake → FCT_ORDERS → data transformed.

7️⃣ CONNECT POWER BI

Open Power BI Desktop

Connect → Snowflake

Server: YOUR_ACCOUNT.snowflakecomputing.com

Database: DEMO_DB

Schema: PUBLIC

Load table → FCT_ORDERS

Build visuals:

Total revenue by customer

Order counts

Revenue trends over time

8️⃣ FINAL CHECK

Kafka producing → ✅

Airflow consuming → ✅

Snowflake loaded → ✅

dbt transformed → ✅

Power BI dashboard → ✅

✅ COMMAND SUMMARY
# 1. Start Docker services
docker compose up -d

# 2. Check services
docker compose ps

# 3. Run Kafka producer
python kafka/producer.py

# 4. Trigger Airflow DAG
# (via Airflow UI)

# 5. Run DBT models
cd dbt
dbt run

# 6. Open Power BI and visualize
# Connect to Snowflake and load 'FCT_ORDERS'


-------------------------------------------------------
1️⃣ Enter your Airflow container
docker exec -it data-platform-poc-airflow bash

2️⃣ Initialize the database (if not already done)
airflow db init


This ensures the metadata DB is properly set up.

3️⃣ Create an admin user

Run inside the container:

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin


You can change username and password as per your preference.

--role Admin is important to allow full access.

4️⃣ Restart the Airflow container

Exit the container and restart:

docker restart data-platform-poc-airflow

5️⃣ Access Airflow UI

Go to: http://localhost:8080

Username: admin

Password: admin (or what you set)
-----------------------------------------------------