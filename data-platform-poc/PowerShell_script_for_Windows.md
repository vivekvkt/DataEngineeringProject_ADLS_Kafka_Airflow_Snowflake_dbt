1️⃣ CREATE run_pipeline.ps1 (PowerShell script for Windows)
# ==========================================
# End-to-End Data Platform POC Automation
# Kafka → Airflow → Snowflake → dbt
# ==========================================

# 1️⃣ Start Docker Services
Write-Output "Starting Docker services..."
docker compose up -d

Start-Sleep -Seconds 15
Write-Output "Docker services started."

# 2️⃣ Check Docker services
docker compose ps

# 3️⃣ Run Kafka Producer
Write-Output "Running Kafka producer..."
python ./kafka/producer.py

# 4️⃣ Trigger Airflow DAG
Write-Output "Triggering Airflow DAG..."
# Install airflow CLI if needed
# The DAG will consume Kafka and load into Snowflake
docker exec -it $(docker ps -q -f "ancestor=apache/airflow:2.9.0") airflow dags trigger kafka_to_snowflake

# Wait for DAG to finish
Write-Output "Waiting 30 seconds for Airflow DAG to complete..."
Start-Sleep -Seconds 30

# 5️⃣ Run DBT transformations
Write-Output "Running DBT transformations..."
cd ./dbt
dbt run
cd ..

Write-Output "End-to-End pipeline completed!"
Write-Output "Check Power BI dashboard connected to Snowflake for analytics."

2️⃣ HOW IT WORKS

Starts Docker → Zookeeper, Kafka, Airflow

Runs Kafka producer → pushes sample JSON orders

Triggers Airflow DAG → consumes Kafka → loads Snowflake

Runs DBT models → staging + fact table

✅ Everything is ready for Power BI visualization

3️⃣ HOW TO RUN

Open PowerShell in project root (data-platform-poc/):

.\run_pipeline.ps1