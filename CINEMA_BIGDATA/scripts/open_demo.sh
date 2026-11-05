#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/resume_spark.sh
# Explicit services: never restart the one-shot online producer.
docker compose up -d --no-deps kafka-ui postgres spark-streaming generator-api counter-api
docker compose up -d airflow-webserver airflow-scheduler
docker compose exec -T spark-streaming python3 -m scripts.customer_views
echo 'Airflow: http://localhost:18080 | Kafka: http://localhost:8888 | Spark: http://localhost:4040'
echo 'Airflow login: AIRFLOW_ADMIN_USER and AIRFLOW_ADMIN_PASSWORD in local .env'
