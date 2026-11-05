#!/bin/bash
set -euo pipefail
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory 1g \
  --conf spark.jars.ivy=/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /app/spark/streaming_job.py
