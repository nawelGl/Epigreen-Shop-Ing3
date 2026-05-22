#!/usr/bin/env bash
set -euo pipefail

SPARK_SUBMIT="/opt/spark/bin/spark-submit"
MASTER="local[*]"
DRIVER_HOST="172.31.252.110"
PG_JAR="/opt/spark/jars/postgresql-42.7.4.jar"
APP="/home/hadoop/jobs-seulgi/demo_bi/04_generate_events.py"

exec "$SPARK_SUBMIT" \
  --master "$MASTER" \
  --jars "$PG_JAR" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host="$DRIVER_HOST" \
  --conf spark.port.maxRetries=64 \
  "$APP"