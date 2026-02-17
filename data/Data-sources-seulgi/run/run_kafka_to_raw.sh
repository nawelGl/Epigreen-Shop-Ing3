#!/usr/bin/env bash
set -euo pipefail

SPARK_SUBMIT="/opt/spark/bin/spark-submit"
MASTER="local[*]"
HDFS="hdfs://vm-datalake:9000"
DRIVER_HOST="172.31.253.136"
APP="/home/hadoop/jobs-seulgi/kafka_to_raw.py"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

exec "$SPARK_SUBMIT" \
  --master "$MASTER" \
  --conf spark.hadoop.fs.defaultFS="$HDFS" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host="$DRIVER_HOST" \
  --conf spark.port.maxRetries=64 \
  --packages "$KAFKA_PKG" \
  "$APP"