#!/bin/bash

export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop/hadoop-3.3.6
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin

INPUT="hdfs://vm-datalake:9000/datalake/nawres/raw/nyc_taxi/yellow/year=2025/month=11"
OUTPUT="hdfs://vm-datalake:9000/datalake/nawres/curated/product_ec_base/year=2025/month=11"

spark-submit \
  --master spark://vm-spark-master:7077 \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.eventLog.enabled=false \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:/home/hadoop/jobs/log4j2-silent.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=file:/home/hadoop/jobs/log4j2-silent.properties" \
  /home/hadoop/jobs/job1_detournement.py \
  --input "$INPUT" \
  --output "$OUTPUT" \
  --n-product-ref 42019 \
  --n-warehouse 200 \
  --presence-pct 30