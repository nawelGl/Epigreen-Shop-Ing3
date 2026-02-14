#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit

# adresse de kafka
BOOTSTRAP = "172.31.249.79:9092"
# topic des évènements des utilisateurs
TOPICS = "user-event-click,user-event-cart,user-event-search"

RAW_BASE_PATH = "/datalake/raw/user_events"

def main():

    spark = (
        SparkSession.builder
        .appName("kafka_to_raw_batch")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")   # tout depuis le début => à changer pour airflow
        .option("endingOffsets", "latest")       # jusqu'à maintenant
        .load()
    )

    raw_df = (
        df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_value"),
        )
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("dt", to_date(col("kafka_timestamp")))
        .withColumn("pipeline", lit("user_events_pipeline"))
    )

    # enregistrement par parquet
    raw_df.write \
        .mode("append") \
        .partitionBy("topic", "dt") \
        .parquet(RAW_BASE_PATH)

    spark.stop()

if __name__ == "__main__":
    main()
