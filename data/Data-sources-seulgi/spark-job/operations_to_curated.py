from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType
import time
# =======================
# HDFS Paths
HDFS_IP = "172.31.249.134" 
HDFS_PORT = "9000"
BASE_PATH = f"hdfs://{HDFS_IP}:{HDFS_PORT}/datalake/seulgi/raw"

RAW_EVENTS_PATH = f"{BASE_PATH}/user_events"
RAW_HOLIDAYS_PATH = f"{BASE_PATH}/holidays"
RAW_PRODUCTS_PATH = f"{BASE_PATH}/products"
RAW_CUSTOMERS_PATH = f"{BASE_PATH}/customers"

CURATED_OUT_PATH= f"hdfs://{HDFS_IP}:{HDFS_PORT}/datalake/seulgi/curated"

def main():
    start=time.time()
    spark = (
        SparkSession.builder
        .appName("SIRIUS_Raw_to_Curated_Transformation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")


    # 1) lecture de la source kafka

    events_raw = spark.read.parquet(RAW_EVENTS_PATH)
    print(events_raw.show())
    

    payload_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True), 
        StructField("event_type", StringType(), True)
    ])

    events = (
        events_raw.select(
            col("kafka_timestamp").alias("ts"),
            from_json(col("raw_value"), payload_schema).alias("payload")
        )
        .select(
            col("ts"),
            col("payload.user_id").alias("user_id"),
            col("payload.product_id").alias("p_id"),
            col("payload.event_type").alias("event_type")
        )
        .filter(col("user_id").isNotNull() & col("p_id").isNotNull())
        .withColumn("date_day", to_date(col("ts")))
    )
    events.show()
   
    # 2) Référentiels 
    # Customer
    customer = spark.read.parquet(RAW_CUSTOMERS_PATH).select(
        col("id").alias("user_id"),
        col("birth_date"),
        col("gender")
    )

    # Product catalog
    product = spark.read.parquet(RAW_PRODUCTS_PATH).select(
        col("id_catalog_product").alias("p_id"),
        col("sub_category").alias("p_category"),
        col("color").alias("p_color"),
        col("price").alias("p_price"),
        col("brand").alias("p_brand"),
        col("gender_segment").alias("p_gender"),
        col("score_ec").alias("p_score_ec"),
        col("season").alias("p_season")
    )

    # 3) source jours feries
 
    holidays = (
        spark.read.parquet(RAW_HOLIDAYS_PATH)
        .select(
            to_date(col("date")).alias("date_day")
        )
        .dropDuplicates(["date_day"])
        .withColumn("is_holiday", lit(True))
    )


    # 4) Join + Aggregation 

    joined = (
        events
        .join(customer, on="user_id", how="left")
        .join(product, on="p_id", how="left")
        .join(holidays, on="date_day", how="left")
        .withColumn("is_holiday", when(col("is_holiday").isNull(), lit(False)).otherwise(col("is_holiday")))
    )

    # réduction des évènement par user,product
    curated = (
        joined.groupBy(
            col("date_day").alias("date"),
            col("user_id"),
            col("birth_date"),
            col("gender"),
            col("p_id"),
            col("p_category"),
            col("p_color"),
            col("p_price"),
            col("p_brand"),
            col("p_gender"),
            col("p_score_ec"),
            col("p_season"),
            col("is_holiday"),
            col("event_type")
        )
        .agg(count(lit(1)).alias("interaction_count"))
        .withColumn("ingestion_ts", current_timestamp())
    )
    curated.show()

    
    # 5) Enregistrement sur la zone curated
    
    (curated.write
        .mode("overwrite")
        .partitionBy("date") # partition par date
        .parquet(CURATED_OUT_PATH)
    )

    print(f" Saved curated aggregated data to {CURATED_OUT_PATH}")
    spark.stop()
    end=time.time()
    
    elapsed= end-start
    print(f"opération est terminée : {elapsed}sec")

if __name__ == "__main__":
    main()