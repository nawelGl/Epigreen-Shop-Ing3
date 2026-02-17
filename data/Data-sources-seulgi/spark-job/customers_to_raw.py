#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import time

# ===== DB INFO =====
JDBC_URL = "jdbc:postgresql://172.31.252.28:5432/membership-db"
DB_USER = "postgres"
DB_PASSWORD = "SIRIUS"
DB_TABLE = "public.customers"

# ===== HDFS =====
RAW_PATH = "hdfs://vm-datalake:9000/datalake/seulgi/raw/customers"


def main():
    start=time.time()
    spark = SparkSession.builder.appName("customers_db_to_raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # lecture dans la DB
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", DB_TABLE)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # ajout de ts de l'ingestion
    df2 = (
        df.withColumn("ingestion_ts", current_timestamp())
          .withColumn("pipeline", lit("products_db_pipeline"))
    )

    # enregistrement HDFS 
    (df2.write
        .mode("overwrite")   # overwrite car c'est une dimension
        .parquet(RAW_PATH)
    )

    print("Products loaded to RAW successfully")

    spark.stop()

    end=time.time()
    elapsed=end-start

    print(f"operatios est terminee : {elapsed} sec")
if __name__ == "__main__":
    main()