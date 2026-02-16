#!/usr/bin/env python3
import os
import sys
from pyspark.sql import SparkSession

def main():
    nn_uri = os.environ.get("NN_URI", "hdfs://vm-datalake:9000")
    raw_path = os.environ.get("RAW_PATH",  f"{nn_uri}/datalake/nawres/raw/nyc_taxi/yellow/year=2025/month=11")
    cur_path = os.environ.get("CUR_PATH",  f"{nn_uri}/datalake/nawres/curated/product_ec_base/year=2025/month=11")

    spark = (
        SparkSession.builder
        .appName("DEMO-before-after")
        .getOrCreate()
    )

    # Réduire les logs
    spark.sparkContext.setLogLevel("ERROR")

    print("===== DEMO AVANT / APRES =====")
    print(f"RAW     = {raw_path}")
    print(f"CURATED = {cur_path}\n")

    raw = spark.read.parquet(raw_path)
    print("---- RAW schema ----")
    raw.printSchema()
    print("---- RAW sample (5) ----")
    raw.show(5, truncate=False)
    print(f"RAW count = {raw.count()}\n")

    curated = spark.read.parquet(cur_path)
    print("---- CURATED schema ----")
    curated.printSchema()
    print("---- CURATED sample (5) ----")
    curated.show(5, truncate=False)
    print(f"CURATED count = {curated.count()}\n")

    print("===== FIN DEMO =====")
    spark.stop()

if __name__ == "__main__":
    sys.exit(main())
