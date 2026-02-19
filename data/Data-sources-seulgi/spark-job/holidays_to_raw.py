#!/usr/bin/env python3
import json
import urllib.request
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, current_timestamp, lit

import time

# ====== API ======
ZONE = "metropole"  
API_URL = f"https://calendrier.api.gouv.fr/jours-feries/{ZONE}.json"

# ====== HDFS RAW ======
# HDFS Paths
HDFS_IP = "172.31.249.134" 
HDFS_PORT = "9000"
BASE_PATH = f"hdfs://{HDFS_IP}:{HDFS_PORT}/datalake/seulgi/raw"
RAW_BASE_PATH = f"{BASE_PATH}/holidays"

def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode("utf-8"))


def main():
    start=time.time()
    spark = (
        SparkSession.builder
        .appName("api_jours_feries_to_raw_batch")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Call API
    data = fetch_json(API_URL) 

    # 2) JSON -> rows
    fetched_at = datetime.utcnow().isoformat() + "Z"
    rows = []
    for date_str, name in data.items():
        rows.append({
            "zone": ZONE,
            "date_str": date_str,
            "holiday_name": name,
            "source": "calendrier.api.gouv.fr",
            "fetched_at": fetched_at,
            "raw_json": json.dumps({date_str: name}, ensure_ascii=False)
        })

    df = spark.createDataFrame(rows)

    # 3) Cast / add metadata
    df2 = (
        df.withColumn("date", to_date(col("date_str")))
          .withColumn("year", year(col("date")))
          .withColumn("ingestion_ts", current_timestamp())
    )

   
    print("Rows:", df2.count())
    df2.orderBy(col("date").asc()).show(10, truncate=False)

    # 5) Write to HDFS (partitioned)
    (df2.write
        .mode("overwrite")
        .partitionBy("zone", "year")
        .parquet(RAW_BASE_PATH)
    )

    print(f"Inserted rows into {RAW_BASE_PATH} (zone={ZONE})")
    spark.stop()

    end=time.time()
    elapsed=end-start
    print(f"operation est terminee : {elapsed} sec")


if __name__ == "__main__":
    main()