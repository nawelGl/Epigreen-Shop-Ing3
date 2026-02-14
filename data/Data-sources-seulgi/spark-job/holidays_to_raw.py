#!/usr/bin/env python3
import json
import urllib.request



with urllib.request.urlopen(API_URL) as response:
    data = json.loads(response.read().decode())

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, current_timestamp

# ====== données pour API ======
API_URL = "https://calendrier.api.gouv.fr/jours-feries/metropole.json"
RAW_BASE_PATH = "/datalake/raw/jours_feries"
# ====================

def main():

    spark = (
        SparkSession.builder
        .appName("api_jours_feries_to_raw_batch")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # appel API
    
    with urllib.request.urlopen(API_URL) as response:
        data = json.loads(response.read().decode())

    # conversion JSON => liste
    rows = []
    for date_str, name in data.items():
        rows.append({
            "date_str": date_str,
            "holiday_name": name,
            "zone": ZONE,
            "source": "calendrier.api.gouv.fr",
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "raw_json": json.dumps({date_str: name}, ensure_ascii=False)
        })

    df = spark.createDataFrame(rows)

    df2 = (
        df.withColumn("date", to_date(col("date_str")))
          .withColumn("year", year(col("date")))
          .withColumn("ingestion_ts", current_timestamp())
    )

    # enregistrement vers HDFS  (parittion zone/year)
    (df2.write
        .mode("append")
        .partitionBy("zone", "year")
        .parquet(RAW_BASE_PATH)
    )

    print(f"Inserted {df2.count()} rows into {RAW_BASE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
