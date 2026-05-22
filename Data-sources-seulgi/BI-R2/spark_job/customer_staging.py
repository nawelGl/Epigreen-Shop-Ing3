#!/usr/bin/env python3


#DB source (customers) => PostgreSQL staging.stg_customers

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import time

# ===== DB SOURCE =====
SRC_URL = "jdbc:postgresql://172.31.249.114:5432/membership-db"
SRC_USER = "postgres"
SRC_PASSWORD = "SIRIUS"
SRC_TABLE = "public.customers"

# ===== DB DESTINATION =====
DST_URL = "jdbc:postgresql://172.31.249.114:5432/projet_bi"
DST_USER = "postgres"
DST_PASSWORD = "SIRIUS"


def read_pg(spark, url, table):
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", SRC_USER)
        .option("password", SRC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_pg(df, url, table, mode="overwrite"):
    (df.write.format("jdbc")
     .option("url", url)
     .option("dbtable", table)
     .option("user", DST_USER)
     .option("password", DST_PASSWORD)
     .option("driver", "org.postgresql.Driver")
     .mode(mode)
     .save())


def main():
    start = time.time()
    spark = SparkSession.builder.appName("01_customers_to_staging").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = read_pg(spark, SRC_URL, SRC_TABLE)

    df2 = (
        df.withColumn("ingestion_ts", current_timestamp())
          .withColumn("pipeline", lit("customers_staging_pipeline"))
    )

    write_pg(df2, DST_URL, "staging.stg_customers")

    print(f"stg_customers: {df2.count()} lignes")
    spark.stop()

    elapsed = time.time() - start
    print(f"01_customers_to_staging terminé : {elapsed:.1f} sec")


if __name__ == "__main__":
    main()