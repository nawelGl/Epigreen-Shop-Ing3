#!/usr/bin/env python3
import argparse
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    abs as sql_abs,
    hash as sql_hash,
    lit,
    rand,
    when,
    monotonically_increasing_id,
    greatest,
    least,
    round as sql_round,
)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="HDFS input parquet path (RAW)")
    parser.add_argument("--output", required=True, help="HDFS output parquet path (CURATED)")
    parser.add_argument("--n-product-ref", type=int, required=True, help="ex: 42019")
    parser.add_argument("--n-warehouse", type=int, required=True, help="ex: 200")
    parser.add_argument("--presence-pct", type=int, default=50, help="0-100, % presence produit/entrepot")
    parser.add_argument("--sample-n", type=int, default=0, help="Optionnel: limite le volume (demo). 0 = full")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("JOB1 - NYC Taxi -> product_ec (detournement)")
        .getOrCreate()
    )
   
    spark.sparkContext.setLogLevel("FATAL")
    # Réduit énormément le spam Spark (INFO -> WARN)
    spark.conf.set("spark.sql.shuffle.partitions", "16")

    # Logs propres côté job
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [JOB1] %(message)s",
    )
    log = logging.getLogger("JOB1")

    t0 = time.perf_counter()

    log.info("START")
    log.info("Input  : %s", args.input)
    log.info("Output : %s", args.output)
    log.info("Params : n_product_ref=%s n_warehouse=%s presence_pct=%s sample_n=%s",
             args.n_product_ref, args.n_warehouse, args.presence_pct, args.sample_n)

    # 1) Lecture RAW
    log.info("Step 1/7 - Read RAW parquet")
    df = spark.read.parquet(args.input)

    # Option demo : limite le volume
    if args.sample_n and args.sample_n > 0:
        log.info("Step 2/7 - Limit demo to %s rows (approx)", args.sample_n)
        df = df.limit(int(args.sample_n))
    else:
        log.info("Step 2/7 - No limit (full dataset)")

    # 2) Colonnes utiles uniquement
    log.info("Step 3/7 - Select useful columns")
    df = df.select("PULocationID", "DOLocationID", "fare_amount", "trip_distance")

    # 3) Nettoyage de base
    log.info("Step 4/7 - Basic cleaning (nulls + non-negative)")
    df = df.filter(
        (col("fare_amount").isNotNull()) &
        (col("trip_distance").isNotNull()) &
        (col("fare_amount") >= 0) &
        (col("trip_distance") >= 0)
    )

    # 4) Remapping IDs
    log.info("Step 5/7 - Remap to ids (warehouse/product_ref)")
    df = (
        df.withColumn(
            "id_warehouse",
            (sql_abs(col("PULocationID").cast("long")) % lit(args.n_warehouse)) + lit(1)
        )
        .withColumn(
            "id_product_ref",
            (sql_abs(col("DOLocationID").cast("long")) % lit(args.n_product_ref)) + lit(1)
        )
    )

    # 5) Simulation présence produit / entrepôt (%)
    log.info("Step 6/7 - Presence filter (%s%%) (deterministic hash)", args.presence_pct)
    df = (
        df.withColumn(
            "presence_flag",
            (sql_abs(sql_hash(col("id_product_ref"), col("id_warehouse"))) % lit(100))
        )
        .filter(col("presence_flag") < lit(args.presence_pct))
    )

    # 6) EC PROCESS (référentiel : 2 → 50 kg CO2eq) + arrondi 1 décimale
    ec_process_raw = (
        when(col("fare_amount") < 10, col("fare_amount") * lit(1.2))
        .when(col("fare_amount") < 30, col("fare_amount") * lit(1.6))
        .otherwise(col("fare_amount") * lit(2.0))
    )

    df = df.withColumn(
        "ec_process",
        sql_round(
            least(lit(50.0), greatest(lit(2.0), ec_process_raw.cast("double"))),
            1
        )
    )

    # 7) EC TRANSPORT (5% → 40% du process) + arrondi 1 décimale
    df = df.withColumn(
        "ec_transport",
        sql_round(
            (col("ec_process") * (rand() * lit(0.35) + lit(0.05))).cast("double"),
            1
        )
    )

    # 8) ID technique (commence à 1)
    df = df.withColumn("id_product_instance", monotonically_increasing_id() + lit(1))

    # Projection finale (Job1 ne calcule PAS ec_total)
    df_final = df.select(
        "id_product_instance",
        "id_product_ref",
        "id_warehouse",
        "ec_process",
        "ec_transport"
    )

    t1 = time.perf_counter()
    log.info("Writing CURATED parquet (overwrite)")
    df_final.write.mode("overwrite").parquet(args.output)
    t2 = time.perf_counter()

    log.info("DONE")
    log.info("elapsed_write_seconds=%.2f", (t2 - t1))
    log.info("elapsed_total_seconds=%.2f", (t2 - t0))
    # Metrics
    log.info("DONE")
    log.info("elapsed_transform_seconds=%.2f", (t1 - t0))
    log.info("elapsed_write_seconds=%.2f", (t2 - t1))
    log.info("elapsed_total_seconds=%.2f", (t2 - t0))

    spark.stop()


if __name__ == "__main__":
    main()
