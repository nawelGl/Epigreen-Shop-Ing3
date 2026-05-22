#!/usr/bin/env python3


# staging.stg_events_delta =>  Agrégation => Append datamart.f_conversions

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import psycopg2
import sys
import time

# ===== PostgreSQL =====
PG_URL = "jdbc:postgresql://172.31.249.114:5432/projet_bi"
PG_HOST = "172.31.249.114"
PG_PORT = 5432
PG_DB = "projet_bi"
PG_USER = "postgres"
PG_PASSWORD = "SIRIUS"


def read_pg(spark, table):
    return (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def main():
    start = time.time()
    spark = SparkSession.builder.appName("07_append_fact").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Date J-1
    if len(sys.argv) > 1:
        date_j1 = sys.argv[1]
    else:
        date_j1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    id_date_j1 = int(date_j1.replace("-", ""))
    print(f"Append fact pour: {date_j1} (id_date: {id_date_j1})")

    
    # Idempotence: supprimer données existantes
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("DELETE FROM warehouse.f_fact_table WHERE id_date = %s", (id_date_j1,))
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Lignes supprimées (idempotence): {deleted}")

    
    # Charger delta events depuis staging
    df_events = read_pg(spark, "staging.stg_events_delta")

    
    #  Agrégation
    df_delta_fact = (
        df_events
        .withColumn("id_date", lit(id_date_j1))
        .groupBy("id_date", "id_client", "id_produit")
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("nb_vues"),
            sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("nb_ajouts_panier"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("nb_achats"),
            sum(when(col("event_type") == "purchase", col("prix_unitaire"))
                .otherwise(0)).alias("montant_total")
        )
        .withColumn("date_insertion", current_timestamp())
        .withColumn("source_batch", lit(f"DELTA_{date_j1}"))
    )

    
    #  Append PostgreSQL datamart
    (df_delta_fact.write.format("jdbc")
     .option("url", PG_URL)
     .option("dbtable", "datamart.f_conversions")
     .option("user", PG_USER)
     .option("password", PG_PASSWORD)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .save())

    
    # Vérification
    
    cnt = df_delta_fact.count()
    vues = df_delta_fact.agg(sum("nb_vues")).collect()[0][0]
    achats = df_delta_fact.agg(sum("nb_achats")).collect()[0][0]

    print(f"Delta ajouté: {cnt} lignes, {vues} vues, {achats} achats")

    spark.stop()
    elapsed = time.time() - start
    print(f"07_append_fact terminé : {elapsed:.1f} sec")


if __name__ == "__main__":
    main()