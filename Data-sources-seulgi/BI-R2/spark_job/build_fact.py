#!/usr/bin/env python3


# PostgreSQL staging.stg_events =>  Agrégation => datamart.f_conversions


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

# ===== PostgreSQL =====
PG_URL = "jdbc:postgresql://172.31.249.114:5432/projet_bi"
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


def write_pg(df, table, mode="overwrite"):
    (df.write.format("jdbc")
     .option("url", PG_URL)
     .option("dbtable", table)
     .option("user", PG_USER)
     .option("password", PG_PASSWORD)
     .option("driver", "org.postgresql.Driver")
     .mode(mode)
     .save())


def main():
    start = time.time()
    spark = SparkSession.builder.appName("05_build_fact").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    
    # Charger événements depuis staging
    df_events = read_pg(spark, "staging.stg_events")

    
    # Agrégation table de fait
    
    df_f_conversions = (
        df_events
        .withColumn("id_date", date_format("event_date", "yyyyMMdd").cast("int"))
        .groupBy("id_date", "id_client", "id_produit")
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("nb_vues"),
            sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("nb_ajouts_panier"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("nb_achats"),
            sum(when(col("event_type") == "purchase", col("prix_unitaire"))
                .otherwise(0)).alias("montant_total")
        )
        .withColumn("date_insertion", current_timestamp())
        .withColumn("source_batch", lit("FULL_LOAD"))
    )

    
    # Écriture PostgreSQL datamart
    
    write_pg(df_f_conversions, "warehouse.f_fact_table")

    
    #  Vérification
    cnt = df_f_conversions.count()
    vues = df_f_conversions.agg(sum("nb_vues")).collect()[0][0]
    paniers = df_f_conversions.agg(sum("nb_ajouts_panier")).collect()[0][0]
    achats = df_f_conversions.agg(sum("nb_achats")).collect()[0][0]
    ca = df_f_conversions.agg(sum("montant_total")).collect()[0][0]

    print(f"F_Conversions: {cnt} lignes")
    print(f"  Vues: {vues}")
    print(f"  Paniers: {paniers}")
    print(f"  Achats: {achats}")
    print(f"  CA total: {ca}")

    spark.stop()
    elapsed = time.time() - start
    print(f"05_build_fact terminé : {elapsed:.1f} sec")


if __name__ == "__main__":
    main()