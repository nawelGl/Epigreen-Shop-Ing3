#!/usr/bin/env python3


#PostgreSQL staging => Transform => Dimensions => PostgreSQL warehouse schema

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime
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
    spark = SparkSession.builder.appName("03_build_dimensions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    
    # 1. Dim_Client
    
    df_stg = read_pg(spark, "staging.stg_customers")

    df_dim_client = (
        df_stg
        .withColumn("id_client", col("id"))
        .withColumn("persona",
            when(substring(col("birth_date"), 1, 4).cast("int") >= 1991, "Eco-responsable")
            .when(rand() < (3.0 / 7.0), "Sensible au prix")
            .otherwise("Standard"))
        .withColumn("tranche_age",
            when(substring(col("birth_date"), 1, 4).cast("int") >= 2000, "18-24")
            .when(substring(col("birth_date"), 1, 4).cast("int") >= 1991, "25-34")
            .when(substring(col("birth_date"), 1, 4).cast("int") >= 1981, "35-44")
            .when(substring(col("birth_date"), 1, 4).cast("int") >= 1971, "45-54")
            .otherwise("55+"))
        .withColumn("date_creation", current_timestamp())
        .withColumn("date_maj", current_timestamp())
        .select("id_client", "gender", "tranche_age", "persona",
                "date_creation", "date_maj")
    )

    write_pg(df_dim_client, "warehouse.dim_client")
    print(f"dim_client: {df_dim_client.count()} lignes")

   
    # 2. Dim_Produit
   
    df_stg_p = read_pg(spark, "staging.stg_products")

    df_dim_produit = (
        df_stg_p
        .select(
            col("id_catalog_product").alias("id_produit"),
            col("name").alias("nom"),
            col("main_category").alias("categorie"),
            col("sub_category").alias("sous_categorie"),
            col("article_type"),
            col("score_label").alias("score_ec"),
            col("price").cast("double").alias("prix_unitaire"),
            col("brand").alias("marque")
        )
        .withColumn("date_creation", current_timestamp())
        .withColumn("date_maj", current_timestamp())
    )

    write_pg(df_dim_produit, "warehouse.dim_produit")
    print(f"dim_produit: {df_dim_produit.count()} lignes")

    
    # 3. Dim_Temps (365 jours)
   
    start_date = datetime.date(2025, 4, 1)
    dates = [(start_date + datetime.timedelta(days=i),) for i in range(365)]
    df_dim_temps = spark.createDataFrame(dates, ["date_complete"])

    jours_feries = [
        datetime.date(2025, 5, 1),  datetime.date(2025, 5, 8),
        datetime.date(2025, 5, 29), datetime.date(2025, 6, 9),
        datetime.date(2025, 7, 14), datetime.date(2025, 8, 15),
        datetime.date(2025, 11, 1), datetime.date(2025, 11, 11),
        datetime.date(2025, 12, 25), datetime.date(2026, 1, 1),
        datetime.date(2026, 3, 31),
    ]
    feries_df = spark.createDataFrame([(d,) for d in jours_feries], ["date_ferie"])

    df_dim_temps = (
        df_dim_temps
        .withColumn("id_date", date_format("date_complete", "yyyyMMdd").cast("int"))
        .withColumn("annee", year("date_complete"))
        .withColumn("trimestre", quarter("date_complete"))
        .withColumn("mois", month("date_complete"))
        .withColumn("jour", dayofmonth("date_complete"))
        .withColumn("jour_semaine", dayofweek("date_complete"))
    )

    df_dim_temps = (
        df_dim_temps
        .join(feries_df, df_dim_temps.date_complete == feries_df.date_ferie, "left")
        .withColumn("est_ferie",
            when(col("date_ferie").isNotNull(), "Oui").otherwise("Non"))
        .drop("date_ferie")
        .select("id_date", "date_complete", "annee", "trimestre",
                "mois", "jour", "jour_semaine", "est_ferie")
    )

    write_pg(df_dim_temps, "warehouse.dim_temps")
    print(f"dim_temps: {df_dim_temps.count()} lignes")

    spark.stop()
    elapsed = time.time() - start
    print(f"03_build_dimensions terminé : {elapsed:.1f} sec")


if __name__ == "__main__":
    main()
