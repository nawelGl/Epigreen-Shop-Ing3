#!/usr/bin/env python3


# simulation de l'enrichissement incrémentiel
# Génération événements mockés J-1 (~4000-5000) => PostgreSQL staging.stg_events_delta

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import sys
import time

# ===== PostgreSQL =====
PG_URL = "jdbc:postgresql://172.31.249.114:5432/projet_bi"
PG_USER = "postgres"
PG_PASSWORD = "SIRIUS"

TARGET_DAILY_VIEWS = 4500


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
    spark = SparkSession.builder.appName("06_generate_delta_events").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Date J-1
    if len(sys.argv) > 1:
        date_j1 = sys.argv[1]
    else:
        date_j1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Génération delta pour: {date_j1}")

    
    # Charger dimensions
    df_clients = read_pg(spark, "warehouse.dim_client").select("id_client", "persona")
    df_produits = read_pg(spark, "warehouse.dim_produit").select("id_produit", "score_ec", "prix_unitaire")

    
    # Règles de conversion
    conversion_rules = {
        ("Eco-responsable", "A"): (0.40, 0.70),
        ("Eco-responsable", "B"): (0.28, 0.50),
        ("Eco-responsable", "C"): (0.10, 0.20),
        ("Eco-responsable", "D"): (0.08, 0.05),
        ("Eco-responsable", "E"): (0.06, 0.05),
        ("Sensible au prix", "A"): (0.12, 0.35),
        ("Sensible au prix", "B"): (0.12, 0.35),
        ("Sensible au prix", "C"): (0.12, 0.35),
        ("Sensible au prix", "D"): (0.12, 0.35),
        ("Sensible au prix", "E"): (0.12, 0.35),
        ("Standard", "A"): (0.12, 0.35),
        ("Standard", "B"): (0.11, 0.33),
        ("Standard", "C"): (0.10, 0.30),
        ("Standard", "D"): (0.08, 0.28),
        ("Standard", "E"): (0.07, 0.25),
    }

    conv_rows = [(p, s, v2c, c2a) for (p, s), (v2c, c2a) in conversion_rules.items()]
    df_conv = spark.createDataFrame(conv_rows, ["persona", "score_ec", "p_cart", "p_purchase"])

    q1_price = df_produits.approxQuantile("prix_unitaire", [0.25], 0.01)[0]
    

    # Échantillon du jour 
    
    nb_clients = df_clients.count()
    nb_products = df_produits.count()
    
    # calcul nb product par client
    products_per_client = int(TARGET_DAILY_VIEWS / nb_clients) + 1

    # sampling avnat Cross Join pour éconimiser la ressource 
    calc_ratio = (products_per_client * 1.2) / nb_products
    ratio_produits = calc_ratio if calc_ratio < 1.0 else 1.0
    df_produits_sample = df_produits.sample(False, ratio_produits)
    
    # Crossjoin
    df_views = df_clients.crossJoin(df_produits_sample).limit(TARGET_DAILY_VIEWS)

    df_views = (
        df_views
        .withColumn("event_date", lit(date_j1).cast("date"))
        .withColumn("rand1", rand())
        .withColumn("rand2", rand())
    )

    
    # Joindre probas + boost
    
    df_views = (
        df_views.join(
            df_conv,
            on=[df_views.persona == df_conv.persona,
                df_views.score_ec == df_conv.score_ec],
            how="left"
        )
        .drop(df_conv.persona)
        .drop(df_conv.score_ec)
    )

    df_views = (
        df_views
        .withColumn("p_cart",
            when((col("persona") == "Sensible au prix") & (col("prix_unitaire") <= q1_price),
                 least(col("p_cart") * 2.0, lit(1.0)))
            .otherwise(col("p_cart")))
        .withColumn("p_purchase",
            when((col("persona") == "Sensible au prix") & (col("prix_unitaire") <= q1_price),
                 least(col("p_purchase") * 1.5, lit(1.0)))
            .otherwise(col("p_purchase")))
    )

    
    # 5. Funnel
    
    cols_final = ["event_date", "id_client", "id_produit", "event_type",
                  "prix_unitaire", "persona", "score_ec"]

    df_v = df_views.withColumn("event_type", lit("view")).select(cols_final)
    df_cart = (
        df_views.filter(col("rand1") < col("p_cart"))
        .withColumn("event_type", lit("cart")).select(cols_final)
    )
    df_purchase = (
        df_views.filter((col("rand1") < col("p_cart")) & (col("rand2") < col("p_purchase")))
        .withColumn("event_type", lit("purchase")).select(cols_final)
    )

    df_events = df_v.unionAll(df_cart).unionAll(df_purchase)

    df_events = (
        df_events
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("pipeline", lit(f"events_mock_delta_{date_j1}"))
    )

    
    # Écriture PostgreSQL staging (table delta temporaire)
    write_pg(df_events, "staging.stg_events_delta")

    df_events.groupBy("event_type").count().show()
    print(f"Total événements delta: {df_events.count()}")

    spark.stop()
    elapsed = time.time() - start
    print(f"06_generate_delta_events terminé : {elapsed:.1f} sec")


if __name__ == "__main__":
    main()