#!/usr/bin/env python3
"""
JOB2 - Agrégation empreinte carbone (product_ec) - VERSION FINALE
==================================================================
1. Lecture curated_base (ec_process, ec_transport)
2. Premier GroupBy (product_ref + warehouse) → score par couple
3. Écriture HDFS curated_final
4. Second GroupBy (product_ref uniquement) → score global par produit
5. Écriture PostgreSQL temp_product_scores
6. UPDATE ref_product_catalog avec les scores

Usage :
  spark-submit job2_ec_aggregation.py \
    --input  hdfs://vm-datalake:9000/datalake/nawres/curated/product_ec_base/year=2025 \
    --output hdfs://vm-datalake:9000/datalake/nawres/curated/product_ec_final/year=2025 \
    --shuffle-partitions 24 \
    --pg-host 172.31.252.28 \
    --pg-port 5432 \
    --pg-database product_db \
    --pg-user postgres \
    --pg-password secret
"""

import argparse
import time
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    sum as sql_sum,
    round as sql_round,
    when,
    lit,
    count,
)


# ---------------------------------------------------------------------------
# Configuration des logs - Mode DEV (DEBUG) ou PROD (INFO)
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)s [JOB2] %(message)s",
)
log = logging.getLogger("JOB2")


# ---------------------------------------------------------------------------
# Score environnemental par paliers (0-100)
# ---------------------------------------------------------------------------
def compute_score_column(ec_total_col):
    """Calcule un score 0-100 à partir de ec_total moyen."""
    return (
        when(ec_total_col <= 20,
             sql_round(90 + (20 - ec_total_col) / 20 * 10, 1))
        .when(ec_total_col <= 40,
             sql_round(70 + (40 - ec_total_col) / 20 * 20, 1))
        .when(ec_total_col <= 60,
             sql_round(50 + (60 - ec_total_col) / 20 * 20, 1))
        .when(ec_total_col <= 80,
             sql_round(25 + (80 - ec_total_col) / 20 * 25, 1))
        .otherwise(
             sql_round(
                 when(25 - (ec_total_col - 80) > 0, 25 - (ec_total_col - 80))
                 .otherwise(lit(0.0)),
                 1
             )
        )
    )


def score_to_label(score_col):
    """Convertit le score numérique en label A-E."""
    return (
        when(score_col >= 90, lit("A"))
        .when(score_col >= 70, lit("B"))
        .when(score_col >= 50, lit("C"))
        .when(score_col >= 25, lit("D"))
        .otherwise(lit("E"))
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="JOB2 - EC Aggregation (Version Finale)")
    parser.add_argument("--input",  required=True, help="HDFS curated_base path")
    parser.add_argument("--output", required=True, help="HDFS curated_final output")
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    parser.add_argument("--pg-host", required=True, help="PostgreSQL host")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-password", required=True)
    args = parser.parse_args()

    # -----------------------------------------------------------------------
    # 1) Initialisation Spark
    # -----------------------------------------------------------------------
    log.info("=" * 60)
    log.info("DÉMARRAGE - Phase finale (curated_final + PostgreSQL)")
    log.info("Input HDFS   : %s", args.input)
    log.info("Output HDFS  : %s", args.output)
    log.info("PostgreSQL   : %s:%d/%s", args.pg_host, args.pg_port, args.pg_database)
    log.info("Partitions   : %d", args.shuffle_partitions)
    log.info("Log level    : %s", LOG_LEVEL)
    log.info("=" * 60)

    spark = (
        SparkSession.builder
        .appName("JOB2 - EC Aggregation (curated_final)")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.task.maxFailures", "4")
        .config("spark.executor.heartbeatInterval", "10s")
        .config("spark.network.timeout", "60s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("FATAL")

    t0 = time.perf_counter()

    # -----------------------------------------------------------------------
    # 2) Étape 1/7 - Lecture HDFS curated_base
    # -----------------------------------------------------------------------
    log.info("Étape 1/7 - Lecture HDFS curated_base")
    log.debug("Path complet : %s", args.input)
    t_read_start = time.perf_counter()

    try:
        df_raw = spark.read.parquet(args.input)
        
        # Vérification schéma
        expected_cols = {"id_product_ref", "id_warehouse", "ec_process", "ec_transport"}
        actual_cols   = set(df_raw.columns)
        missing       = expected_cols - actual_cols
        if missing:
            log.error("ERREUR SCHÉMA - Colonnes manquantes : %s", missing)
            spark.stop()
            sys.exit(1)

        df = df_raw.select("id_product_ref", "id_warehouse", "ec_process", "ec_transport")
        log.debug("Schéma vérifié : 4 colonnes attendues présentes")
        
        t_read_end = time.perf_counter()
        log.info("Étape 1/7 - Lecture terminée | durée=%.1fs", t_read_end - t_read_start)

    except Exception as e:
        log.error("Étape 1/7 - ÉCHEC | erreur=%s", str(e))
        spark.stop()
        sys.exit(1)

    # -----------------------------------------------------------------------
    # 3) Étape 2/7 - Filtre qualité données
    # -----------------------------------------------------------------------
    log.info("Étape 2/7 - Filtre qualité données")

    df_clean = df.filter(
        (col("ec_process").isNotNull())   &
        (col("ec_transport").isNotNull()) &
        (col("ec_process")   >= 0)        &
        (col("ec_transport") >= 0)
    )

    rows_read     = df.count()
    rows_clean    = df_clean.count()
    rows_filtered = rows_read - rows_clean

    log.info("Étape 2/7 - Nettoyage | lignes=%s | filtrées=%d | propres=%s",
             f"{rows_read:,}", rows_filtered, f"{rows_clean:,}")

    if rows_filtered > 0:
        log.warning("QUALITÉ - %d lignes supprimées (valeurs nulles ou négatives)", rows_filtered)

    # -----------------------------------------------------------------------
    # 4) Étape 3/7 - Premier GroupBy (product_ref + warehouse)
    # -----------------------------------------------------------------------
    log.info("Étape 3/7 - Premier GroupBy (product_ref + warehouse)")
    log.debug("Calcul des moyennes ec_process, ec_transport, ec_total")
    log.debug("Partitions shuffle = %d (paramètre clé scale-out)", args.shuffle_partitions)
    t_agg1_start = time.perf_counter()

    df_with_total = df_clean.withColumn(
        "ec_total",
        sql_round(col("ec_process") + col("ec_transport"), 2)
    )

    df_agg1 = (
        df_with_total
        .groupBy("id_product_ref", "id_warehouse")
        .agg(
            sql_round(avg("ec_process"),   2).alias("avg_ec_process"),
            sql_round(avg("ec_transport"), 2).alias("avg_ec_transport"),
            sql_round(avg("ec_total"),     2).alias("avg_ec_total"),
            count("*").alias("nb_instances"),
        )
        .withColumn("score_ec", compute_score_column(col("avg_ec_total")))
        .withColumn("score_label", score_to_label(col("score_ec")))
    )

    t_agg1_end = time.perf_counter()
    log.info("Étape 3/7 - Agrégation 1 terminée | durée=%.1fs", t_agg1_end - t_agg1_start)

    # -----------------------------------------------------------------------
    # 5) Étape 4/7 - Écriture HDFS curated_final (résultats intermédiaires)
    # -----------------------------------------------------------------------
    log.info("Étape 4/7 - Écriture HDFS curated_final")
    log.debug("Path : %s", args.output)
    t_hdfs_start = time.perf_counter()

    try:
        df_agg1.write.mode("overwrite").parquet(args.output)
        t_hdfs_end = time.perf_counter()
        log.info("Étape 4/7 - Écriture HDFS terminée | durée=%.1fs", t_hdfs_end - t_hdfs_start)
    except Exception as e:
        log.error("Étape 4/7 - ÉCHEC écriture HDFS | erreur=%s", str(e))
        spark.stop()
        sys.exit(1)

    # -----------------------------------------------------------------------
    # 6) Étape 5/7 - Second GroupBy (product_ref uniquement) pour score global
    # -----------------------------------------------------------------------
    log.info("Étape 5/7 - Second GroupBy (product_ref uniquement)")
    log.debug("Calcul moyenne pondérée du score par produit")
    t_agg2_start = time.perf_counter()

    # Moyenne pondérée : sum(score * nb_instances) / sum(nb_instances)
    df_global = (
        df_agg1
        .groupBy("id_product_ref")
        .agg(
            sql_round(
                sql_sum(col("score_ec") * col("nb_instances")) / sql_sum(col("nb_instances")),
                1
            ).alias("score_ec_global"),
            sql_sum(col("nb_instances")).alias("total_instances")
        )
        .withColumn("score_label_global", score_to_label(col("score_ec_global")))
    )

    unique_products = df_global.count()
    t_agg2_end = time.perf_counter()
    log.info("Étape 5/7 - Agrégation 2 terminée | produits_uniques=%d | durée=%.1fs",
             unique_products, t_agg2_end - t_agg2_start)

    # -----------------------------------------------------------------------
    # 7) Étape 6/7 - Écriture PostgreSQL temp_product_scores
    # -----------------------------------------------------------------------
    log.info("Étape 6/7 - Écriture PostgreSQL temp_product_scores")
    t_pg_write_start = time.perf_counter()

    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_database}"
    jdbc_properties = {
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": "org.postgresql.Driver"
    }

    try:
        df_to_pg = df_global.select(
            col("id_product_ref"),
            col("score_ec_global"),
            col("score_label_global")
        )

        jdbc_properties = {
    "user": args.pg_user,
    "password": args.pg_password,
    "driver": "org.postgresql.Driver",
    "batchsize": "2000",
    "isolationLevel": "READ_COMMITTED"
}

        df_to_pg = df_global.select(
            col("id_product_ref"),
            col("score_ec_global"),
            col("score_label_global")
        ).repartition(10)   # <= équivalent “10 connexions” côté Spark

        df_to_pg.write \
            .mode("overwrite") \
            .jdbc(url=jdbc_url, table="temp_product_scores", properties=jdbc_properties)

        t_pg_write_end = time.perf_counter()
        log.info("Étape 6/7 - Écriture PostgreSQL terminée | lignes=%d | connexions=10 | durée=%.1fs",
                 unique_products, t_pg_write_end - t_pg_write_start)

    except Exception as e:
        log.error("Étape 6/7 - ÉCHEC écriture PostgreSQL | erreur=%s", str(e))
        log.warning("FALLBACK - Données sécurisées dans HDFS curated_final")
        spark.stop()
        sys.exit(1)

    # -----------------------------------------------------------------------
    # 8) Étape 7/7 - UPDATE ref_product_catalog
    # -----------------------------------------------------------------------
    log.info("Étape 7/7 - UPDATE ref_product_catalog")
    t_pg_update_start = time.perf_counter()

    try:
        import psycopg2

        conn = psycopg2.connect(
            host=args.pg_host,
            port=args.pg_port,
            database=args.pg_database,
            user=args.pg_user,
            password=args.pg_password
        )
        cursor = conn.cursor()

        update_query = """
            UPDATE ref_product_catalog r
            SET score_ec = t.score_ec_global::varchar,
                score_label = t.score_label_global
            FROM temp_product_scores t
            WHERE r.id_catalog_product = t.id_product_ref
        """

        cursor.execute(update_query)
        rows_updated = cursor.rowcount
        conn.commit()

        cursor.close()
        conn.close()

        t_pg_update_end = time.perf_counter()
        log.info("Étape 7/7 - UPDATE terminé | lignes_maj=%d | durée=%.1fs",
                 rows_updated, t_pg_update_end - t_pg_update_start)

    except Exception as e:
        log.error("Étape 7/7 - ÉCHEC UPDATE | erreur=%s", str(e))
        log.warning("ROLLBACK - Suppression table temporaire")
        
        try:
            cursor.execute("DROP TABLE IF EXISTS temp_product_scores")
            conn.commit()
            conn.close()
        except:
            pass

        spark.stop()
        sys.exit(1)

    # -----------------------------------------------------------------------
    # 9) Métriques finales
    # -----------------------------------------------------------------------
    t_total = time.perf_counter() - t0

    log.info("=" * 60)
    log.info("SUCCÈS")
    log.info("Durée totale       : %.1fs", t_total)
    log.info("Lignes lues        : %s", f"{rows_read:,}")
    log.info("Lignes filtrées    : %d", rows_filtered)
    log.info("Lignes HDFS        : %s", f"{rows_clean:,}")
    log.info("Produits uniques   : %d", unique_products)
    log.info("Produits MAJ (DB)  : %d", rows_updated)
    log.info("=" * 60)

    # Métriques parsables pour spark-monitor
    log.info("elapsed_read_seconds=%.2f", t_read_end - t_read_start)
    log.info("elapsed_first_agg_seconds=%.2f", t_agg1_end - t_agg1_start)
    log.info("elapsed_hdfs_write_seconds=%.2f", t_hdfs_end - t_hdfs_start)
    log.info("elapsed_second_agg_seconds=%.2f", t_agg2_end - t_agg2_start)
    log.info("elapsed_pg_write_seconds=%.2f", t_pg_write_end - t_pg_write_start)
    log.info("elapsed_pg_update_seconds=%.2f", t_pg_update_end - t_pg_update_start)
    log.info("elapsed_total_seconds=%.2f", t_total)
    log.info("rows_read=%d", rows_read)
    log.info("rows_filtered=%d", rows_filtered)
    log.info("rows_written_hdfs=%d", rows_clean)
    log.info("rows_written_pg=%d", unique_products)
    log.info("rows_updated_catalog=%d", rows_updated)
    log.info("shuffle_partitions=%d", args.shuffle_partitions)

    spark.stop()


if __name__ == "__main__":
    main()