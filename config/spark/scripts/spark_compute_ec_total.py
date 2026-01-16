from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ----------------- PARAMÈTRES -----------------
MASTER_URL = "spark://172.31.253.136:7077"
JDBC_URL = "jdbc:postgresql://172.31.250.18:5432/carbon_db"

DB_USER = "postgres"
DB_PASSWORD = "toto"

INPUT_TABLE = "products"                 # table d'entrée
OUTPUT_TABLE = "products_with_ec_total"  # table de sortie
# ------------------------------------------------


def main():
    print("===== [DEMO] Calcul ec_total avec Spark + PostgreSQL =====")

    # 1) Créer la SparkSession connectée au  cluster
    spark = (
        SparkSession.builder
        .appName("ComputeECTotal")
        .master(MASTER_URL)
        .getOrCreate()
    )

    sc = spark.sparkContext
    print(f"[INFO] Application ID : {sc.applicationId}")
    print(f"[INFO] Master         : {sc.master}")

    connection_props = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # 2) Lire la table products depuis PostgreSQL
    print(f"[INFO] Lecture de la table '{INPUT_TABLE}' depuis PostgreSQL...")
    df = spark.read.jdbc(JDBC_URL, INPUT_TABLE, properties=connection_props)

    print("[INFO] Schéma lu :")
    df.printSchema()

    print("[INFO] Données d'entrée :")
    df.show()

    # 3) Calcul de ec_total = ec_process + ec_transport
    print("[INFO] Calcul de la colonne ec_total...")
    df_result = df.withColumn(
        "ec_total",
        col("ec_process") + col("ec_transport")
    )

    print("[INFO] Données avec ec_total :")
    df_result.show()

    # 4) Écrire le résultat dans une nouvelle table
    print(f"[INFO] Écriture du résultat dans la table '{OUTPUT_TABLE}'...")
    df_result.write.mode("overwrite").jdbc(
        JDBC_URL,
        OUTPUT_TABLE,
        properties=connection_props
    )

    print("[OK] Terminé. La table de sortie est :", OUTPUT_TABLE)

    spark.stop()
    print("===== [DEMO] Fin du job =====")


if __name__ == "__main__":
    main()
