from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date

def main():
    spark = (
        SparkSession.builder
        .appName("sales_raw_to_curated")
        .getOrCreate()
    )

    # 1) lecture de raw de HDFS
    raw_path = "/datalake/raw/sales/daily_sales.csv"

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )

    # 2) changement de type de données 
    df_clean = (
        df_raw
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .withColumn("amount", col("amount").cast("double"))
    )

    # 3) calcul d'agrégation par pays et date
    df_curated = (
        df_clean
        .groupBy("date", "country")
        .agg(_sum("amount").alias("total_amount"))
        .orderBy("date", "country")
    )

    # 4) enregistrement sur curated de datalake en parquet
    output_path = "/datalake/curated/sales_daily_agg"

    (
        df_curated
        .coalesce(1)  # on met pour l'insant 1 car c'est un petit proto mais à changer si on veux des exexcutors distribués 
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()

if __name__ == "__main__":
    main()