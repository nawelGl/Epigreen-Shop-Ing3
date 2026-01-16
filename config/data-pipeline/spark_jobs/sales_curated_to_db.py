from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("sales_curated_to_postgres") \
    .getOrCreate()

# 1) lecture de données en curated
curated_path = "/datalake/curated/sales_daily_agg"  
df_curated = spark.read.parquet(curated_path)

# 2) connexion postrges
pg_url = "jdbc:postgresql://172.31.250.155:5432/dwh"

connection_properties = {
    "user": "postgres",        
    "password": "postgres",    
    "driver": "org.postgresql.Driver",
}

table_name = "sales_curated"

# 3) enreigstrement de curated à DB
(
    df_curated.write
    	.mode("append")         # si premier fois "overwrite" ==> à changer en "appned" pour la suite ok
	.jdbc(pg_url, table_name, properties=connection_properties)
)

spark.stop()