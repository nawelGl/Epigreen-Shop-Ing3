spark-submit \
  --master spark://vm-spark-master:7077 \
  job1_detournement.py \
  --input hdfs:///datalake/nawres/raw/nyc_taxi/yellow/year=2025/month=11 \
  --output hdfs:///datalake/nawres/curated/product_ec_base/year=2025/month=11 \
  --n-product-ref 42019 \
  --n-warehouse 200 \
  --presence-pct 30