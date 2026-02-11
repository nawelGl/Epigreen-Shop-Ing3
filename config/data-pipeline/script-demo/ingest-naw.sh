python ingestion/nyc_yellow_ingest_to_hdfs.py --latest \
  --hdfs-base /datalake/nawres/raw/nyc_taxi/yellow

hdfs dfs -ls -h /datalake/nawres/raw/nyc_taxi/yellow/year=2025/month=11
hdfs dfs -du -h /datalake/nawres/raw/nyc_taxi/yellow/year=2025/month=11