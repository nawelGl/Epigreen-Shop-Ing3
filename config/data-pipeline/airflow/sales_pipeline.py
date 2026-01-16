from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime




default_args = {
    "owner": "seulgi",
    "start_date": datetime(2025, 1, 1),
}


with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1) copie le fichier csv au serveur de datalake (scp)
    copy_csv_to_datalake = BashOperator(
        task_id="copy_csv_to_datalake",
        bash_command="""
scp /opt/data_sources/sales/daily_sales.csv vm-datalake@172.31.249.134:/home/vm-datalake/daily_sales.csv
""",
    )

    # 2) chargement de données en HDFS RAW (ssh + hdfs dfs -put)
    ingest_to_hdfs = BashOperator(
        task_id="copy_csv_to_hdfs_raw",
        bash_command="""
ssh vm-datalake@172.31.249.134 "/home/vm-datalake/apps/hadoop/bin/hdfs dfs -mkdir -p /datalake/raw/sales && /home/vm-datalake/apps/hadoop/bin/hdfs dfs -put -f /home/vm-datalake/daily_sales.csv /datalake/raw/sales/daily_sales.csv"
""",
    )
	# 3) script spark raw => curated
    raw_to_curated = BashOperator(
        task_id="spark_raw_to_curated",
        bash_command="""
	ssh vm-spark@172.31.253.167 '\
  /home/vm-spark/apps/spark/bin/spark-submit \
    --master local[*] \
	--conf spark.hadoop.fs.defaultFS=hdfs://172.31.249.134:9000 \
    /home/vm-spark/spark_jobs/sales_raw_to_curated.py \
'
""",
    )

# 4) script spark curated => Postgres
    curated_to_db = BashOperator(
        task_id="spark_curated_to_db",
        bash_command="""
	ssh vm-spark@172.31.253.167 '\
  /home/vm-spark/apps/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://172.31.249.134:9000 \
	/home/vm-spark/spark_jobs/sales_curated_to_db.py \
'
""",
    )

# 5) création / mise à jour de la vue sales_daily dans Postgres
    create_sales_daily_view = BashOperator(
        task_id="create_sales_daily_view",
        bash_command=f"""
ssh vm-postgres@172.31.250.155 '\
  psql -h localhost -U postgres -d dwh -c "CREATE OR REPLACE VIEW sales_daily AS \
    SELECT date, SUM(total_amount) AS total_amount \
    FROM sales_curated \
    GROUP BY date \
    ORDER BY date;" \
'
""",
    )
    copy_csv_to_datalake >> ingest_to_hdfs >> raw_to_curated >> curated_to_db >> create_sales_daily_view