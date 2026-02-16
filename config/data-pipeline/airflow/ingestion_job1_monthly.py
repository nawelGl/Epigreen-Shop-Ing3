from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {"owner": "epigreen", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id="nyc_ingestion_job1_monthly",
    default_args=default_args,
    schedule="0 2 15 * *", # Minute 0 , Heure 2, Jour 15, Tous les mois, Tous les jours de la semaine
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nyc", "spark", "datalake"],
) as dag:

    ingestion_task = SSHOperator(
        task_id="ingestion_latest_month",
        ssh_conn_id="ssh_datalake",
        command="bash -lc '/home/hadoop/datalake-ingestion/ingestion/run_ingest_latest_yellow.sh'",
        do_xcom_push=False,
    )

    job1_task = SSHOperator(
      task_id="job1_detournement",
      ssh_conn_id="ssh_spark_master",
      command="bash -lc '/home/hadoop/jobs/run_job1_detournement.sh'",
      conn_timeout=30,      
      cmd_timeout=600,      
    )

    ingestion_task >> job1_task