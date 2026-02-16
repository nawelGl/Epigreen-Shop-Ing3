from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "epigreen",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="nyc_bootstrap_6months_job1",
    default_args=default_args,
    description="Bootstrap 6 months RAW then run Job1 detournement for all RAW partitions",
    schedule=None,          # déclenché à la main
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nyc", "bootstrap", "spark", "datalake"],
) as dag:

    # 1) Ingestion bootstrap 6 mois (sur vm-datalake)
    ingestion_bootstrap = SSHOperator(
        task_id="ingestion_bootstrap_6months",
        ssh_conn_id="ssh_datalake",
        command="bash -lc '/home/hadoop/datalake-ingestion/ingestion/run_ingest_boostrap_6_months.sh'",
        cmd_timeout=60*60,   
        get_pty=True,
    )

    # 2) Job1 detournement sur tous les mois RAW (sur vm-spark-master)
    job1_all = SSHOperator(
        task_id="job1_detournement_all_raw",
        ssh_conn_id="ssh_spark_master",
        command="bash -lc '/home/hadoop/jobs/run_job1_detournement_all_raw.sh'",
        cmd_timeout=60*60,  
        get_pty=True,
    )

