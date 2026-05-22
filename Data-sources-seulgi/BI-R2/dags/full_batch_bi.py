from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

JOBS_PATH = "/home/hadoop/jobs-seulgi/demo_bi/run"

default_args = {
    "owner": "seulgi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="full_batch_bi",
    default_args=default_args,
    start_date=datetime(2025, 4, 1),
    schedule=None,
    catchup=False,
    tags=["seulgi", "ec", "full_load"],
) as dag:

    customers_to_staging = SSHOperator(
        task_id="spark_customers_to_staging",
        ssh_conn_id="ssh_spark_master",
        command=f"bash -lc '{JOBS_PATH}/run_01_customers_to_staging.sh'",
        cmd_timeout=60 * 60,
    )

    products_to_staging = SSHOperator(
        task_id="spark_products_to_staging",
        ssh_conn_id="ssh_spark_master",
        command=f"bash -lc '{JOBS_PATH}/run_02_products_to_staging.sh'",
        cmd_timeout=60 * 60,
    )

    build_dimensions = SSHOperator(
        task_id="spark_build_dimensions",
        ssh_conn_id="ssh_spark_master",
        command=f"bash -lc '{JOBS_PATH}/run_03_build_dimensions.sh'",
        cmd_timeout=60 * 60,
    )

    generate_events = SSHOperator(
        task_id="spark_generate_events",
        ssh_conn_id="ssh_spark_master",
        command=f"bash -lc '{JOBS_PATH}/run_04_generate_events.sh'",
        cmd_timeout=60 * 60,
    )

    build_fact = SSHOperator(
        task_id="spark_build_fact",
        ssh_conn_id="ssh_spark_master",
        command=f"bash -lc '{JOBS_PATH}/run_05_build_fact.sh'",
        cmd_timeout=60 * 60,
    )

    customers_to_staging >> products_to_staging >> build_dimensions >> generate_events >> build_fact