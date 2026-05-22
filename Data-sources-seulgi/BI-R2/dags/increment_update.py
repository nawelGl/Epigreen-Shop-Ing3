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
    dag_id="increment_update_bi",
    default_args=default_args,
    start_date=datetime(2025, 4, 2),
    schedule="0 6 * * *",
    catchup=False,
    tags=["seulgi", "ec", "delta"],
) as dag:

    generate_delta = SSHOperator(
        task_id="spark_generate_delta_events",
        ssh_conn_id="ssh_spark_master",
        command=f"bash -lc '{JOBS_PATH}/run_06_generate_delta.sh {{{{ ds }}}}'",
        cmd_timeout=60 * 60,
    )

    append_fact = SSHOperator(
        task_id="spark_append_fact",
        ssh_conn_id="ssh_spark_master",
        command=f"bash -lc '{JOBS_PATH}/run_07_append_fact.sh {{{{ ds }}}}'",
        cmd_timeout=60 * 60,
    )

    generate_delta >> append_fact