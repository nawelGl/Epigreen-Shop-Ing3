from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "seulgi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="events_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["seulgi", "spark"],
) as dag:

    kafka_to_raw = SSHOperator(
        task_id="spark_kafka_to_raw",
        ssh_conn_id="ssh_spark_master",  
        command="bash -lc '/home/hadoop/jobs-seulgi/run/run_kafka_to_raw.sh'",
        cmd_timeout=60 * 60,
    )

    products_to_raw = SSHOperator(
        task_id="spark_products_to_raw",
        ssh_conn_id="ssh_spark_master",
        command="bash -lc '/home/hadoop/jobs-seulgi/run/run_products_to_raw.sh'",
        cmd_timeout=60 * 60,
    )

    customers_to_raw = SSHOperator(
        task_id="spark_customers_to_raw",
        ssh_conn_id="ssh_spark_master",
        command="bash -lc '/home/hadoop/jobs-seulgi/run/run_customers_to_raw.sh'",
        cmd_timeout=60 * 60,
    )

    holidays_to_raw = SSHOperator(
        task_id="spark_holidays_to_raw",
        ssh_conn_id="ssh_spark_master",
        command="bash -lc '/home/hadoop/jobs-seulgi/run/run_holidays_to_raw.sh'",
        cmd_timeout=60 * 60,
    )

    raw_to_curated = SSHOperator(
        task_id="spark_raw_to_curated",
        ssh_conn_id="ssh_spark_master",
        command="bash -lc '/home/hadoop/jobs-seulgi/run/run_raw_to_curated.sh'",
        cmd_timeout=60 * 60,
    )

    # ordre de task
    kafka_to_raw >> products_to_raw >> customers_to_raw >> holidays_to_raw >> raw_to_curated

