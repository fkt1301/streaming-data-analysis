"""
Hourly watchd for the streaming pipeline.

Periodically check that the stream is still healthy
If not, the producer or Spark consumer has likely died.
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "data-eng",
    "retries": 0,
}

log = logging.getLogger(__name__)

def check_recent_rows(**context):
    hook = PostgresHook(postgres_conn_id="app_postgres")
    row = hook.get_first(
        "SELECT COUNT(*) FROM created_users WHERE created_at > NOW() - INTERVAL '1 hour';"
    )
    recent_count = row[0]
    if recent_count == 0:
        raise ValueError(
            "No rows landed in created_users in the last hour -- "
            "producer or Spark consumer may be down."
        )
    log.info("Pipeline healthy: %s rows in the last hour.", recent_count)


with DAG(
    dag_id="pipeline_health_check",
    description="Verify the streaming pipeline is still producing fresh rows",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["monitoring"],
) as dag:

    PythonOperator(
        task_id="check_recent_rows",
        python_callable=check_recent_rows,
    )
