"""
Batch aggregation DAG

Runs once a day, summarizes the previous day's streamed users by country
into a separate table. This is a scheduled batch job over data the
streaming pipeline has already landed
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

log = logging.getLogger(__name__)


def compute_daily_country_counts(**context):
    hook = PostgresHook(postgres_conn_id="app_postgres")
    run_date = context["ds"]  # logical date of the DAG run, YYYY-MM-DD

    hook.run(
        """
        CREATE TABLE IF NOT EXISTS daily_country_counts (
            run_date    DATE NOT NULL,
            country     TEXT NOT NULL,
            user_count  INTEGER NOT NULL,
            PRIMARY KEY (run_date, country)
        );

        DELETE FROM daily_country_counts WHERE run_date = %s;

        INSERT INTO daily_country_counts (run_date, country, user_count)
        SELECT %s::date, country, COUNT(*) AS user_count
        FROM created_users
        WHERE created_at::date = %s::date
        GROUP BY country;
        """,
        parameters=(run_date, run_date, run_date),
    )

    # Logging the latest result in Airflow
    row = hook.get_first(
    "SELECT COUNT(*) FROM daily_country_counts WHERE run_date = %s;",
    parameters=(run_date,),
    )
    country_count = row[0]
    log.info("Aggregated %s countries for %s.", country_count, run_date)


with DAG(
    dag_id="daily_country_aggregates",
    description="Summarize the previous day's streamed users by country",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 30),
    catchup=False,
    tags=["batch", "aggregation"],
) as dag:

    PythonOperator(
        task_id="compute_daily_country_counts",
        python_callable=compute_daily_country_counts,
    )
