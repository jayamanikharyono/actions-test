# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

REGIONS = ["us_east", "us_west", "eu_central", "ap_southeast"]

default_args = {
    "owner": "data-engineering-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}


def _process_region(region, **context):
    print(f"Processing data for region: {region}, date: {context['ds']}")


with DAG(
    "dynamic_regional_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */4 * * *",
    catchup=False,
    max_active_runs=2,
    tags=["dynamic", "regional"],
    description="Dynamically generated tasks from a config list",
) as dag:

    start = EmptyOperator(task_id="start")
    aggregate = EmptyOperator(task_id="aggregate_all_regions")
    done = EmptyOperator(task_id="done")

    for region in REGIONS:
        t = PythonOperator(
            task_id=f"process_{region}",
            python_callable=_process_region,
            op_kwargs={"region": region},
        )
        start >> t >> aggregate

    aggregate >> done
