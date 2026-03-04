# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.python import PythonSensor

from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _data_available():
    """Simulate checking for upstream data."""
    return True


def _run_pipeline(**context):
    print(f"Running pipeline for {context['ds']}")


with DAG(
    "sensor_driven_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval="30 8 * * 1-5",
    catchup=False,
    tags=["sensors", "pipeline"],
    description="DAG with sensor-based waiting and timeouts",
    dagrun_timeout=timedelta(hours=3),
) as dag:

    wait_window = TimeDeltaSensor(
        task_id="wait_for_data_window",
        delta=timedelta(minutes=5),
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    check_upstream = PythonSensor(
        task_id="check_upstream_data",
        python_callable=_data_available,
        poke_interval=120,
        timeout=1800,
        mode="reschedule",
        soft_fail=True,
    )

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=_run_pipeline,
    )

    report = EmptyOperator(task_id="send_report")
    cleanup = EmptyOperator(task_id="cleanup")

    wait_window >> check_upstream >> run >> [report, cleanup]
