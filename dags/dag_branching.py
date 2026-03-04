# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from datetime import datetime

default_args = {
    "owner": "ml-team",
}


def _pick_branch(**context):
    day = context["logical_date"].weekday()
    if day < 5:
        return "weekday_processing"
    return "weekend_cleanup"


with DAG(
    "branching_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["branching"],
    description="BranchPythonOperator with conditional paths and trigger rule join",
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="check_day_of_week",
        python_callable=_pick_branch,
    )

    weekday = EmptyOperator(task_id="weekday_processing")
    weekend = EmptyOperator(task_id="weekend_cleanup")

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    notify = EmptyOperator(task_id="notify")

    start >> branch >> [weekday, weekend] >> join >> notify
