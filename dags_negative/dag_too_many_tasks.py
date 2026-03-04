# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from datetime import datetime

with DAG(
    "too_many_tasks_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    default_args={"owner": "test"},
    description="DAG exceeding maxTaskCount threshold — triggers task_count warning",
) as dag:
    prev = EmptyOperator(task_id="step_0")
    for i in range(1, 10):
        t = EmptyOperator(task_id=f"step_{i}")
        prev >> t
        prev = t
