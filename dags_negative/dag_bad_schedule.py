# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from non_existent_sensor import MagicSensor

from datetime import datetime

with DAG(
    "bad_schedule_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    default_args={"owner": "test"},
    description="DAG with broken import — triggers import error",
) as dag:
    EmptyOperator(task_id="start") >> MagicSensor(task_id="wait")
