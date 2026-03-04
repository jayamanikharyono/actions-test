# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta

default_args = {
    "owner": "sre",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _might_fail(**context):
    print("Task that might fail in production")


with DAG(
    "trigger_rules_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["trigger-rules", "error-handling"],
    description="DAG demonstrating various trigger rules for error handling",
) as dag:

    start = EmptyOperator(task_id="start")

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=_might_fail,
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=_might_fail,
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=_might_fail,
    )

    all_success_gate = EmptyOperator(
        task_id="all_success_gate",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    any_failed_alert = EmptyOperator(
        task_id="any_failed_alert",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    always_cleanup = EmptyOperator(
        task_id="always_cleanup",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    done = EmptyOperator(
        task_id="done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> [task_a, task_b, task_c]
    [task_a, task_b, task_c] >> all_success_gate >> done
    [task_a, task_b, task_c] >> any_failed_alert >> done
    [task_a, task_b, task_c] >> always_cleanup >> done
