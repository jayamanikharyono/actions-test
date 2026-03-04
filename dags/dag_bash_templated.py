# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "devops",
    "retries": 0,
}

with DAG(
    "bash_templated_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["bash", "jinja"],
    description="BashOperator with Jinja templating and env vars",
) as dag:

    print_context = BashOperator(
        task_id="print_context",
        bash_command='echo "Run ID: {{ run_id }} | Date: {{ ds }} | DAG: {{ dag.dag_id }}"',
    )

    generate_report = BashOperator(
        task_id="generate_report",
        bash_command='echo "Report for {{ macros.ds_format(ds, \'%Y-%m-%d\', \'%B %d, %Y\') }}"',
    )

    conditional_step = BashOperator(
        task_id="conditional_step",
        bash_command='if [ "{{ dag_run.conf.get("mode", "full") }}" = "quick" ]; '
                     'then echo "Quick mode"; else echo "Full mode"; fi',
    )

    archive = BashOperator(
        task_id="archive_logs",
        bash_command='echo "Archiving logs older than {{ params.retention_days }} days"',
        params={"retention_days": 30},
    )

    done = EmptyOperator(task_id="done")

    print_context >> generate_report >> conditional_step >> archive >> done
