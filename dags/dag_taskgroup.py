# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

default_args = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "taskgroup_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["taskgroup", "pipeline"],
    description="DAG with nested TaskGroups",
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("ingestion") as ingestion:
        ingest_api = EmptyOperator(task_id="ingest_api")
        ingest_db = EmptyOperator(task_id="ingest_db")
        ingest_s3 = EmptyOperator(task_id="ingest_s3")

    with TaskGroup("processing") as processing:
        with TaskGroup("cleaning") as cleaning:
            deduplicate = EmptyOperator(task_id="deduplicate")
            fill_nulls = EmptyOperator(task_id="fill_nulls")
            deduplicate >> fill_nulls

        with TaskGroup("enrichment") as enrichment:
            geocode = EmptyOperator(task_id="geocode")
            currency_convert = EmptyOperator(task_id="currency_convert")

        cleaning >> enrichment

    with TaskGroup("output") as output:
        write_warehouse = EmptyOperator(task_id="write_warehouse")
        write_cache = EmptyOperator(task_id="write_cache")
        send_report = EmptyOperator(task_id="send_report")
        write_warehouse >> send_report

    end = EmptyOperator(task_id="end")

    start >> ingestion >> processing >> output >> end
