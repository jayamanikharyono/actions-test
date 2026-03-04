# -*- coding: utf-8 -*-

from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="taskflow_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "analytics"},
    tags=["taskflow", "etl"],
    description="TaskFlow API DAG demonstrating @task decorators and XCom",
)
def taskflow_etl():

    @task()
    def extract():
        return {"users": 150, "orders": 320, "revenue": 48200.50}

    @task(multiple_outputs=True)
    def transform(raw_data: dict):
        return {
            "avg_order_value": round(raw_data["revenue"] / raw_data["orders"], 2),
            "orders_per_user": round(raw_data["orders"] / raw_data["users"], 2),
        }

    @task()
    def load(avg_order_value: float, orders_per_user: float):
        print(f"AOV: {avg_order_value}, Orders/User: {orders_per_user}")

    raw = extract()
    metrics = transform(raw)
    load(metrics["avg_order_value"], metrics["orders_per_user"])


taskflow_etl()
