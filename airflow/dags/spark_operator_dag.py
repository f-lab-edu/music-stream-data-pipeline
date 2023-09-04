from __future__ import annotations

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

DAG_ID = "spark_operator_dags"

with DAG(
    dag_id=DAG_ID,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 2),
    catchup=False,
    tags=["spark"],
) as dag:
    auth_silver_table_task = SparkKubernetesOperator(
        application_file="spark_application/auth.yaml",
        namespace="default",
        kubernetes_conn_id="k8s_spark",
        task_id="auth_silver",
    )

    listen_silver_table_task = SparkKubernetesOperator(
        application_file="spark_application/listen.yaml",
        namespace="default",
        kubernetes_conn_id="k8s_spark",
        task_id="listen_silver",
    )

    page_view_silver_table_task = SparkKubernetesOperator(
        application_file="spark_application/page_view.yaml",
        namespace="default",
        kubernetes_conn_id="k8s_spark",
        task_id="page_view_silver",
    )

    status_change_silver_table_task = SparkKubernetesOperator(
        application_file="spark_application/status_change.yaml",
        namespace="default",
        kubernetes_conn_id="k8s_spark",
        task_id="status_change_silver",
    )

    listen_gold_table_task = SparkKubernetesOperator(
        application_file="spark_application/listen_join.yaml",
        namespace="default",
        kubernetes_conn_id="k8s_spark",
        task_id="listen_gold",
    )

    auth_silver_table_task
    listen_silver_table_task >> listen_gold_table_task
    page_view_silver_table_task
    status_change_silver_table_task
