from __future__ import annotations

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.apache.druid.operators.druid import DruidOperator

DAG_ID = "data_processing_operator_dags"

with DAG(
    dag_id=DAG_ID,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 2),
    catchup=False,
    tags=["data_processing"],
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

    auth_event_ingestion = DruidOperator(
        json_index_file="druid_application/auth_ingestion.json",
        druid_ingest_conn_id="druid_connection",
        task_id="auth_ingestion",
    )

    listen_event_ingestion = DruidOperator(
        json_index_file="druid_application/listen_ingestion.json",
        druid_ingest_conn_id="druid_connection",
        task_id="listen_ingestion",
    )

    page_view_event_ingestion = DruidOperator(
        json_index_file="druid_application/page_view_ingestion.json",
        druid_ingest_conn_id="druid_connection",
        task_id="page_view_ingestion",
    )

    status_change_event_ingestion = DruidOperator(
        json_index_file="druid_application/status_change_ingestion.json",
        druid_ingest_conn_id="druid_connection",
        task_id="status_change_ingestion",
    )

    auth_silver_table_task >> auth_event_ingestion
    listen_silver_table_task >> listen_gold_table_task >> listen_event_ingestion
    page_view_silver_table_task >> page_view_event_ingestion
    status_change_silver_table_task >> status_change_event_ingestion
