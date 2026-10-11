# dags/ingest_mes_hourly.py
import yaml
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from mes_extract import extract_to_s3, load_to_snowflake

with open("/opt/airflow/dags/config/mes_tables.yml") as f:
    CONFIG = yaml.safe_load(f)

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5 → 10 → 20 分钟
    "on_failure_callback": notify_slack,
}

with DAG(
    dag_id="ingest_mes_hourly",
    schedule="10 * * * *",               # 每小时第 10 分钟跑
    start_date=datetime(2024, 1, 1),
    catchup=False,                       # 部署时不自动补跑过去几年
    max_active_runs=1,                   # 上一次没跑完，下一次不启动
    default_args=default_args,
    tags=["mes", "ingestion"],
) as dag:

    for table in CONFIG["tables"]:
        with TaskGroup(group_id=table["name"].lower()):

            extract = PythonOperator(
                task_id="extract",
                python_callable=extract_to_s3,
                op_kwargs={
                    "conn_id": CONFIG["source_conn_id"],
                    "table": table,
                    # Airflow 自动传入这次运行负责的时间段
                    "interval_start": "{{ data_interval_start }}",
                    "interval_end": "{{ data_interval_end }}",
                },
                pool="mes_db_pool",       # pool 大小设为 2：最多 2 张表同时查 MES
            )

            load = PythonOperator(
                task_id="load",
                python_callable=load_to_snowflake,
                op_kwargs={"table": table, "target_schema": CONFIG["target_schema"]},
                outlets=[Dataset(f"snowflake://raw/mes/{table['name'].lower()}")],
            )

            extract >> load
