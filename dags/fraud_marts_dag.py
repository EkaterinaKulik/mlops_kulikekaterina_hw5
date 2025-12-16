from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DWH_CONN_ID = "postgres_dwh"

SQL_DIR = "/opt/airflow/sql"
SQL_FILES_IN_ORDER = [
    "00_create_schemas.sql",
    "01_mart_daily_state_metrics.sql",
    "02_mart_fraud_by_category.sql",
    "03_mart_fraud_by_state.sql",
    "04_mart_customer_risk_profile.sql",
    "05_mart_hourly_fraud_pattern.sql",
    "06_mart_merchant_analytics.sql",
]


def _check_raw_table_and_rowcount(min_rows: int = 1) -> None:
    
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

    exists_sql = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'raw'
          AND table_name = 'raw_transactions'
    ) AS exists_flag;
    """

    rowcount_sql = "SELECT COUNT(*) FROM raw.raw_transactions;"

    exists = hook.get_first(exists_sql)[0]
    if not exists:
        raise AirflowException(
            "Source table raw.raw_transactions does not exist. "
            "Check that dwh-init loaded ./data/train.csv and ran initdb SQL."
        )

    cnt = hook.get_first(rowcount_sql)[0]
    if cnt < min_rows:
        raise AirflowException(
            f"Source table raw.raw_transactions is empty (rows={cnt}). "
            "Check that ./data/train.csv exists and was loaded."
        )


with DAG(
    dag_id="fraud_marts_postgres",
    description="Build fraud analytics marts in Postgres from raw.raw_transactions",
    start_date=datetime(2025, 1, 1),
    schedule=None,  
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    tags=["homework", "fraud", "marts", "postgres"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_source = PythonOperator(
        task_id="check_raw_source",
        python_callable=_check_raw_table_and_rowcount,
        op_kwargs={"min_rows": 1},
    )

    sql_tasks = []
    for filename in SQL_FILES_IN_ORDER:
        task = SQLExecuteQueryOperator(
            task_id=f"run_{filename.replace('.sql', '')}",
            conn_id=DWH_CONN_ID,
            sql=f"{SQL_DIR}/{filename}",
        )
        sql_tasks.append(task)

    end = EmptyOperator(task_id="end")

    start >> check_source >> sql_tasks[0]
    for i in range(len(sql_tasks) - 1):
        sql_tasks[i] >> sql_tasks[i + 1]
    sql_tasks[-1] >> end
