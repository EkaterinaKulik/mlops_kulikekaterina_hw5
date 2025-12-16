from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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

# SQL checks (fail fast if raw table missing or empty)
CHECK_RAW_EXISTS_SQL = """
SELECT 1
FROM information_schema.tables
WHERE table_schema = 'raw'
  AND table_name = 'raw_transactions';
"""

CHECK_RAW_NOT_EMPTY_SQL = """
SELECT CASE
         WHEN (SELECT COUNT(*) FROM raw.raw_transactions) > 0 THEN 1
         ELSE 1/0
       END AS ok;
"""

with DAG(
    dag_id="fraud_marts_postgres",
    description="Build fraud analytics marts in Postgres from raw.raw_transactions",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1},
    tags=["homework", "fraud", "marts", "postgres"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_raw_exists = SQLExecuteQueryOperator(
        task_id="check_raw_exists",
        conn_id=DWH_CONN_ID,
        sql=CHECK_RAW_EXISTS_SQL,
    )

    check_raw_not_empty = SQLExecuteQueryOperator(
        task_id="check_raw_not_empty",
        conn_id=DWH_CONN_ID,
        sql=CHECK_RAW_NOT_EMPTY_SQL,
    )

    sql_tasks = []
    for filename in SQL_FILES_IN_ORDER:
        sql_tasks.append(
            SQLExecuteQueryOperator(
                task_id=f"run_{filename.replace('.sql', '')}",
                conn_id=DWH_CONN_ID,
                sql=f"{SQL_DIR}/{filename}",
            )
        )

    end = EmptyOperator(task_id="end")

    start >> check_raw_exists >> check_raw_not_empty >> sql_tasks[0]
    for i in range(len(sql_tasks) - 1):
        sql_tasks[i] >> sql_tasks[i + 1]
    sql_tasks[-1] >> end
