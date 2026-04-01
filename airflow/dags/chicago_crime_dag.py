from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner'           : 'saketh',
    'retries'         : 3,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id          = 'chicago_crime_pipeline',
    default_args    = default_args,
    description     = 'Daily Chicago Crime data ingestion and dbt transformation',
    schedule        = '@daily',
    start_date      = datetime(2025, 1, 1),
    catchup         = False,
    tags            = ['chicago', 'crime', 'snowflake', 'dbt'],
) as dag:

    # ── Task 1: Run Python ingestion script ──
    ingest_data = BashOperator(
        task_id  = 'ingest_crime_data',
        bash_command = 'cd /opt/airflow && pip install -q snowflake-connector-python[pandas] requests python-dotenv && python chicago_crime_load.py',
    )

    # ── Task 2: Run dbt transformations ──
    dbt_run = BashOperator(
        task_id      = 'dbt_run',
        bash_command = 'pip install -q dbt-snowflake && dbt run --project-dir /opt/airflow/dags/chicago_crime_dbt --profiles-dir /opt/airflow/dags',
    )

    # ── Task 3: Run dbt tests ──
    dbt_test = BashOperator(
        task_id      = 'dbt_test',
        bash_command = 'dbt test --project-dir /opt/airflow/dags/chicago_crime_dbt --profiles-dir /opt/airflow/dags',
    )

    # ── Task 4: Generate dbt docs ──
    dbt_docs = BashOperator(
        task_id      = 'dbt_docs_generate',
        bash_command = 'dbt docs generate --project-dir /opt/airflow/dags/chicago_crime_dbt --profiles-dir /opt/airflow/dags',
    )

    # ── Pipeline order ──
    ingest_data >> dbt_run >> dbt_test >> dbt_docs