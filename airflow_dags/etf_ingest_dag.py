from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG: periodic incremental ingestion of ETFs into DuckDB
default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='etf_incremental_ingest',
    default_args=default_args,
    description='Run etf_ingest.py incrementally each day',
    schedule_interval='0 18 * * *',  # daily at 18:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Note: Adjust venv path as needed for your environment
    VENV_PY = '/path/to/your/repo/.venv/bin/python'
    REPO_DIR = '/path/to/your/repo'
    DB_FILE = f'{REPO_DIR}/etf_data.duckdb'
    TABLE = 'etf_prices'

    ingest_cmd = (
        f"cd {REPO_DIR} && {VENV_PY} etf_ingest.py --symbol SPY --end $(date +%Y-%m-%d) "
        f"--incremental --db {DB_FILE} --table {TABLE} --chunk-days 7 --pause 1.0"
    )

    run_ingest = BashOperator(
        task_id='run_incremental_ingest',
        bash_command=ingest_cmd,
        env={},
    )

    run_ingest
