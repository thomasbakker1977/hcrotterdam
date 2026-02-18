ETF Ingestion to DuckDB

This repository contains `etf_ingest.py`, a small Python utility to ingest ETF daily price data into a persistent DuckDB database.

Features
- Chunked downloads to avoid rate limits
- Rate-limiting pause and exponential backoff
- Uses `yfinance` (preferred) with a CSV fallback
- Canonicalizes columns to Date, Open, High, Low, Close, Adj Close, Volume
- Deduplicates by Date before inserting
- Incremental update mode (only fetches dates after the latest saved date)

Requirements
Install the Python dependencies in the repository's virtualenv or your environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Basic usage

```bash
# Full range ingest
python etf_ingest.py --symbol SPY --start 2022-01-01 --end 2022-03-01 --db etf_data.duckdb --table etf_prices

# Chunk size and pause control
python etf_ingest.py --symbol SPY --start 2022-01-01 --end 2022-12-31 --chunk-days 30 --pause 1.5
```

Incremental usage

```bash
# If the DB table already has data, use incremental mode to fetch only new dates.
python etf_ingest.py --symbol SPY --end 2022-12-31 --incremental

# If DB is empty, provide --start when using --incremental
python etf_ingest.py --symbol SPY --start 2022-01-01 --end 2022-12-31 --incremental
```

Notes
- The script creates the DuckDB file and table if they do not exist.
- Incremental mode sets the start date to the day after the maximum `Date` stored in the table.
- Tweak `--chunk-days` and `--pause` to tune for API limits.

License
- MIT-style usage for example code in this repo.

Airflow scheduling
------------------

An example Airflow DAG is provided in `airflow_dags/etf_ingest_dag.py`. It uses a `BashOperator` to run `etf_ingest.py` in incremental mode daily.

Before enabling the DAG:

- Install and initialize Airflow (example using pip):

```bash
export AIRFLOW_HOME=~/airflow
pip install 'apache-airflow'
airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

- Adjust the DAG variables in `airflow_dags/etf_ingest_dag.py`: set `VENV_PY` and `REPO_DIR` to the correct paths on the machine where Airflow runs.

- Copy the `airflow_dags` folder to your Airflow DAGs directory (typically `$AIRFLOW_HOME/dags`), then start the scheduler:

```bash
# copy DAG
cp -r airflow_dags $AIRFLOW_HOME/dags/
# start webserver and scheduler (for testing only)
airflow webserver --port 8080 &
airflow scheduler &
```

The DAG will run daily at 18:00 UTC. Update `schedule_interval` in the DAG file to change cadence.
