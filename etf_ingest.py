import duckdb
import pandas as pd
import requests
import io
import os
import time
import argparse
from datetime import datetime, timedelta

# Default configuration
DEFAULT_DB = 'etf_data.duckdb'
DEFAULT_TABLE = 'etf_prices'


def unix_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def chunk_date_ranges(start: datetime, end: datetime, chunk_days: int):
    cur = start
    while cur < end:
        next_end = min(end, cur + timedelta(days=chunk_days))
        yield cur, next_end
        cur = next_end + timedelta(days=1)


def fetch_etf_data_range(symbol: str, start_dt: datetime, end_dt: datetime, session: requests.Session, headers: dict, max_retries: int = 5, pause_seconds: float = 1.0):
    # Prefer using yfinance (handles Yahoo auth/crumbs); fall back to CSV endpoint with retries
    try:
        import yfinance as yf
    except Exception:
        # fallback to CSV download
        start_ts = unix_ts(start_dt)
        end_ts = unix_ts(end_dt + timedelta(hours=23, minutes=59, seconds=59))
        url = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_ts}&period2={end_ts}&interval=1d&events=history&includeAdjustedClose=true'
        backoff = pause_seconds
        for attempt in range(1, max_retries + 1):
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                return pd.read_csv(io.StringIO(resp.text))
            if resp.status_code == 429:
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Failed to fetch after {max_retries} retries: {url}")
    else:
        # yfinance download: end is exclusive, so add one day
        yf_start = start_dt.strftime('%Y-%m-%d')
        yf_end = (end_dt + timedelta(days=1)).strftime('%Y-%m-%d')
        df = yf.download(symbol, start=yf_start, end=yf_end, interval='1d', progress=False)
        if df is None or df.empty:
            return pd.DataFrame()
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
        else:
            df = df.reset_index()
        # Ensure column names match Yahoo CSV (Date, Open, High, Low, Close, Adj Close, Volume)
        # yfinance returns 'Adj Close' with same name; keep as-is.
        return df


def store_chunk_in_duckdb(df: pd.DataFrame, db_file: str, table_name: str):
    # Normalize date column in pandas and dedupe before writing to DuckDB
    # Normalize DataFrame to canonical columns: Date, Open, High, Low, Close, Adj Close, Volume
    # If index is DatetimeIndex, use it as Date
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    def col_label(c):
        if isinstance(c, tuple):
            for part in c:
                if isinstance(part, str) and part.strip():
                    return part
            return '_'.join([str(x) for x in c])
        return str(c)

    labels = [col_label(c) for c in df.columns]
    cols_lower = [l.lower() for l in labels]

    canonical = {}
    # Date
    if 'date' in cols_lower:
        canonical['Date'] = df.iloc[:, cols_lower.index('date')]
    else:
        # fallback to first column
        canonical['Date'] = df.iloc[:, 0]

    # OHLCV mapping
    for name in ['open', 'high', 'low', 'close', 'adj close', 'adj_close', 'volume']:
        col = None
        for i, lab in enumerate(cols_lower):
            if name in lab:
                col = df.iloc[:, i]
                break
        if col is not None:
            # normalize 'adj_close' label
            key = 'Adj Close' if 'adj' in name else name.capitalize()
            canonical[key] = col

    # Build canonical DataFrame
    new_df = pd.DataFrame()
    new_df['Date'] = pd.to_datetime(canonical['Date']).dt.strftime('%Y-%m-%d')
    for k in ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']:
        if k in canonical:
            new_df[k] = canonical[k].values

    df = new_df
    df = df.drop_duplicates(subset=['Date'], keep='last')

    con = duckdb.connect(db_file)
    # Ensure table exists with same schema as df (if not exists)
    if not con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='" + table_name.lower() + "'").fetchone()[0]:
        # Create empty table using df schema
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df LIMIT 0;")
    # Insert chunk (pandas DataFrame `df` will be bound by DuckDB)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df;")
    con.close()


def ingest_symbol(symbol: str, start: str, end: str, db_file: str = DEFAULT_DB, table_name: str = DEFAULT_TABLE, chunk_days: int = 30, pause_seconds: float = 1.0):
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; ETFIngest/1.0; +https://example.com)'
    }
    total_rows = 0
    for s, e in chunk_date_ranges(start_dt, end_dt, chunk_days):
        print(f"Fetching {symbol} from {s.date()} to {e.date()}...")
        try:
            df = fetch_etf_data_range(symbol, s, e, session, headers, pause_seconds=pause_seconds)
        except Exception as exc:
            print(f"Failed to fetch chunk {s.date()}-{e.date()}: {exc}")
            continue
        if df.empty:
            print("No rows in this chunk.")
        else:
            store_chunk_in_duckdb(df, db_file, table_name)
            total_rows += len(df)
            print(f"Inserted {len(df)} rows for chunk.")
        time.sleep(pause_seconds)
    print(f"Ingestion complete. Total rows processed (not deduped): {total_rows}")


def get_max_date(db_file: str, table_name: str) -> str | None:
    """Return max Date in table as 'YYYY-MM-DD' or None if table doesn't exist or empty."""
    if not os.path.exists(db_file):
        return None
    con = duckdb.connect(db_file)
    try:
        # check table exists
        exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='" + table_name.lower() + "'").fetchone()[0]
        if not exists:
            return None
        # Determine the actual Date-like column name by inspecting the table columns
        try:
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 1").fetchdf()
        except Exception:
            return None
        if sample.empty and len(sample.columns) == 0:
            return None
        cols = list(sample.columns)
        # pick a column whose name contains 'date' (case-insensitive), otherwise use first column
        date_col = None
        for c in cols:
            if 'date' in str(c).lower():
                date_col = c
                break
        if date_col is None:
            date_col = cols[0]

        col_escaped = str(date_col).replace('"', '""')
        try:
            res = con.execute(f'SELECT MAX(CAST("{col_escaped}" AS DATE))::DATE FROM {table_name}').fetchone()
        except Exception:
            return None
        if not res or res[0] is None:
            return None
        max_date = res[0]
        if isinstance(max_date, str):
            return max_date
        return max_date.strftime('%Y-%m-%d')
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description='ETF ingestion to DuckDB with chunked Yahoo requests')
    parser.add_argument('--symbol', required=True, help='ETF symbol, e.g. SPY')
    parser.add_argument('--start', required=False, help='Start date (YYYY-MM-DD). If --incremental and DB has data, this is optional.')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--db', default=DEFAULT_DB, help='DuckDB file')
    parser.add_argument('--table', default=DEFAULT_TABLE, help='Table name')
    parser.add_argument('--chunk-days', type=int, default=30, help='Days per chunk')
    parser.add_argument('--pause', type=float, default=1.0, help='Seconds pause between requests')
    parser.add_argument('--incremental', action='store_true', help='If set, start date will be set to the day after the latest Date in the DB table (if present).')
    args = parser.parse_args()

    start_arg = args.start
    if args.incremental:
        maxd = get_max_date(args.db, args.table)
        if maxd:
            # start the next calendar day
            next_day = (datetime.fromisoformat(maxd) + timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"Incremental mode: DB latest date {maxd}, starting from {next_day}")
            start_arg = next_day
        else:
            if not start_arg:
                parser.error('--incremental specified but DB is empty and --start not provided')

    if not start_arg:
        parser.error('--start is required when not running --incremental with existing data')

    ingest_symbol(args.symbol, start_arg, args.end, db_file=args.db, table_name=args.table, chunk_days=args.chunk_days, pause_seconds=args.pause)


if __name__ == '__main__':
    main()
