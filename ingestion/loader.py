"""
Warehouse loader: loads DataFrames into DuckDB or BigQuery

The warehouse type is controlled by the WAREHOUSE environment variable.
Default: duckdb
"""
import os
from duckdb import Value
import pandas as pd
from pathlib import Path
from ingestion.utils import setup_logging

logger = setup_logging('loader')

def get_warehouse_type() -> str:
    return os.environ.get('WAREHOUSE', 'duckdb').lower()

# DuckDB implementation
def _get_duckdb_connection():
    import duckdb

    db_path = os.environ.get('DUCKDB_PATH', 'data/warehouse.duckdb')
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute('CREATE SCHEMA IF NOT EXISTS raw')

    return conn

def _load_to_duckdb(df: pd.DataFrame, table_name: str, mode: str = 'append'):
    conn = _get_duckdb_connection()
    full_table = f'raw.{table_name}'

    try:
        if mode == 'replace':
            conn.execute(f'DROP TABLE IF EXISTS {full_table}')
            conn.execute(f'CREATE TABLE {full_table} AS SELECT * FROM df')
        else:
            try:
                conn.execute(f'INSERT INTO {full_table} SELECT * FROM df')
            except Exception:
                conn.execute(f'CREATE TABLE {full_table} AS SELECT * FROM df')
        
        count = conn.execute(f'SELECT COUNT(*) FROM {full_table}').fetchone()[0]
        logger.info(f' {full_table}, now has {count:,} rows')
    finally:
        conn.close()

# BigQuery implementation
def _load_to_bigquery(df: pd.DataFrame, table_name: str, mode: str = 'append'):
    from google.cloud import bigquery

    project_id = os.environ.get('GCP_PROJECT_ID')
    dataset_id = os.environ.get('BQ_DATASET', 'raw')

    if not project_id:
        raise ValueError(
            'GCP_PROJECT_ID environment variable not set.\n'
            'Set it in your .env file.'
        )

    client = bigquery.Client(project=project_id)
    dataset_ref = f'{project_id}.{dataset_id}'

    try: 
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = os.environ.get('BQ_LOCATION', 'US')
        client.create_dataset(dataset)
        logger.info(f' Created BigQuery dataset: {dataset_ref}')

    if mode == 'replace':
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    table_ref = f'{dataset_ref}.{table_name}'
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    table = client.get_table(table_ref)
    logger.info(f' {table_ref} now has {table.num_rows:,} rows')

# Public interface
def load_dataframe(df: pd.DataFrame, table_name: str, mode: str = 'append'):
    if df.empty:
        logger.warning(f' Skipping {table_name} - DataFrame is empty')
        return
    
    warehouse = get_warehouse_type()
    logger.info(f'Loading {len(df):,} rows -> {warehouse}://raw.{table_name} (mode={mode})')

    if warehouse == 'bigquery':
        _load_to_bigquery(df, table_name, mode)
    elif warehouse == 'duckdb':
        _load_to_duckdb(df, table_name, mode)
    else:
        raise ValueError(
            f"Unknown warehouse type: '{warehouse}'. "
            f"Set WAREHOUSE to 'duckdb' or 'bigquery' in your .env file."
        )