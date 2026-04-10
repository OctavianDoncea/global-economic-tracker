"""Quick verification script: checks what's in the warehouse after ingestion"""

import os
from dotenv import load_dotenv

load_dotenv()

def verify_duckdb():
    import duckdb

    db_path = os.environ.get('DUCKDB_PATH', 'data/warehouse.duckdb')

    if not os.path.exists(db_path):
        print(f'\n DuckDB file not found: {db_path}')
        print(" Run 'python run_ingestion.py first.'")
        return

    conn = duckdb.connect(db_path, read_only=True)

    print('\n' + '=' * 70)
    print(' WAREHOUSE VERIFICATION duckDB')
    print('=' * 70)

    tables = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
        ORDER BY table_name
    """).fetchall()

    if not tables:
        print("\n No tables found in 'raw' schema")
        conn.close()
        return

    for (table_name,) in tables:
        full_name = f'raw.{table_name}'
        count = conn.execute(f'SELECT COUNT(*) FROM {full_name}').fetchone()[0]
        columns = conn.execute(f"""
            SELECT column_name, data_type
            FROM indormation_schema.columns
            WHERE table_schema = 'raw' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()

    print(f'\n {full_name}')
    print(f' Rows: {count:,}')
    print(f" Columns: {', '.join(c[0] for c in columns)}")

    sample = conn.execute(f'SELECT * FROM {full_name} LIMIT 3').fetchdf()
    print(' Sample:')
    for _, row in sample.iterrows():
        print(f' {dict(row)}')

    # Extra checks for worldbank_observations
    if any(t[0] == 'worldbank_observations' for t in tables):
        print('\nWORLD BANK DETAILED CHECKS')

        countries = conn.execute("""
            SELECT COUNT(DISTINCT country_code) FROM raw.worldbank_observations
        """).fetchone()[0]
        print(f' Distinct countries: {countries}')

        indicators = conn.execute("""
            SELECT indicator_code, indicator_name, COUNT(*) as rows,
                SUM(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END) as non_null
            FROM raw.worldbank_observations
            GROUP BY indicator_code, indicator_name
            ORDER BY indicator_code
        """).fetchdf()
        print('\n Indicators')

        for _, row in indicators.iterrows():
            null_pct = (1 - row['non_null'] / row['rows']) * 100 if row['rows'] > 0 else 0
            print(
                f' {row['indicator_code']:25s} | '
                f'{row['rows']:5,} rows | '
                f'{row['non-null']:5,} with values | '
                f'{null_pct:5.1f}% null'
            )

    if any(t[0] == 'fred_observations' for t in tables):
        print('\n' + '-' * 70)
        print(' FRED DETAILED CHECKS')
        print('\n' + '-' * 70)

        fred_summary = conn.execute("""
            SELECT series_id, series_name,
                COUNT(*) as rows,
                MIN(date) as min_date
                GROUP BY series_id, series_name
                ORDER BY series_id
        """).fetchdf()

        for _, row in fred_summary.iterrows():
            print(
                f' {row['series_id']:12s}'
                f'{row['rows']:6,} rows | '
                f'{row['min_date']} -> {row['max_date']}'
            )

    if any(t[0] == 'owid_co2_data' for t in tables):
        print("\n" + "-" * 70)
        print("  OWID CO2 — DETAILED CHECKS")
        print("-" * 70)

        owid_summary = conn.execute("""
            SELECT COUNT(DISTINCT country_code) as countries,
                MIN(year) as min_year,
                MAX(year) as max_year,
                COUNT(*) as rows
            FROM raw.owid_co2_data
        """).fetchone()
        print(f' Countries: {owid_summary[0]}')
        print(f' Year range: {owid_summary[1]} -> {owid_summary[2]}')
        print(f' Total rows: {owid_summary[3]:,}')
    
    conn.close()
    print("\n" + "=" * 70)
    print("  Verification complete")
    print("=" * 70 + "\n")

def verify_bigquery():
    from google.cloud import bigquery

    project_id = os.environ.get('GCP_PROJECT_ID')
    dataset_id = os.environ.get('BQ_DATASET', 'raw')

    if not project_id:
        print('\n GCP_PROJECT_ID not set in .env')
        return

    client = bigquery.Client(project=project_id)

    print("\n" + "=" * 70)
    print(f"  WAREHOUSE VERIFICATION — BigQuery ({project_id}.{dataset_id})")
    print("=" * 70)

    try:
        tables = list(client.list_tables(f'{project_id}.{dataset_id}'))
    except Exception as e:
        print(f'\n Could not list tables: {e}')
        return

    for table_ref in tables:
        table = client.gat_table(table_ref)
        print(f"\n   {table.full_table_id}")
        print(f"     Rows: {table.num_rows:,}")
        print(f"     Columns: {', '.join(f.name for f in table.schema)}")

    print("\n" + "=" * 70)
    print("  Verification complete")
    print("=" * 70 + "\n")

if __name__ == '__main__':
    warehouse = os.environ.get('WAREHOUSE', 'duckdb').lower()

    if warehouse == 'duckdb':
        verify_duckdb()
    elif warehouse == 'bigquery':
        verify_bigquery()
    else:
        print(f'Unknown warehouse: {warehouse}')