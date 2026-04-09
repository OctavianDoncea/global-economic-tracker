"""
Our World in Data CO2 emissions CSV extraction.
Downloads the full CSV from GitHub, filters to our 25 countries, and selects relevant columns.
Source: https://github.com/owid/co2-data
"""

import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone
from ingestion.config import COUNTRIES, OWID_CO2_URL, OWID_CO2_COLUMNS
from ingestion.utils import setup_logging, retry, save_raw_csv

logger = setup_logging('owid_csv')

# Download
@retry(max_attempts=3, delay=5)
def _download_csv(url: str) -> str:
    logger.info(f'Downloading CSV from: {url}')
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    logger.info(f' Download complete: {len(response.text):,} characters')

# Public entry point
def extract_all() -> pd.DataFrame:
    """Download OWID CO2 data, filter to our 25 countries, select relevant columns"""
    logger.info('=' * 50)
    logger.info('OWID CO2 CSV EXTRACTION - START')
    logger.info('=' * 50)

    csv_text = _download_csv(OWID_CO2_URL)
    save_raw_csv(csv_text, 'owid', 'co2_data')

    df = pd.read_csv(StringIO(csv_text), low_memory=False)
    logger.info(f'Raw CSV: {len(df):,} rows, {len(df.columns)} columns')

    df_filtered = df[df['iso_code'].isin(COUNTRIES)].copy()
    logger.info(
        f'Filtered to {len(df_filtered):,} rows'
        f'({df_filtered['iso_code'].nunique()} countries)'
    )

    available = [col for col in OWID_CO2_COLUMNS if col in df_filtered.columns]
    missing = [col for col in OWID_CO2_COLUMNS if col not in df_filtered.columns]

    if missing:
        logger.warning(f' Columns not found in CSV (skipped): {missing}')

    df_filtered = df_filtered[available].copy()

    if 'iso_code' in df_filtered.columns:
        df_filtered = df_filtered.rename(columns={'iso_code': 'country_code'})
    
    df_filtered['loaded_at'] = datetime.now(timezone.utc).isoformat()

    logger.info(f'OWID CO2 CSV EXTRACTION COMPLETE: {len(df_filtered):,} rows')
    return df_filtered