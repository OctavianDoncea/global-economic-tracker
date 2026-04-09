"""
FRED (Federal Reserve Economic Data) API extraction.

Pulls 5 US economic series with incremental loading.
API docs: https://fred.stlouisfed.org/docs/api/fred/
"""

import os
import requests
import pandas as pd
from datetime import datetime, timezone
from ingestion.config import FRED_SERIES, FRED_BASE_URL
from ingestion.utils import setup_logging, retry, save_raw_json, load_state, save_state

logger = setup_logging('fred')

# API helpers
def _get_api_key() -> str:
    api_key = os.environ.get('FRED_API_KEY')

    if not api_key or api_key == 'your_fred_api_key_here':
        raise ValueError(
            'FRED_API_KEY not set or still has placeholder value.\n'
            "Get a free ket at: https://fred.stlouisfed.org/docs/api/api_key.html\n"
            "Then set it in your .env file"
        )
    
    return api_key

@retry(max_attempts=3, delay=5)
def _fetch_observations(series_id: str, api_key: str, observation_start: str = None) -> dict:
    url = f'{FRED_BASE_URL}/series/observations'
    params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json'
    }

    if observation_start:
        params['observation_start'] = observation_start

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

# Series extraction
def extract_series(series_id: str, series_name: str, api_key: str) -> pd.DataFrame:
    """
    Pull observations for one FRED series.
    Uses incremental loading: if we've pulled this series before, only requests data from the last known observation date onward
    """

    logger.info(f'Extracting: {series_name} ({series_id})')

    state=load_state('fred')
    last_date = state.get(series_id)

    if last_date:
        logger.info(f' Incremental load from {last_date}')
    else:
        logger.info(' Ful load (no previous state found)')
    
    data = _fetch_observations(series_id, api_key, observation_start=last_date)
    save_raw_json(data, 'fred', f'series_{series_id}')
    observations = data.get('observations', [])
    records = []

    for obs in observations:
        raw_value = obs.get('value')

        if raw_value == '.' or raw_value is None:
            value = None
        else:
            try:
                value = float(raw_value)
            except (ValueError, TypeError):
                value = None

        records.append(
            {
                'series_id': series_id,
                'series_name': series_name,
                'date': obs.get('date'),
                'value': value
            }
        )

    df = pd.DataFrame(records)
    df['loaded_at'] = datetime.now(timezone.utc).isoformat()

    if not df.empty and df['date'].notna().any():
        latest_date = df['date'].max()
        state[series_id] = latest_date
        save_state('fred', state)
        logger.info(f' State updated: last_date = {latest_date}')

    non_null = df['value'].notna().sum() if not df.empty else 0
    logger.info(f' Extracted {len(df)} rows ({non_null} with values)')

    return df

# Public entry
def extract_all() -> pd.pd.DataFrame:
    logger.info('=' * 50)
    logger.info('FRED EXTRACTION - START')
    logger.info('=' * 50)

    api_key = _get_api_key()
    all_dfs = []

    for series in FRED_SERIES:
        try:
            df = extract_series(series['id'], series['name'], api_key)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f' Failed to extract {series['id']}: {e}')
            continue

    if not all_dfs:
        logger.error(f'No data extracted from FRED')
        return pd.DataFrame

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f'FRED EXTRACTION COMPLETE: {len(combined)} total rows')
    return combined