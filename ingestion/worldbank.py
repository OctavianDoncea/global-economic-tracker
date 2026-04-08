"""
World Bank API extraction.
Pulls 11 indicators for 25 countries plus country metadata.
API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
"""

from numpy.random import noncentral_chisquare
import requests
import pandas as pd
from datetime import datetime, timezone
from ingestion.config import COUNTRIES, INDICATORS, WORLDBANK_BASE_URL, WORLDBANK_DATE_RANGE, WORLDBANK_PER_PAGE
from ingestion.utils import setup_logging, retry, save_raw_json

logger = setup_logging('worldbank')

# API helpers

def _country_codes_str() -> str:
    return ';'.join(COUNTRIES)

@retry(max_attempts=3, delay=5)
def _fetch_page(url: str, params: dict) -> list:
    """
    Fetch one page from the World Bank API
    
    The API returns a list: [metadata_dict, data_list].
    On error or empty response, returns an empty list.
    """
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if isinstance(data, list) and len(data) == 2:
        return data
    else:
        logger.warning(f'Unexpected API response structure: {type(data)}')
        return []

# Country metadata extraction
def extract_country_metadata() -> pd.DataFrame:
    logger.info()

    url = f'{WORLDBANK_BASE_URL}/country/{_country_codes_str()}'
    params = {'format': 'json', 'per_page': 100}
    data = _fetch_page(url, params)

    if not data:
        logger.error('No country metadata returned from API')
        return pd.DataFrame()

    save_raw_json(data, 'worldbank', 'country_metadata')

    metadata = data[0]
    countries = data[1]

    if countries is None:
        logger.error('Country list is None')
        return pd.DataFrame

    records = []
    for country in countries:
        lon = country.get('longitude')
        lat = country.get('latitude')

        records.append(
            {
                'country_code': country.get('id'),
                'country_name': country.get('name'),
                'region': country.get('region', {}).get('value'),
                'income_group': country.get('incomeLevel', {}).get('value'),
                'capital_city': country.get('capitalCity'),
                'longitude': float(lon) if lon else None,
                'latitude': float(lat) if lat else None,
                'loaded_at': datetime.now(timezone.utc).isoformat()
            }
        )

    df = pd.DataFrame(records)
    logger.info(f'Extracted metadata for {len(df)} countries')
    return df

# Indicator extraction
def extract_indicator(indicator_code: str, indicator_name: str) -> pd.DataFrame:
    """
    Pull all observations for one indicator across all 25 countries.

    Handles pagination automatically.
    Returns a DataFrame with: country_code, country_name, indicator_code, indicator_name, year, value, loaded_at
    """

    logger.info(f'Extracting: {indicator_name} ({indicator_code})')

    url = f'{WORLDBANK_BASE_URL}/country/{_country_codes_str()}/indicator/{indicator_code}'
    all_records = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        params = {
            'format': 'json',
            'per_page': WORLDBANK_PER_PAGE,
            'date': WORLDBANK_DATE_RANGE,
            'page': page
        }
        data = _fetch_page(url, params)

        if not data:
            logger.warning(f' No data on page {page}')
            break

        metadata = data[0]
        observations = data[1]

        total_pages = metadata.get('pages', 1)

        if observations is None:
            logger.warning(f' Null observations on page {page}')
            break

        if page == 1:
            save_raw_json(data, 'worldbank', f'indicator_{indicator_code}_p{page}')

        for obs in observations:
            raw_value = obs.get('value')
            value = float(raw_value) if raw_value is not None else None
            raw_year = obs.get('date')

            try:
                year = int(raw_year) if raw_year else None
            except ValueError:
                year = None

            all_records.append(
                {
                    'country_code': obs.get('country', {}).get('id'),
                    'country_name': obs.get('country', {}).get('value'),
                    'indicator_code': obs.get('indicator', {}).get('id'),
                    'indicator_name': obs.get('indicator', {}).get('value'),
                    'year': year,
                    'value': value
                }
            )
        
        logger.info(f' Page {page}/{total_pages} - {len(observations)} observations')
        page += 1

    df = pd.DataFrame(all_records)
    df['loaded_at'] = datetime.now(timezone.utc).isoformat()

    non_null = df['value'].notna().sum() if not df.empty else 0
    null = df['value'].isna().sum() if not df.empty else 0
    logger.info(f' Total: {len(df)} rows ({non_null}) with values, {null} null')

    return df

def extract_all_indicators() -> pd.DataFrame:
    logger.info(f'Starting extraction for {len(INDICATORS)} indicators...')

    all_dfs = []
    for indicator in INDICATORS:
        df = extract_indicator(indicator['code'], indicator['name'])
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        logger.error('No indicator data extracted')
        return pd.DataFrame

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f'All indicators extracted: {len(combined)} total rows')
    return combined

# Public entry point
def extract_all() -> tuple:
    """Main entry point for World Bank extraction."""
    logger.info('=' * 50)
    logger.info('WORLD BANK EXTRACTION - START')
    logger.info('=' * 50)

    countries_df = extract_country_metadata()
    observations_df = extract_all_indicators()

    logger.info('WORLD BANK EXTRACTION - COMPLETE')
    return countries_df, observations_df