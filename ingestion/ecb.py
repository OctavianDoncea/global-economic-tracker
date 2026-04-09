"""
ECB Statistical Data Warehouse extraction via SDMX XML.

Pulls 3 Eurozone series and parses the XML response.
API docs: https://data.ecb.europa.eu/help/api/overview
"""

from smtplib import SMTP_SSL_PORT
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from ingestion.config import ECB_SERIES, ECB_BASE_URL, ECB_START_PERIOD
from ingestion.utils import setup_logging, retry, save_raw_xml

logger = setup_logging('ecb')

# API helpers
@retry(max_attempts=3, delay=5)
def _fetch_sdmx(flow_ref: str, key: str) -> str:
    """
    Fetch raw SDMX XML from the ECB API. Returns the raw XML text.
    """

    url = f'{ECB_BASE_URL}/{flow_ref}/{key}'
    params = {
        'startPeriod': ECB_START_PERIOD,
        'detail': 'dataonly'
    }
    headers = {'Accept': 'application/vnd.sdmx.genericdata+xml;version=2.1'}

    response = requests.get(url, headers=headers, params=params, timeout = 60)
    response.raise_for_status()

    return response.text

# XML parsing
NS_GENERIC = 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'

def _parse_sdmx_xml(xml_text: str, series_name: str) -> list:
    """
    Parse SDMX XML into a list of ebservation dicts.

    Handle two formats:
    1. Generic SDMX (ObsDimension + ObsValue elements)
    2. Structure-specific SDMX (Obs elements with TIME_PERIOD + OBS_VALUE attributes)
    """
    root = ET.fromstring(xml_text)
    records = []

    # Generic SDMX format
    for obs in root.iter(f'{{{NS_GENERIC}}}Obs'):
        obs_dim = obs.find(f'{{{NS_GENERIC}}}ObsDimension')
        obs_val = obs.find(f'{{{NS_GENERIC}}}ObsValue')

        if obs_dim is not None and obs_val is not None:
            date_str = obs_dim.get('value')
            value_str = obs_val.get('value')

            try:
                value = float(value_str) if value_str else None
            except (ValueError, TypeError):
                value = None

            records.append({
                'date': date_str,
                'value': value,
                'series_name': series_name
            })

    # Structure-specific format (fallback)
    if not records:
        logger.info(' Generic format not found, trying structure-specific...')

        for elem in root.iter():
            local_tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag

            if local_tag == 'Obs':
                time_period = elem.get('TIME_PERIOD')
                obs_value = elem.get('OBS_VALUE')

                if time_period and obs_value:
                    try:
                        value = float(obs_value)
                    except (ValueError, TypeError):
                        value = None

                    records.append({
                        'date': time_period,
                        'value': value,
                        'series_name': series_name
                    })

    return records

# Series extraction
def extract_series(series_config: dict) -> pd.DataFrame:
    """
    Extract one ECB series: fetch XML, parse it, return a DataFrame
    """
    flow_ref = series_config['flow_ref']
    key = series_config['key']
    name = series_config['name']
    series_key = f'{flow_ref}/{key}'

    logger.info(f'Extracting: {name} ({series_key})')

    try:
        xml_text = _fetch_sdmx(flow_ref, key)
    except Exception as e:
        logger.error(f' Failed to fetch {name}: {e}')
        return pd.DataFrame

    safe_name = name.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
    save_raw_xml(xml_text, 'ecb', safe_name)

    records = _parse_sdmx_xml(xml_text, name)
    if not records:
        logger.warning(f' No observations parsed from {name}')
        logger.warning(' This might men the XML format is unexpected.')
        logger.warning(' Check the saved XML file in data/raw/eecb/ for debugging.')
        return pd.DataFrame

    df = pd.DataFrame(records)
    df['series_key'] = series_key
    df['loaded_at'] = datetime.now(timezone.utc).isoformat()

    logger.info(f' Extracted {len(df)} observations')
    return df

# Public entry point
def extract_all() -> pd.DataFrame:
    """Pull all ECB series and combine them into  a single DataFrame"""
    logger.info('=' * 50)
    logger.info('ECB EXTRACTION - START')
    logger.info('=' * 50)

    all_dfs = []
    for series_config in ECB_SERIES:
        try:
            df = extract_series(series_config)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f' Series failed: {e}')
            continue

    if not all_dfs:
        logger.error('No data extracted from ECB')
        return pd.DataFrame()

    combined = pd.concat(all_dfs)
    logger.info(f'ECB EXTRACTION COMPLETE: {len(combined)} total rows')
    return combined