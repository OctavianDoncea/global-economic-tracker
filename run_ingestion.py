"""Main entry point for the data ingestion pipeline"""

import argparse
import sys
from datetime import datetime
from dotenv import load_dotenv
from ingestion.utils import setup_logging
from ingestion import worldbank, fred, ecb, owid_csv
from ingestion.loader import load_dataframe, get_warehouse_type

load_dotenv()
logger = setup_logging('pipeline')

# Source runners
def run_worldbank() -> dict:
    countries_df, observations_df = worldbank.extract_all()

    load_dataframe(countries_df, 'worldbank_countries', mode='replace')
    load_dataframe(observations_df, 'worldbank_countries', mode='append')

    return {
        'countries': len(countries_df),
        'observations': len(observations_df)
    }

def run_fred() -> dict:
    df = fred.extract_all()
    load_dataframe(df, 'fred_observations', mode='append')

    return {'observations': len(df)}

def run_ecb() -> dict:
    df = ecb.extract_all()
    load_dataframe(df, 'ecb_observations', mode='append')

    return {'obsservations': len(df)}

def run_owid() -> dict:
    df = owid_csv.extract_all()
    load_dataframe(df, 'owid_co2_data', mode='replace')

    return {'observations': len(df)}

# Main
def main():
    parser = argparse.ArgumentParser(description='Run the global economic indicators data ingestion pipeline.')
    parser.add_argument('--source', choices=['worldbank', 'fred', 'ecb', 'owid', 'all'], default='all', help='Which data source to extract (default: all)')
    args = parser.parse_args()

    # Pipeline header
    start_time = datetime.now()
    warehouse = get_warehouse_type()

    logger.info('GLOBAL ECONOMIC INDICATORS - INGESTION PIPELINE')
    logger.info(f'Started at : {start_time.isoformat()}')
    logger.info(f'Source     : {args.source}')
    logger.info(f'Warehouse  : {warehouse}')
    logger.info('')

    runners = {
        'worldbank': ('World Bank', run_worldbank),
        'fred': ('FRED', run_fred),
        'ecb': ('ECB', run_ecb),
        'owid': ('OWID CO2 CSV', run_owid)
    }

    if args.source == 'all':
        sources_to_run = list(runners.keys())
    else:
        sources_to_run = [args.source]

    results = {}
    failures = []

    for source_key in sources_to_run:
        source_name, runner_func = runners[source_key]
        try:
            result = runner_func()
            results[source_name] = result
        except Exception as e:
            logger.error(f'{source_name} FAILED: {e}', exc_info=True)
            failures.append(source_name)

    # Pipeline summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info('')
    logger.info('PIPELINE SUMMARY')

    for source_name, result in results.items():
        detail = ', '.join(f'{k}: {v:,}' for k, v in result.items())
        logger.info(f' {source_name:20s} - {detail}')

    for source_name in failures:
        logger.info(f" {source_name:20s} FAILED")

    logger.info('')
    logger.info(f' Duration: {duration:.1f} seconds')
    logger.info(f' Finished: {end_time.isoformat()}')

    if failures:
        logger.error(f' {len(failures)} source(s) failed!')
        sys.exit(1)
    else:
        logger.info(' All sources completed successfully!')

if __name__ == '__main__':
    main()