"""
Ingestion package for the Global Economic Indicators Tracker.

Modules:
    config      - Country codes, indicator codes, API URLs, and all configuration
    utils       - Logging, retry decorator, file I/O helpers, state management
    worldbank   - World Bank API extraction
    fred        - FRED API extraction
    ecb         - ECB SDMX/XML extraction
    owid_csv    - Our World in Data CSV extraction
    loader      - Warehouse loading (DuckDB or BigQuery)
"""