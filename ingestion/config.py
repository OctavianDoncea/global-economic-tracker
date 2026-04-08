"""
Central configuration for all data sources, countries, and indicators.
Change anything here to adjust scope without touching extraction logic
"""

COUNTRIES = [
    # G7
    'USA', 'GBR', 'DEU', 'FRA', 'JPN', 'CAN', 'ITA',
    # Major Emerging
    'CHN', 'IND', 'BRA', 'MEX', 'KOR', 'TUR',
    # Interesting contrasts
    'NOR', 'NGA', 'SGP', 'ARG', 'POL', 'VNM', 'CHL',
    # Regional coverage
    'ZAF', 'SAU', 'IDN', 'KEN', 'EGY'
]

# World Bank indicators
INDICATORS = [
    {
        'code': 'NY.GDP.MKTP.CD',
        'name': 'GDP (current US$)',
        'category': 'Economic',
        'unit': 'US$'
    },
    {
        'code': 'NY.GDP.MKTP.KD.ZG',
        'name': 'GDP growth (annual %)',
        'category': 'Economic',
        'unit': '%'
    },
    {
        'code': 'NY.GDP.PCAP.CD',
        'name': 'GDP per capita (current US$)',
        'category': 'Economic',
        'unit': 'US$'
    },
    {
        'code': 'FP.CPI.TOTL.ZG',
        'name': 'Inflation, consumer prices (annual %)',
        'category': 'Economic',
        'unit': '%'
    },
    {
        'code': 'SL.UEM.TOTL.ZS',
        'name': 'Unemployment (% of labor force)',
        'category': 'Economic',
        'unit': '%'
    },
    {
        "code": "GC.DOD.TOTL.GD.ZS",
        "name": "Government debt (% of GDP)",
        "category": "Economic",
        "unit": "%",
    },
    {
        "code": "SP.POP.TOTL",
        "name": "Population, total",
        "category": "Demographic",
        "unit": "persons",
    },
    {
        "code": "SP.DYN.LE00.IN",
        "name": "Life expectancy at birth (years)",
        "category": "Demographic",
        "unit": "years",
    },
    {
        "code": "NE.TRD.GNFS.ZS",
        "name": "Trade (% of GDP)",
        "category": "Trade",
        "unit": "%",
    },
    {
        "code": "IT.NET.USER.ZS",
        "name": "Internet users (% of population)",
        "category": "Technology",
        "unit": "%",
    },
    {
        "code": "SE.XPD.TOTL.GD.ZS",
        "name": "Education spending (% of GDP)",
        "category": "Government",
        "unit": "%",
    }
]

# FRED series (5 US-focused high frequency series)
FRED_SERIES = [
    {
        "id": "FEDFUNDS",
        "name": "Federal Funds Effective Rate",
        "unit": "%",
        "frequency": "monthly",
    },
    {
        "id": "CPIAUCSL",
        "name": "Consumer Price Index (All Urban Consumers)",
        "unit": "index_1982_84=100",
        "frequency": "monthly",
    },
    {
        "id": "UNRATE",
        "name": "Unemployment Rate",
        "unit": "%",
        "frequency": "monthly",
    },
    {
        "id": "SP500",
        "name": "S&P 500 Index",
        "unit": "index",
        "frequency": "daily",
    },
    {
        "id": "UMCSENT",
        "name": "Consumer Sentiment Index (U. of Michigan)",
        "unit": "index_1966Q1=100",
        "frequency": "monthly",
    },
]

# ECB Series (3 Eurozone series - returns XML)
ECB_SERIES = [
    {
        "flow_ref": "FM",
        "key": "B.U2.EUR.4F.KR.MRR.LEV",
        "name": "ECB Main Refinancing Rate",
        "unit": "%",
    },
    {
        "flow_ref": "EXR",
        "key": "D.USD.EUR.SP00.A",
        "name": "EUR/USD Exchange Rate",
        "unit": "USD per EUR",
    },
    {
        "flow_ref": "ICP",
        "key": "M.U2.N.000000.4.ANR",
        "name": "Eurozone Inflation (HICP)",
        "unit": "%",
    },
]
ECB_START_PERIOD = '2000-01-01'

# OWID CO2 CSV
OWID_CO2_URL = ('https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv')

# Columns to keep from the CSV
OWID_CO2_COLUMNS = ['iso_code', 'country', 'year', 'co2', 'co2_per_capita', 'co2_per_gdp', 'energy_per_capita', 'share_global_co2']

# API Base URLs
WORLDBANK_BASE_URL = 'https://api.worldbank.org/v2'
FRED_BASE_URL = 'https://api.stlouisfed.org/fred'
ECB_BASE_URL = 'https://data-api.ecb.europa.eu/service/data'

# Extraction parameters
WORLDBANK_DATE_RANGE = '1960:2024'
WORLDBANK_PER_PAGE = 1000