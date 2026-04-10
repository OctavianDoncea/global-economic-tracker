# Global Economic Indicators Tracker

An end-to-end data pipeline that ingests macroeconomic data from **4 sources**
into a star-schema warehouse, with automated orchestration, data quality checks,
and interactive dashboards.

![Architecture Diagram](docs/architecture_diagram.png)

---

## Data Sources

| Source | Format | Auth | Frequency | Records |
|--------|--------|------|-----------|---------|
| [World Bank API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392) | JSON (paginated) | None | Quarterly | 11 indicators × 25 countries × ~60 years |
| [FRED API](https://fred.stlouisfed.org/docs/api/fred/) | JSON | API key (free) | Daily–Monthly | 5 US economic series |
| [ECB SDMX API](https://data.ecb.europa.eu/help/api/overview) | XML (SDMX 2.1) | None | Monthly | 3 Eurozone series |
| [Our World in Data](https://github.com/owid/co2-data) | CSV | None | Periodic | CO2 & energy data |

## Countries (25)

G7: USA, UK, Germany, France, Japan, Canada, Italy
Emerging: China, India, Brazil, Mexico, South Korea, Turkey
Contrasts: Norway, Nigeria, Singapore, Argentina, Poland, Vietnam, Chile
Regional: South Africa, Saudi Arabia, Indonesia, Kenya, Egypt

## Indicators (11 from World Bank)

| Category | Indicators |
|----------|-----------|
| Economic | GDP, GDP growth, GDP per capita, Inflation, Unemployment, Gov. debt |
| Demographic | Population, Life expectancy |
| Trade | Trade (% of GDP) |
| Technology | Internet users (%) |
| Government | Education spending (% of GDP) |

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.11+ | Ingestion scripts |
| DuckDB / BigQuery | Data warehouse |
| dbt | Transformations (Phase 2) |
| Prefect | Orchestration (Phase 4) |
| Great Expectations | Data quality (Phase 3) |
| Looker Studio | Dashboards (Phase 5) |
| Docker | Containerization (Phase 6) |

## Getting Started

### Prerequisites

- Python 3.11 or higher
- A free FRED API key ([get one here](https://fred.stlouisfed.org/docs/api/api_key.html))

### Setup

```bash
git clone https://github.com/YOUR_USERNAME/global-economic-tracker.git
cd global-economic-tracker
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env and add your FRED_API_KEY