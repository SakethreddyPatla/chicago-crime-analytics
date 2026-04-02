# Chicago Crime Data Pipeline

An end-to-end data engineering pipeline that ingests 8M+ Chicago crime records from the Socrata Open Data API into Snowflake, transforms them with dbt Core, and orchestrates daily runs with Apache Airflow on Docker.

---

## Purpose

Chicago publishes one of the most comprehensive open crime datasets in the United States — 8 million records spanning over 20 years, updated daily by the Chicago Police Department. The goal of this project is to analyze crime trends across the city: which neighborhoods are most affected, how arrest rates have changed over time, and which crime types are rising or falling year over year.

The raw data from the Socrata API is difficult to work with directly — inconsistent types, noisy computed columns, and no analytical layer on top. This pipeline solves that by building a clean, automated, and always up-to-date analytical foundation.

## Solution

- **Analytics-ready tables** — dbt transforms the raw API data into a clean staging layer and 8 pre-aggregated mart tables covering crime trends by type, location, district, community area, and year-over-year change. Any BI tool can connect directly to the marts without additional data wrangling.

- **Automated daily updates** — an Airflow DAG running on Docker schedules the full pipeline every day: ingest new records → run dbt transformations → run data quality tests → regenerate docs. Once deployed, the data stays current with zero manual effort.

- **Reliable ingestion at scale** — the Socrata API drops connections on large requests, so the ingestion script fetches in 20k-row batches with exponential backoff retry logic, writing to Snowflake every 200k rows to prevent memory overload and preserve partial progress across runs.

- **No duplicate records** — daily incremental runs fetch only records updated since the last load and use a `MERGE` statement keyed on `CASE_NUMBER` to upsert changes, so re-running the pipeline never creates duplicates.

---

---

## Architecture

```
Socrata API (8M+ records)
        │
        ▼
Python Ingestion Script
(chunked fetch + retry logic)
        │
        ▼
Snowflake RAW.CRIME_INFO
(incremental MERGE on CASE_NUMBER)
        │
        ▼
dbt Core Transformations
        │
        ├── STAGING.stg_crime_info (view)
        │
        └── MARTS
            ├── fct_crimes
            ├── mart_crimes_by_type
            ├── mart_crimes_by_location
            ├── mart_monthly_trends
            ├── mart_arrest_rate_trends
            ├── mart_top_dangerous_districts
            ├── mart_crime_hotspots
            └── mart_yoy_crime_change
        │
        ▼
Apache Airflow (Docker)
(daily scheduled DAG)
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Requests, Socrata Open Data API |
| Storage | Snowflake |
| Transformation | dbt Core 1.11 |
| Orchestration | Apache Airflow 2.9 (Docker) |
| Language | Python 3.13 |

---

## Project Structure

```
chicago-crime/
├── chicago_crime_load.py       # Ingestion script
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
├── chicago_crime_dbt/          # dbt project
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── staging.yml
│       │   └── stg_crime_info.sql
│       └── marts/
│           ├── marts.yml
│           ├── fct_crimes.sql
│           ├── mart_crimes_by_type.sql
│           ├── mart_crimes_by_location.sql
│           ├── mart_monthly_trends.sql
│           ├── mart_arrest_rate_trends.sql
│           ├── mart_top_dangerous_districts.sql
│           ├── mart_crime_hotspots.sql
│           └── mart_yoy_crime_change.sql
└── airflow/
    ├── docker-compose.yml
    └── dags/
        ├── chicago_crime_dag.py
        └── profiles.yml
```

---

## Key Features

**Ingestion**
- Fetches 8M+ records from the Socrata API in 20k-row batches
- Chunked loading — writes to Snowflake every 200k rows to avoid memory issues
- Exponential backoff retry on connection resets and timeouts
- Incremental loads use `MERGE` on `CASE_NUMBER` to upsert changed records without duplicates
- Full load vs incremental load auto-detected based on `MAX(UPDATED_ON)` in Snowflake

**Transformation**
- Staging layer cleans and casts raw API fields into typed columns
- Mart layer provides pre-aggregated analytical tables for BI tools
- `QUALIFY` + window functions used for ranking districts and hotspots
- `LAG()` used for year-over-year crime change calculations
- dbt tests cover uniqueness, not_null, accepted_values across all models

**Orchestration**
- Airflow DAG runs daily at 6am UTC
- Tasks: `ingest → dbt run → dbt test → dbt docs generate`
- 3 retries with 5-minute delay on each task
- Environment variables passed securely via Docker Compose

---

## dbt Models

| Model | Materialization | Description |
|---|---|---|
| `stg_crime_info` | View | Cleaned and typed staging layer |
| `fct_crimes` | Table | Core facts table with all dimensions |
| `mart_crimes_by_type` | Table | Crime counts and arrest rates by type and year |
| `mart_crimes_by_location` | Table | Crime counts by district and community area |
| `mart_monthly_trends` | Table | Monthly crime volume trends by year |
| `mart_arrest_rate_trends` | Table | Year-over-year arrest rate changes |
| `mart_top_dangerous_districts` | Table | Top 10 districts by crime count per year |
| `mart_crime_hotspots` | Table | Top 20 community area hotspots per year |
| `mart_yoy_crime_change` | Table | Year-over-year crime count and percentage change |

---

## Setup & Running Locally

### Prerequisites
- Python 3.10+
- Docker Desktop
- Snowflake account
- Socrata App Token ([register here](https://data.cityofchicago.org/login))

### 1. Clone the repo
```bash
git clone https://github.com/SakethreddyPatla/chicago-crime.git
cd chicago-crime
```

### 2. Create and activate virtual environment
```bash
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
cp .env.example .env
# Fill in your Snowflake and Socrata credentials in .env
```

### 5. Set up Snowflake
```sql
CREATE DATABASE IF NOT EXISTS CHICAGO_CRIME_DB;
CREATE SCHEMA IF NOT EXISTS CHICAGO_CRIME_DB.RAW;
CREATE SCHEMA IF NOT EXISTS CHICAGO_CRIME_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS CHICAGO_CRIME_DB.MARTS;
```

### 6. Run the ingestion script
```bash
python chicago_crime_load.py
```

### 7. Run dbt transformations
```bash
cd chicago_crime_dbt
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### 8. Start Airflow
```bash
cd airflow
docker-compose up airflow-init
docker-compose up -d
# Open http://localhost:8082 (admin / admin)
```

---

## Data Source

[City of Chicago — Crimes 2001 to Present](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

- ~8 million records
- Updated daily by the Chicago Police Department
- Fields include crime type, location, arrest status, district, community area, and coordinates