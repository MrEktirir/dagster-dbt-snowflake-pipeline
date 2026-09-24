# Dagster + dbt + Snowflake Data Pipeline

An asset-oriented data engineering pipeline that ingests NYC Yellow Taxi data into Snowflake, transforms it with dbt, and orchestrates the complete workflow with Dagster.

This project was built to explore how ingestion, transformation, data quality, lineage, and scheduling can be managed as a unified asset-based pipeline.

## Architecture

```text
NYC Yellow Taxi Parquet
          |
          v
   Dagster Ingestion
          |
          v
Snowflake RAW.TAXI_TRIPS
          |
          v
   dbt Staging Model
     STG_TAXI_TRIPS
          |
          v
     dbt Mart Model
      TRIP_METRICS
```

### Technology Responsibilities

| Technology | Responsibility |
|---|---|
| Dagster | Asset orchestration, dependency management, scheduling, and observability |
| dbt | SQL transformations and data quality tests |
| Snowflake | Data storage and compute |
| Python | External data ingestion and Snowflake loading |
| uv | Python dependency and environment management |

## Data Pipeline

### 1. Ingestion

The `taxi_trips` Dagster asset downloads the January 2023 NYC Yellow Taxi Parquet dataset.

The file is uploaded to a temporary Snowflake stage and loaded into:

```text
TAXI_DATA.RAW.TAXI_TRIPS
```

The ingestion flow is:

```text
CloudFront
    |
    v
Python / Dagster
    |
    v
Temporary local Parquet file
    |
   PUT
    |
    v
Snowflake Temporary Stage
    |
 COPY INTO
    |
    v
RAW.TAXI_TRIPS
```

### 2. Staging

dbt transforms the raw taxi data through:

```text
STG_TAXI_TRIPS
```

The staging model:

- renames source columns
- calculates trip duration
- removes invalid distance and amount values
- restricts the dataset to January 2023

The model is materialized as a Snowflake **view**.

### 3. Analytics Mart

The `TRIP_METRICS` dbt model aggregates the staging data by pickup hour and pickup location.

Metrics include:

- trip count
- average trip distance
- average trip duration
- average total amount
- total revenue
- average tip amount

The model is materialized as a Snowflake **table**.

## Asset Lineage

Dagster represents the complete dependency chain as assets:

```text
taxi_trips
    |
    v
stg_taxi_trips
    |
    v
trip_metrics
```

The dbt source is mapped to the Dagster ingestion asset, allowing Python ingestion and dbt transformations to appear in the same lineage graph.

## Data Quality

dbt tests are executed as Dagster asset checks.

Checks include `not_null` validation for important fields in the staging and mart layers.

A successful pipeline run materializes all three assets and executes five asset checks.

## Pipeline Verification

The final pipeline produced:

| Layer | Object | Rows |
|---|---|---:|
| Raw | `RAW.TAXI_TRIPS` | 3,066,766 |
| Staging | `DBT.STG_TAXI_TRIPS` | 2,998,600 |
| Mart | `DBT.TRIP_METRICS` | 67,536 |

The raw dataset contained timestamps outside the target month. The staging model filters the analytical dataset to January 2023.

## Scheduling

A Dagster schedule is defined for the complete asset pipeline:

```text
0 6 * * *
```

This represents a daily execution at 06:00 UTC.

The schedule is intentionally left disabled in the development environment to avoid unnecessary Snowflake compute usage.

## Key Engineering Lessons

### Asset-oriented orchestration

Instead of treating ingestion and transformation as unrelated scripts, Dagster models them as connected data assets with explicit dependencies.

### dbt schema configuration

The dbt target schema is configured in `profiles.yml`.

Defining the same value again with `+schema` would be interpreted as a custom schema and could result in a schema such as `DBT_DBT`.

### Parquet logical types

The Parquet ingestion required Snowflake to correctly interpret logical timestamp types.

The file format therefore uses:

```sql
USE_LOGICAL_TYPE = TRUE
USE_VECTORIZED_SCANNER = TRUE
```

Without correct logical-type handling, the pipeline could execute successfully while producing invalid timestamp values.

This demonstrated an important distinction:

> A successful pipeline execution does not necessarily guarantee correct data.

Validation of the actual materialized data is still required.

## Project Structure

```text
dagster_snowflake_tutorial/
├── analytics/
│   ├── models/
│   │   ├── sources/
│   │   │   └── raw_taxi.yml
│   │   ├── staging/
│   │   │   ├── stg_taxi_trips.sql
│   │   │   └── staging.yml
│   │   └── marts/
│   │       ├── trip_metrics.sql
│   │       └── marts.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── src/
│   └── dagster_snowflake_tutorial/
│       └── defs/
│           ├── assets/
│           │   └── ingestion.py
│           ├── dbt_transforms/
│           ├── resources.py
│           └── schedules.py
├── pyproject.toml
└── uv.lock
```

## Running the Project

Install the dependencies:

```bash
cd dagster_snowflake_tutorial
uv sync
```

Create a `.env` file containing the required Snowflake connection settings:

```text
SNOWFLAKE_ACCOUNT=<account>
SNOWFLAKE_USER=<user>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_DATABASE=TAXI_DATA
SNOWFLAKE_WAREHOUSE=TAXI_WH
SNOWFLAKE_ROLE=DAGSTER_ROLE
```

Validate the Dagster definitions:

```bash
dg check defs
```

Start Dagster:

```bash
dg dev
```

The complete pipeline can then be materialized from the Dagster UI.

## Dataset

NYC Yellow Taxi Trip Records — January 2023.

Source: NYC Taxi & Limousine Commission (TLC).

## Skills Demonstrated

- Data pipeline orchestration with Dagster
- Asset-based data engineering
- Snowflake ingestion and staging
- dbt transformations
- dbt data quality testing
- Dagster + dbt lineage integration
- Parquet ingestion
- Snowflake role-based access control
- Pipeline scheduling
- Data validation and troubleshooting