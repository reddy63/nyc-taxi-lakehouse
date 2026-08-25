# NYC Taxi Data Lakehouse & Analytics Platform

> **Medallion Architecture on Delta Lake — Bronze → Silver → Gold**  
> Stack: Python · PySpark · Apache Spark 3.5 · Delta Lake 3.2 · Apache Airflow 2.9 · dbt · Docker · MinIO

---

## What This Project Does

This pipeline ingests **NYC TLC Yellow Taxi trip records** (public Parquet dataset, ~3–5 million rows/month) and processes them through a full **Medallion Lakehouse architecture**, producing analytics-ready data marts.

```
NYC TLC Parquet Data  (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
        │
        ▼
  MinIO / Landing Zone          ←─ S3-compatible object storage
        │
        ▼
   ┌───────────────────────────────┐
   │  BRONZE  (Delta Lake)        │  Raw rows + audit metadata
   │  _ingested_at, _batch_id     │  MERGE-on-insert (idempotent)
   └───────────────────────────────┘
        │  PySpark transform
        ▼
   ┌───────────────────────────────┐
   │  SILVER  (Delta Lake)        │  Cleaned, validated, enriched
   │  + trip_duration_minutes     │  Deduplication via Window fn
   │  + speed_mph, fare_per_mile  │  Business-rule filters
   │  + time_of_day_bucket        │  Z-ORDER for fast reads
   └───────────────────────────────┘
        │  PySpark aggregation
        ▼
   ┌───────────────────────────────┐
   │  GOLD    (Delta Lake)        │  Business KPIs & dimensions
   │  daily_revenue               │  daily_revenue
   │  hourly_demand               │  hourly_demand
   │  location_stats              │  location_stats
   │  payment_breakdown           │  payment_breakdown
   └───────────────────────────────┘
        │  dbt models
        ▼
   Analytics Marts  (dbt + DuckDB)
        │  fct_monthly_revenue  — MoM growth
        │  fct_peak_hours       — demand by hour/day
        └  dim_payment_types    — revenue by payment method

        ▲
        │  orchestrated by
     Apache Airflow   (monthly DAG)
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Storage | MinIO (S3-compatible) | Landing zone + Delta Lake store |
| Format | Delta Lake 3.2 | ACID, time-travel, schema evolution |
| Processing | Apache Spark 3.5 + PySpark | Bronze/Silver/Gold transforms |
| Orchestration | Apache Airflow 2.9 | Monthly pipeline scheduling |
| Analytics | dbt + DuckDB | Analytics mart models + tests |
| Infrastructure | Docker Compose | Local dev stack |

---

## Project Structure

```
nyc-taxi-lakehouse/
├── docker/
│   ├── spark/          Spark image with Delta Lake + S3A JARs
│   └── airflow/        Airflow image with PySpark provider
├── spark/
│   ├── bronze/         ingest_bronze.py  — raw ingestion
│   ├── silver/         transform_silver.py  — clean + enrich
│   ├── gold/           aggregate_gold.py  — KPI aggregation
│   └── utils/          spark_session.py, delta_utils.py, vacuum_all.py
├── dags/
│   └── nyc_taxi_pipeline_dag.py   Airflow DAG (8 tasks)
├── dbt/
│   ├── models/
│   │   ├── staging/    Views over Gold Delta tables
│   │   └── marts/      fct_monthly_revenue, fct_peak_hours, dim_payment_types
│   ├── tests/          dbt schema tests
│   ├── dbt_project.yml
│   └── profiles.yml    DuckDB (dev) + Spark Thrift (prod)
├── scripts/
│   ├── download_data.sh    Download TLC Parquet from public URL
│   └── upload_to_minio.py  Upload to MinIO landing zone
├── config/
│   └── pipeline_config.yaml
├── tests/
│   ├── test_bronze.py
│   └── test_silver.py
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── .env.example
```

---

## Quick Start

### 1. Clone and configure
```bash
git clone https://github.com/reddy63/nyc-taxi-lakehouse.git
cd nyc-taxi-lakehouse
cp .env.example .env
```

### 2. Build and start the stack
```bash
make build
make up
```

Services after `make up`:

| Service | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master UI |  http://localhost:8083 | — |
| Airflow UI | http://localhost:8082 | admin / admin |

### 3. Download data and run the pipeline
```bash
# Download Jan 2024 Yellow Taxi data (~50 MB)
make download YEAR=2024 MONTHS=01

# Upload to MinIO landing zone
make upload  YEAR=2024 MONTHS=01

# Run all three Spark layers
make pipeline YEAR=2024 MONTHS=01

# Or run each layer individually
make bronze YEAR=2024 MONTHS=01
make silver YEAR=2024 MONTHS=01
make gold   YEAR=2024 MONTHS=01
```

### 4. Run dbt models
```bash
make dbt-run    # build fct_monthly_revenue, fct_peak_hours, dim_payment_types
make dbt-test   # validate with schema tests
make dbt-docs   # serve interactive docs at http://localhost:8085
```

### 5. Run tests
```bash
make test
```

---

## Silver Layer — Transformations Applied

| Check | Rule |
|---|---|
| Null guard | Drop rows where fare_amount / trip_distance / pickup_datetime is NULL |
| Fare cap | `fare_amount` must be > 0 and < $2,000 |
| Distance | `trip_distance` must be > 0 and < 500 miles |
| Duration | dropoff must be after pickup |
| Passengers | `passenger_count` between 1 and 8 |
| Rate code | `RatecodeID` in {1,2,3,4,5,6} |
| Dedup | Window dedup on (VendorID, pickup_datetime, dropoff_datetime) |

Derived columns added in Silver:
- `trip_duration_minutes`
- `speed_mph`
- `fare_per_mile`
- `total_charges` (fare + tip + tolls + surcharges)
- `payment_type_desc` (readable label)
- `hour_of_day`, `time_of_day_bucket` (Morning / Afternoon / Evening / Night)

---

## Gold Layer — Business Tables

| Table | Grain | Key Metrics |
|---|---|---|
| `daily_revenue` | One row per day | total_trips, total_revenue, avg_fare, avg_distance |
| `hourly_demand` | Day × Hour | trip_count, avg_fare, time_of_day_bucket |
| `location_stats` | Location zone | pickup_count, dropoff_count, total_revenue |
| `payment_breakdown` | Payment type | trip_share_pct, avg_tip, tip_rate_pct |

---

## Delta Lake Features Demonstrated

- **MERGE (upsert)** — idempotent writes across Bronze and Silver
- **Schema evolution** — `mergeSchema=true` on all writes
- **Z-ORDER clustering** — on `tpep_pickup_datetime` + `PULocationID`
- **OPTIMIZE** — small-file compaction after every layer write
- **VACUUM** — automated cleanup of old Delta log files (7-day retention)
- **Time travel** — `DeltaTable.history()` accessible via `delta_utils.py`
- **Partitioning** — `_year` / `_month` on Bronze and Silver

---

## Airflow DAG — Task Graph

```
download_data → upload_to_landing → bronze_ingestion
                                            ↓
                                    silver_transform
                                            ↓
                                    gold_aggregation
                                            ↓
                                         dbt_run
                                            ↓
                                         dbt_test
                                            ↓
                                       delta_vacuum
```

Schedule: `0 1 1 * *` (monthly, 1st of each month at 01:00 UTC)

---

## Dataset

**Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
**Format:** Parquet (~50–150 MB per month)  
**Fields:** VendorID, pickup/dropoff timestamps, trip distance, passenger count, fare/tip/tolls, payment type, location IDs

Data dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

---

## Author

Arun Reddy — Data Engineer  
Hyderabad, India  
GitHub: [reddy63](https://github.com/reddy63)
