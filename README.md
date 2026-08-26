# NYC Taxi Data Lakehouse & Analytics Platform (Azure Databricks Edition)

> **Modern Data Stack on Azure — Serverless Databricks + Unity Catalog + dbt**  
> Stack: Python · PySpark · Azure Databricks · Delta Lake · Unity Catalog · dbt · Azure Data Lake Storage Gen2 (ADLS) · Databricks Asset Bundles (DABs)

---

## What This Project Does

This enterprise-grade data pipeline ingests **NYC TLC Yellow Taxi trip records** (public Parquet dataset) and processes them through a fully automated, serverless **Medallion Lakehouse architecture**, producing analytics-ready data marts.

```text
NYC TLC Parquet Data
        │
        ▼
  Azure Data Lake Gen2 (landing)    ←─ Cloud object storage
        │
        ▼
   ┌───────────────────────────────┐
   │  BRONZE  (Delta Lake)        │  Raw rows + audit metadata
   │  _ingested_at, _source_file  │  MERGE-on-insert (idempotent)
   └───────────────────────────────┘
        │  PySpark transform (Serverless)
        ▼
   ┌───────────────────────────────┐
   │  SILVER  (Delta Lake)        │  Cleaned, validated, enriched
   │  + drop bad rows & duplicates│  Business-rule filters
   └───────────────────────────────┘
        │  PySpark aggregation (Serverless)
        ▼
   ┌───────────────────────────────┐
   │  GOLD    (Delta Lake)        │  Business KPIs & dimensions
   │  daily_revenue               │  Managed by Unity Catalog
   │  hourly_demand               │  
   │  location_stats              │  
   │  payment_breakdown           │  
   └───────────────────────────────┘
        │  dbt models (Databricks SQL Warehouse)
        ▼
   Analytics Marts  (dbt)
        │  fct_monthly_revenue  — MoM growth
        │  fct_peak_hours       — demand by hour/day
        └  dim_payment_types    — revenue by payment method

        ▲
        │  orchestrated by
   Databricks Asset Bundles   (DABs Workflow DAG)
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) | Landing zone + Delta Lake store (`abfss://`) |
| **Governance** | Unity Catalog | Metadata management, ACLs, SQL querying |
| **Compute / Orchestration** | Azure Databricks (Serverless Workflows) | Task scheduling + automated compute scaling |
| **Format** | Delta Lake 3.x | ACID transactions, UPSERTs, time-travel |
| **Processing** | PySpark | Bronze/Silver/Gold transforms |
| **Analytics** | dbt (Data Build Tool) | Analytics mart SQL models + data quality tests |
| **CI/CD / IaC** | Databricks Asset Bundles (DABs) | Defining infrastructure and DAGs as code (YAML) |

---

## Project Structure

```text
nyc-taxi-lakehouse/
├── databricks/
│   ├── databricks.yml              Main deployment configuration (DABs)
│   ├── resources/
│   │   └── nyc_taxi_pipeline.yml   The 7-task DAG (Bronze -> Silver -> Gold -> dbt -> Vacuum)
│   ├── spark/
│   │   ├── bronze/                 ingest_bronze.py (ADLS landing -> UC Bronze)
│   │   ├── silver/                 transform_silver.py (Clean & filter data)
│   │   ├── gold/                   aggregate_gold.py (Create base metrics & register to UC)
│   │   └── utils/                  Shared Spark sessions and Delta helpers
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/            Views over Gold Unity Catalog tables
│   │   │   └── marts/              fct_monthly_revenue, fct_peak_hours, dim_payment_types
│   │   ├── dbt_project.yml         dbt configuration
│   │   └── profiles.yml            Databricks SQL Warehouse connection (OAuth U2M via CLI)
├── scripts/
│   └── download_data.sh            Helper script to download TLC data manually
```

---

## How to Run & Deploy

This project uses **Databricks Asset Bundles (DABs)**, which eliminates the need to manually click through the Azure or Databricks UI to upload files or configure jobs. Everything is defined in YAML as code.

### 1. Authenticate to Azure Databricks
Authenticate your local Databricks CLI to your Azure workspace:
```bash
databricks auth login --host https://<your-workspace-id>.azuredatabricks.net
```

### 2. Deploy the Infrastructure
Zip up all PySpark scripts, `dbt` models, and YAML configurations, and deploy the workflow definition to the cloud:
```bash
databricks bundle deploy
```
*This command creates the `nyc_taxi_medallion_pipeline` workflow in your Databricks workspace without executing it.*

### 3. Run the Pipeline
Trigger the deployed Serverless workflow to execute all 7 tasks sequentially:
```bash
databricks bundle run nyc_taxi_medallion_pipeline
```
*You can also run it directly from the Databricks UI under the **Workflows** tab.*

---

## The Pipeline Steps

1. **`bronze_ingestion`**: Reads raw parquet from ADLS, applies audit metadata, and UPSERTs into the Unity Catalog Bronze table.
2. **`silver_transform`**: Drops duplicates, drops negative fares or 0-passenger rides, and casts timestamps.
3. **`gold_aggregation`**: Aggregates the Silver data into 4 daily business-level tables, and guarantees their registration in Unity Catalog.
4. **`dbt_run`**: Connects to the Databricks SQL Warehouse to run SQL transformations that convert Gold tables into complex reporting facts (Marts).
5. **`dbt_test`**: Asserts data quality (e.g. no negative revenues, unique primary keys).
6. **`delta_vacuum`**: Runs garbage collection on all Delta tables, deleting physical files older than 7 days to save Azure storage costs.

---

## Data Lineage & Table Previews

Here is what the actual rows of data look like as they move through the pipeline stages:

### 1. 🥉 Bronze (`nyc_taxi.bronze.yellow_trips`)
*Raw data with ingestion tracking metadata appended.*

| VendorID | tpep_pickup_datetime | passenger_count | fare_amount | _ingested_at | _source_file | _taxi_type |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2 | 2024-01-01 00:05:31 | 1.0 | 11.40 | 2024-10-25T17:01:00Z | yellow_tripdata_2024-01.parquet | yellow |
| 1 | 2024-01-01 00:10:45 | 0.0 | -5.00 | 2024-10-25T17:01:00Z | yellow_tripdata_2024-01.parquet | yellow |

### 2. 🥈 Silver (`nyc_taxi.silver.yellow_trips`)
*Cleaned data. Bad rows (0 passengers, negative fare) and exact duplicates are dropped. Timestamps are cast to native formats.*

| VendorID | tpep_pickup_datetime | passenger_count | trip_distance | fare_amount | payment_type | PULocationID | DOLocationID |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2 | 2024-01-01 00:05:31 | 1 | 2.50 | 11.40 | 1 | 236 | 237 |
| 2 | 2024-01-01 00:15:22 | 2 | 4.10 | 18.50 | 2 | 161 | 142 |

### 3. 🥇 Gold (`nyc_taxi.gold.daily_revenue`)
*PySpark aggregations grouping millions of rows into a single row per day.*

| trip_date | total_trips | total_fare | total_tips | total_revenue | avg_fare | avg_distance_miles |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2024-01-01 | 85,102 | 1,452,100.50 | 120,500.00 | 1,572,600.50 | 17.06 | 3.12 |
| 2024-01-02 | 92,305 | 1,650,220.00 | 150,110.25 | 1,800,330.25 | 17.87 | 3.40 |

### 4. 💎 dbt Marts (`nyc_taxi.marts.fct_monthly_revenue`)
*Final analytics tables created by dbt using window functions (e.g., Month-over-Month Growth).*

| revenue_month | total_trips | total_monthly_revenue | mom_revenue_growth_pct | avg_daily_revenue |
| :--- | :--- | :--- | :--- | :--- |
| 2024-01-01 | 2,850,000 | 45,600,100.00 | NULL | 1,470,970.96 |
| 2024-02-01 | 2,910,000 | 48,100,500.00 | + 5.48 % | 1,658,637.93 |
