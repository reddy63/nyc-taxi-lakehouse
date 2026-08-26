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
