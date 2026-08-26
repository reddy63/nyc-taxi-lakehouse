"""
databricks/spark/bronze/ingest_bronze.py

BRONZE ingestion - Databricks Runtime + Azure ADLS Gen2 edition.

Changes vs local spark/bronze/ingest_bronze.py
  1. get_spark()         ->  Databricks shim (returns cluster session)
  2. s3a://               ->  abfss://container@account.dfs.core.windows.net/
     (this workspace is Azure Databricks - see databricks.yml's adb-...
     azuredatabricks.net host and .env.databricks's ADLS_STORAGE_ACCOUNT -
     not AWS, so this is abfss://, not s3:///s3a://)
  3. spark.stop()        ->  REMOVED  (cluster manages lifecycle)
  4. optimize_delta()    ->  tries OPTIMIZE via the Unity Catalog table name
     first (nyc_taxi.bronze.yellow_trips, registered once by
     resources/setup_unity_catalog.sql), falls back to path-based OPTIMIZE
     if that external table hasn't been registered yet.

Everything else - add_bronze_metadata(), build_landing_paths(), the MERGE
logic inside delta_utils.upsert_delta() - is identical to the local version.

Run on Databricks:
  Configured as a spark_python_task in resources/nyc_taxi_pipeline.yml.
  Parameters passed as job task parameters: --year 2024 --months 01,02,03
"""
from __future__ import annotations
import argparse
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import inspect
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda:0)))

sys.path.insert(0, os.path.abspath(os.path.join(_script_dir, "../..")))
from spark.utils.spark_session import get_spark
from spark.utils.delta_utils import upsert_delta, optimize_delta

# -- ADLS Gen2 path builder (matches transform_silver.py's adls() helper) --
STORAGE_ACCOUNT   = os.getenv("ADLS_STORAGE_ACCOUNT", "nyctaxilakehouse1")
LANDING_CONTAINER = os.getenv("LANDING_CONTAINER", "landing")
BRONZE_CONTAINER  = os.getenv("BRONZE_CONTAINER",  "bronze")
TAXI_TYPE         = os.getenv("TAXI_TYPE", "yellow")

UC_CATALOG = os.getenv("UC_CATALOG", "nyc_taxi")
UC_SCHEMA  = os.getenv("UC_BRONZE_SCHEMA", "bronze")


def adls(container: str, path: str = "") -> str:
    """Build an abfss:// URI for Azure Data Lake Storage Gen2."""
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def build_landing_paths(taxi_type: str, year: int, months: list[str]) -> list[str]:
    return [
        adls(LANDING_CONTAINER, f"{taxi_type}/{year}/{m}/")
        for m in months
    ]


def add_bronze_metadata(df, source_glob: str, batch_id: str):
    """Identical to local version - no Databricks-specific changes."""
    return (
        df
        .withColumn("_ingested_at",  F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_source_file",  F.col("_metadata.file_path"))
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("_taxi_type",    F.lit(TAXI_TYPE))
        .withColumn("_year",  F.year("tpep_pickup_datetime").cast(StringType()))
        .withColumn("_month", F.lpad(F.month("tpep_pickup_datetime").cast(StringType()), 2, "0"))
    )


def run(year: int, months: list[str]) -> None:
    spark     = get_spark("nyc-taxi-bronze-ingestion")   # returns cluster session
    batch_id  = str(uuid.uuid4())
    paths     = build_landing_paths(TAXI_TYPE, year, months)

    print(f"[BRONZE] Batch {batch_id} | taxi={TAXI_TYPE} | year={year} | months={months}")
    print(f"[BRONZE] Reading from: {paths}")

    raw_df    = spark.read.parquet(*paths)
    print(f"[BRONZE] Raw row count : {raw_df.count():,}")

    bronze_df = add_bronze_metadata(raw_df, str(paths), batch_id)
    
    # Drop duplicates on the merge keys to prevent Delta Merge multiple match errors
    bronze_df = bronze_df.dropDuplicates(["VendorID", "tpep_pickup_datetime", "_source_file"])

    target_path = adls(BRONZE_CONTAINER, f"{TAXI_TYPE}/")

    upsert_delta(
        spark,
        bronze_df,
        target_path,
        merge_keys=["VendorID", "tpep_pickup_datetime", "_source_file"],
    )
    print(f"[BRONZE] Written to : {target_path}")

    # OPTIMIZE via Unity Catalog table name (preferred - respects UC governance
    # and is what makes this show up correctly in Catalog Explorer). The external
    # table nyc_taxi.bronze.yellow_trips is registered once by
    # resources/setup_unity_catalog.sql; fall back to path-based OPTIMIZE if
    # that hasn't been run yet.
    try:
        spark.sql(
            f"OPTIMIZE {UC_CATALOG}.{UC_SCHEMA}.{TAXI_TYPE}_trips "
            f"ZORDER BY (tpep_pickup_datetime)"
        )
        print("[BRONZE] OPTIMIZE complete (Unity Catalog).")
    except Exception as e:
        optimize_delta(spark, target_path, z_order_cols=["tpep_pickup_datetime"])
        print(f"[BRONZE] OPTIMIZE complete (path fallback): {e}")

    # spark.stop() deliberately removed - cluster owns the session


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi - Bronze (Databricks)")
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
