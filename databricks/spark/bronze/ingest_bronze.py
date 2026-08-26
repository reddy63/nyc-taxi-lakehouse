"""
databricks/spark/bronze/ingest_bronze.py

BRONZE ingestion — Databricks Runtime edition.

Changes vs local spark/bronze/ingest_bronze.py
  1. get_spark()         →  Databricks shim (returns cluster session)
  2. s3a://              →  s3://  (DBIO native; IAM creds from instance profile)
  3. spark.stop()        →  REMOVED  (cluster manages lifecycle)
  4. optimize_delta()    →  spark.sql(OPTIMIZE ...) via Unity Catalog table name,
                             not a raw path (respects UC governance)
  5. upsert_delta()      →  target_path is still an S3 path; Unity Catalog
                             external table registration is done once in setup,
                             not on every run.

Everything else — add_bronze_metadata(), build_landing_paths(), the MERGE
logic inside delta_utils.upsert_delta() — is identical.

Run on Databricks:
  Configured as a Python task in the Asset Bundle job definition.
  Parameters passed via Databricks Widgets or job task parameters:
    --year  2024  --months  01,02,03
"""
from __future__ import annotations
import argparse
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# DATABRICKS: import from databricks/ subtree so the shim spark_session is used
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from spark.utils.spark_session import get_spark
from spark.utils.delta_utils import upsert_delta, optimize_delta

# ── constants ────────────────────────────────────────────────────────────────
# DATABRICKS: s3:// (DBIO) instead of s3a:// (Hadoop S3A)
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "nyc-taxi-landing")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET",  "nyc-taxi-bronze")
TAXI_TYPE      = os.getenv("TAXI_TYPE",       "yellow")

# DATABRICKS: Unity Catalog identifiers for OPTIMIZE (requires UC metastore)
UC_CATALOG = os.getenv("UC_CATALOG", "nyc_taxi")
UC_SCHEMA  = os.getenv("UC_BRONZE_SCHEMA", "bronze")


def build_landing_paths(taxi_type: str, year: int, months: list[str]) -> list[str]:
    # DATABRICKS: s3:// not s3a://
    return [
        f"s3://{LANDING_BUCKET}/{taxi_type}/{year}/{m}/"
        for m in months
    ]


def add_bronze_metadata(df, source_glob: str, batch_id: str):
    """Identical to local version — no Databricks-specific changes."""
    return (
        df
        .withColumn("_ingested_at",  F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_source_file",  F.input_file_name())
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

    raw_df    = spark.read.parquet(*paths)
    print(f"[BRONZE] Raw row count : {raw_df.count():,}")

    bronze_df = add_bronze_metadata(raw_df, str(paths), batch_id)

    # DATABRICKS: s3:// target path
    target_path = f"s3://{BRONZE_BUCKET}/{TAXI_TYPE}/"

    upsert_delta(
        spark,
        bronze_df,
        target_path,
        merge_keys=["VendorID", "tpep_pickup_datetime", "_source_file"],
    )
    print(f"[BRONZE] Written to : {target_path}")

    # DATABRICKS: OPTIMIZE via Unity Catalog table name (preferred over raw path)
    # The external table nyc_taxi.bronze.yellow_trips must be registered once
    # in setup (see resources/setup_unity_catalog.sql).
    try:
        spark.sql(
            f"OPTIMIZE {UC_CATALOG}.{UC_SCHEMA}.{TAXI_TYPE}_trips "
            f"ZORDER BY (tpep_pickup_datetime)"
        )
        print("[BRONZE] OPTIMIZE complete (Unity Catalog).")
    except Exception as e:
        # Fallback: OPTIMIZE by path (works without UC registration)
        optimize_delta(spark, target_path, z_order_cols=["tpep_pickup_datetime"])
        print(f"[BRONZE] OPTIMIZE complete (path fallback): {e}")

    # DATABRICKS: spark.stop() deliberately removed — cluster owns the session


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi — Bronze (Databricks)")
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
