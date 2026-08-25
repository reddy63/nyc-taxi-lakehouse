"""
spark/bronze/ingest_bronze.py

BRONZE LAYER  —  raw ingestion from MinIO landing zone into Delta Lake.

What this job does:
  1. Reads raw NYC TLC Parquet files from s3a://nyc-taxi-landing/
  2. Adds ingestion metadata columns (_ingested_at, _source_file, _batch_id)
  3. Writes to s3a://nyc-taxi-bronze/ as a partitioned Delta table
  4. Idempotent: uses MERGE on (VendorID, tpep_pickup_datetime, _source_file)

Run:
  spark-submit spark/bronze/ingest_bronze.py --year 2024 --months 01,02,03
"""
from __future__ import annotations
import argparse
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import get_spark
from utils.delta_utils import upsert_delta, optimize_delta


# ── constants ────────────────────────────────────────────────────────────────
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "nyc-taxi-landing")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET",  "nyc-taxi-bronze")
TAXI_TYPE      = os.getenv("TAXI_TYPE",       "yellow")


# ── helpers ──────────────────────────────────────────────────────────────────
def build_landing_paths(taxi_type: str, year: int, months: list[str]) -> list[str]:
    return [
        f"s3a://{LANDING_BUCKET}/{taxi_type}/{year}/{m}/"
        for m in months
    ]


def add_bronze_metadata(df, source_glob: str, batch_id: str):
    """Attach audit columns so every Bronze row is fully traceable."""
    return (
        df
        .withColumn("_ingested_at",  F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_source_file",  F.input_file_name())
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("_taxi_type",    F.lit(TAXI_TYPE))
        # Partition key (year-month extracted from pickup timestamp)
        .withColumn("_year",  F.year("tpep_pickup_datetime").cast(StringType()))
        .withColumn("_month", F.lpad(F.month("tpep_pickup_datetime").cast(StringType()), 2, "0"))
    )


def run(year: int, months: list[str]) -> None:
    spark     = get_spark("nyc-taxi-bronze-ingestion")
    batch_id  = str(uuid.uuid4())
    paths     = build_landing_paths(TAXI_TYPE, year, months)

    print(f"[BRONZE] Batch {batch_id} | taxi={TAXI_TYPE} | year={year} | months={months}")
    print(f"[BRONZE] Reading from: {paths}")

    # 1. Read raw Parquet (schema inference — TLC files are consistent)
    raw_df = spark.read.parquet(*paths)
    print(f"[BRONZE] Raw row count : {raw_df.count():,}")

    # 2. Attach metadata
    bronze_df = add_bronze_metadata(raw_df, str(paths), batch_id)

    # 3. Write to Delta (partition by year + month for efficient downstream reads)
    target_path = f"s3a://{BRONZE_BUCKET}/{TAXI_TYPE}/"
    upsert_delta(
        spark,
        bronze_df,
        target_path,
        merge_keys=["VendorID", "tpep_pickup_datetime", "_source_file"],
    )
    print(f"[BRONZE] Written to   : {target_path}")

    # 4. Compact small files (Z-ORDER by pickup datetime for Silver reads)
    optimize_delta(spark, target_path, z_order_cols=["tpep_pickup_datetime"])
    print("[BRONZE] Optimize complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi — Bronze ingestion")
    parser.add_argument("--year",   type=int,  required=True)
    parser.add_argument("--months", type=str,  required=True,
                        help="Comma-separated e.g. 01,02,03")
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
