"""
databricks/spark/silver/transform_silver.py

SILVER transform — Databricks Runtime + Azure ADLS Gen2 edition.

Changes vs local spark/silver/transform_silver.py
  1. get_spark()    →  Databricks shim (cluster session, no builder)
  2. s3a://         →  abfss://container@account.dfs.core.windows.net/
  3. spark.stop()   →  REMOVED
  4. Path env vars  →  ADLS_STORAGE_ACCOUNT + container names

All transformation logic (clean, enrich, business rules, dedup window)
is byte-for-byte identical to the local version.
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from spark.utils.spark_session import get_spark
from spark.utils.delta_utils import upsert_delta, optimize_delta

# ── ADLS Gen2 path builder ──────────────────────────────────────────────────
STORAGE_ACCOUNT  = os.getenv("ADLS_STORAGE_ACCOUNT", "nyctaxilakehouse")
BRONZE_CONTAINER = os.getenv("BRONZE_CONTAINER", "bronze")
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "silver")
TAXI_TYPE        = os.getenv("TAXI_TYPE", "yellow")
UC_CATALOG       = os.getenv("UC_CATALOG", "nyc_taxi")
UC_SILVER_SCHEMA = os.getenv("UC_SILVER_SCHEMA", "silver")


def adls(container: str, path: str = "") -> str:
    """Build an abfss:// URI for Azure Data Lake Storage Gen2."""
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


# ── validation rules (unchanged) ─────────────────────────────────────────────────
DROP_NULL_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "VendorID",
]
BUSINESS_RULES = (
    (F.col("trip_distance")  > 0)  &
    (F.col("fare_amount")    > 0)  &
    (F.col("passenger_count").between(1, 8)) &
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
    (F.col("trip_distance")  < 500) &
    (F.col("fare_amount")    < 2000) &
    (F.col("RatecodeID").isin([1, 2, 3, 4, 5, 6]))
)
PAYMENT_LABELS = {
    1: "Credit card", 2: "Cash",    3: "No charge",
    4: "Dispute",    5: "Unknown", 6: "Voided trip",
}


def clean(df: DataFrame) -> DataFrame:
    for col in DROP_NULL_COLS:
        df = df.where(F.col(col).isNotNull())
    df = df.where(BUSINESS_RULES)
    dedup_window = Window.partitionBy(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"
    ).orderBy("_ingested_at")
    return (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .where(F.col("_row_num") == 1)
          .drop("_row_num")
    )


def enrich(df: DataFrame) -> DataFrame:
    payment_map = F.create_map(
        *[item for pair in
          [(F.lit(k), F.lit(v)) for k, v in PAYMENT_LABELS.items()]
          for item in pair]
    )
    return (
        df
        .withColumn("trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
        .withColumn("speed_mph",
            F.round(F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0), 2))
        .withColumn("fare_per_mile",
            F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
        .withColumn("total_charges",
            F.col("fare_amount") +
            F.coalesce(F.col("tip_amount"),             F.lit(0)) +
            F.coalesce(F.col("tolls_amount"),           F.lit(0)) +
            F.coalesce(F.col("improvement_surcharge"),  F.lit(0)) +
            F.coalesce(F.col("congestion_surcharge"),   F.lit(0)))
        .withColumn("payment_type_desc", payment_map[F.col("payment_type")])
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .withColumn("time_of_day_bucket",
            F.when(F.col("hour_of_day").between(6,  11), "Morning")
             .when(F.col("hour_of_day").between(12, 16), "Afternoon")
             .when(F.col("hour_of_day").between(17, 20), "Evening")
             .when(F.col("hour_of_day").between(21, 23), "Night")
             .otherwise("Late Night"))
        .withColumn("_silver_processed_at",
            F.lit(datetime.now(timezone.utc).isoformat()))
    )


def run(year: int, months: list[str]) -> None:
    spark = get_spark("nyc-taxi-silver-transform")

    # DATABRICKS: abfss:// paths for ADLS Gen2
    source_path = adls(BRONZE_CONTAINER, f"{TAXI_TYPE}/")
    target_path = adls(SILVER_CONTAINER, f"{TAXI_TYPE}/")

    month_filter = F.col("_month").isin(months) & (F.col("_year") == str(year))
    bronze_df    = spark.read.format("delta").load(source_path).where(month_filter)
    print(f"[SILVER] Bronze rows read   : {bronze_df.count():,}")

    silver_df = enrich(clean(bronze_df))
    print(f"[SILVER] Silver rows written: {silver_df.count():,}")

    upsert_delta(spark, silver_df, target_path,
                 merge_keys=["VendorID", "tpep_pickup_datetime", "_source_file"])

    try:
        spark.sql(
            f"OPTIMIZE {UC_CATALOG}.{UC_SILVER_SCHEMA}.{TAXI_TYPE}_trips "
            f"ZORDER BY (tpep_pickup_datetime, PULocationID)"
        )
    except Exception as e:
        optimize_delta(spark, target_path,
                       z_order_cols=["tpep_pickup_datetime", "PULocationID"])
        print(f"[SILVER] OPTIMIZE path fallback: {e}")

    print(f"[SILVER] Written + optimized: {target_path}")
    # DATABRICKS: spark.stop() removed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi — Silver (Databricks)")
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
