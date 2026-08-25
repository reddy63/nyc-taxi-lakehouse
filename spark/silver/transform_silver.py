"""
spark/silver/transform_silver.py

SILVER LAYER  —  clean, validate, deduplicate, and enrich Bronze data.

Transformations applied:
  ─ Drop rows with NULL trip_distance, fare_amount, or pickup/dropoff times
  ─ Filter out physically impossible values (negative fares, zero distance, etc.)
  ─ Deduplicate within the batch on natural key
  ─ Cast and normalise column types
  ─ Derive computed columns (trip_duration_minutes, speed_mph, fare_per_mile)
  ─ Standardise payment_type to a human-readable label
  ─ Write partitioned Delta table to Silver bucket

Run:
  spark-submit spark/silver/transform_silver.py --year 2024 --months 01,02,03
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import get_spark
from utils.delta_utils import upsert_delta, optimize_delta

# ── env ───────────────────────────────────────────────────────────────────
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "nyc-taxi-bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "nyc-taxi-silver")
TAXI_TYPE     = os.getenv("TAXI_TYPE",     "yellow")

# Payment type lookup (TLC data dictionary)
PAYMENT_LABELS = {
    1: "Credit card", 2: "Cash", 3: "No charge",
    4: "Dispute",     5: "Unknown", 6: "Voided trip",
}


# ── validation ─────────────────────────────────────────────────────────────
DROP_NULL_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "VendorID",
]

BUSINESS_RULES = (
    (F.col("trip_distance")  > 0)         &
    (F.col("fare_amount")    > 0)         &
    (F.col("passenger_count") >= 1)       &
    (F.col("passenger_count") <= 8)       &
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
    (F.col("trip_distance")  < 500)       &   # NYC → no trip > 500 mi
    (F.col("fare_amount")    < 2000)      &   # hard cap
    (F.col("RatecodeID").isin([1, 2, 3, 4, 5, 6]))
)


def clean(df: DataFrame) -> DataFrame:
    # 1. Drop mandatory-null rows
    for col in DROP_NULL_COLS:
        df = df.where(F.col(col).isNotNull())

    # 2. Apply business-rule filters
    df = df.where(BUSINESS_RULES)

    # 3. Deduplicate (keep first occurrence within pickup second)
    dedup_window = Window.partitionBy(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"
    ).orderBy("_ingested_at")
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .where(F.col("_row_num") == 1)
          .drop("_row_num")
    )
    return df


def enrich(df: DataFrame) -> DataFrame:
    payment_map = F.create_map(
        *[item for pair in
          [(F.lit(k), F.lit(v)) for k, v in PAYMENT_LABELS.items()]
          for item in pair]
    )
    return (
        df
        # Derived metrics
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
        )
        .withColumn(
            "speed_mph",
            F.round(
                F.col("trip_distance") /
                (F.col("trip_duration_minutes") / 60.0),
                2,
            ),
        )
        .withColumn(
            "fare_per_mile",
            F.round(F.col("fare_amount") / F.col("trip_distance"), 2),
        )
        .withColumn(
            "total_charges",
            F.col("fare_amount") + F.coalesce(F.col("tip_amount"), F.lit(0)) +
            F.coalesce(F.col("tolls_amount"), F.lit(0)) +
            F.coalesce(F.col("improvement_surcharge"), F.lit(0)) +
            F.coalesce(F.col("congestion_surcharge"), F.lit(0)),
        )
        # Readable payment label
        .withColumn("payment_type_desc", payment_map[F.col("payment_type")])
        # Time-of-day bucket
        .withColumn(
            "hour_of_day", F.hour("tpep_pickup_datetime")
        )
        .withColumn(
            "time_of_day_bucket",
            F.when(F.col("hour_of_day").between(6,  11), "Morning")
             .when(F.col("hour_of_day").between(12, 16), "Afternoon")
             .when(F.col("hour_of_day").between(17, 20), "Evening")
             .when(F.col("hour_of_day").between(21, 23), "Night")
             .otherwise("Late Night"),
        )
        # Silver audit
        .withColumn("_silver_processed_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))
    )


def run(year: int, months: list[str]) -> None:
    spark        = get_spark("nyc-taxi-silver-transform")
    source_path  = f"s3a://{BRONZE_BUCKET}/{TAXI_TYPE}/"
    target_path  = f"s3a://{SILVER_BUCKET}/{TAXI_TYPE}/"

    # Predicate pushdown: only read relevant partitions
    month_filter = F.col("_month").isin(months) & (F.col("_year") == str(year))

    bronze_df = spark.read.format("delta").load(source_path).where(month_filter)
    print(f"[SILVER] Bronze rows read   : {bronze_df.count():,}")

    silver_df = enrich(clean(bronze_df))
    print(f"[SILVER] Silver rows written: {silver_df.count():,}")

    upsert_delta(
        spark, silver_df, target_path,
        merge_keys=["VendorID", "tpep_pickup_datetime", "_source_file"],
    )
    optimize_delta(spark, target_path,
                   z_order_cols=["tpep_pickup_datetime", "PULocationID"])
    print(f"[SILVER] Written + optimized: {target_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi — Silver transform")
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
