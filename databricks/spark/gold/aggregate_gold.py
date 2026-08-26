"""
databricks/spark/gold/aggregate_gold.py

GOLD aggregation — Databricks Runtime + Azure ADLS Gen2 edition.

Changes vs local spark/gold/aggregate_gold.py
  1. get_spark()   →  Databricks shim
  2. s3a://        →  abfss://  (ADLS Gen2)
  3. spark.stop()  →  REMOVED
  4. write_gold()  →  registers Unity Catalog external table after write
                       so dbt and SQL Editor can query it by name.

All four aggregation functions (daily_revenue, hourly_demand,
location_stats, payment_breakdown) are byte-for-byte identical.
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from spark.utils.spark_session import get_spark
from spark.utils.delta_utils import optimize_delta

# ── env ───────────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = os.getenv("ADLS_STORAGE_ACCOUNT", "nyctaxilakehouse")
SILVER_CONTAINER= os.getenv("SILVER_CONTAINER", "silver")
GOLD_CONTAINER  = os.getenv("GOLD_CONTAINER",  "gold")
TAXI_TYPE       = os.getenv("TAXI_TYPE",        "yellow")
UC_CATALOG      = os.getenv("UC_CATALOG",       "nyc_taxi")
UC_GOLD_SCHEMA  = os.getenv("UC_GOLD_SCHEMA",   "gold")


def adls(container: str, path: str = "") -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


# ── aggregations (identical to local version) ───────────────────────────────────────
def daily_revenue(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
        .groupBy("trip_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("fare_amount"),          2).alias("total_fare"),
            F.round(F.sum("tip_amount"),           2).alias("total_tips"),
            F.round(F.sum("total_charges"),        2).alias("total_revenue"),
            F.round(F.avg("fare_amount"),          2).alias("avg_fare"),
            F.round(F.avg("trip_distance"),        2).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_minutes"),2).alias("avg_duration_min"),
            F.round(F.avg("passenger_count"),      2).alias("avg_passengers"),
            F.round(F.sum("trip_distance"),        2).alias("total_distance_miles"),
        )
        .withColumn("_gold_computed_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .orderBy("trip_date")
    )


def hourly_demand(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("trip_date",   F.to_date("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("day_name",    F.date_format("tpep_pickup_datetime", "EEEE"))
        .groupBy("trip_date", "day_of_week", "day_name", "hour_of_day", "time_of_day_bucket")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"),          2).alias("avg_fare"),
            F.round(F.avg("trip_duration_minutes"),2).alias("avg_duration_min"),
            F.round(F.sum("fare_amount"),          2).alias("total_fare"),
        )
        .withColumn("_gold_computed_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .orderBy("trip_date", "hour_of_day")
    )


def location_stats(df: DataFrame) -> DataFrame:
    pickup = (
        df.groupBy(F.col("PULocationID").alias("location_id"))
        .agg(
            F.count("*").alias("pickup_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_pickup_fare"),
            F.round(F.sum("fare_amount"), 2).alias("total_pickup_revenue"),
        )
    )
    dropoff = (
        df.groupBy(F.col("DOLocationID").alias("location_id"))
        .agg(
            F.count("*").alias("dropoff_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_dropoff_fare"),
        )
    )
    return (
        pickup.join(dropoff, ["location_id"], "full")
        .withColumn("_gold_computed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )


def payment_breakdown(df: DataFrame) -> DataFrame:
    total_trips = df.count()
    return (
        df.groupBy("payment_type", "payment_type_desc")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("fare_amount"),   2).alias("total_fare"),
            F.round(F.sum("tip_amount"),    2).alias("total_tips"),
            F.round(F.sum("total_charges"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"),    2).alias("avg_tip"),
        )
        .withColumn("trip_share_pct",
            F.round(F.col("trip_count") * 100.0 / total_trips, 2))
        .withColumn("_gold_computed_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .orderBy(F.desc("trip_count"))
    )


# ── writer: Delta write + Unity Catalog table registration ─────────────────────────
def write_gold(df: DataFrame, table_name: str, spark) -> None:
    # DATABRICKS: abfss:// path
    path = adls(GOLD_CONTAINER, f"{TAXI_TYPE}/{table_name}/")

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true").save(path)

    optimize_delta(spark, path)
    row_count = df.count()
    print(f"[GOLD]  {table_name:28s} -> {row_count:>10,} rows  -> {path}")

    # DATABRICKS: Register as Unity Catalog external table so SQL Editor
    # and dbt can query it by name (nyc_taxi.gold.<table_name>).
    # This is idempotent — safe to run every pipeline cycle.
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_GOLD_SCHEMA}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_GOLD_SCHEMA}.{table_name}
            USING DELTA
            LOCATION '{path}'
        """)
        print(f"[GOLD]  UC table registered: {UC_CATALOG}.{UC_GOLD_SCHEMA}.{table_name}")
    except Exception as e:
        print(f"[GOLD]  UC registration skipped (no UC metastore?): {e}")


# ── main ───────────────────────────────────────────────────────────────────
def run(year: int, months: list[str]) -> None:
    spark = get_spark("nyc-taxi-gold-aggregation")

    source_path  = adls(SILVER_CONTAINER, f"{TAXI_TYPE}/")
    month_filter = F.col("_month").isin(months) & (F.col("_year") == str(year))
    silver_df    = spark.read.format("delta").load(source_path).where(month_filter)
    silver_df.cache()

    print(f"[GOLD] Silver rows loaded: {silver_df.count():,}")

    write_gold(daily_revenue(silver_df),     "daily_revenue",     spark)
    write_gold(hourly_demand(silver_df),     "hourly_demand",     spark)
    write_gold(location_stats(silver_df),    "location_stats",    spark)
    write_gold(payment_breakdown(silver_df), "payment_breakdown", spark)

    silver_df.unpersist()
    print("[GOLD] All tables written and registered.")
    # DATABRICKS: spark.stop() removed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi — Gold (Databricks)")
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
