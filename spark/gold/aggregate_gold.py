"""
spark/gold/aggregate_gold.py

GOLD LAYER  —  business-level aggregates from Silver Delta tables.

Produces four Gold Delta tables:
  ┌────────────────────────────────────────────────┐
  │ gold_daily_revenue     — KPIs per calendar day            │
  │ gold_hourly_demand     — trip volume + avg fare by hour   │
  │ gold_location_stats    — top pickup / dropoff zones       │
  │ gold_payment_breakdown — revenue split by payment method  │
  └────────────────────────────────────────────────┘

Run:
  spark-submit spark/gold/aggregate_gold.py --year 2024 --months 01,02,03
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import get_spark
from utils.delta_utils import optimize_delta

# ── env ───────────────────────────────────────────────────────────────────
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "nyc-taxi-silver")
GOLD_BUCKET   = os.getenv("GOLD_BUCKET",   "nyc-taxi-gold")
TAXI_TYPE     = os.getenv("TAXI_TYPE",     "yellow")

AUDIT_COL = F.lit(datetime.now(timezone.utc).isoformat()).alias("_gold_computed_at")


# ── aggregation functions ───────────────────────────────────────────────────
def daily_revenue(df: DataFrame) -> DataFrame:
    """Per-day KPIs: trip count, revenue, averages."""
    return (
        df
        .withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
        .groupBy("trip_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("fare_amount"),       2).alias("total_fare"),
            F.round(F.sum("tip_amount"),        2).alias("total_tips"),
            F.round(F.sum("total_charges"),     2).alias("total_revenue"),
            F.round(F.avg("fare_amount"),       2).alias("avg_fare"),
            F.round(F.avg("trip_distance"),     2).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            F.round(F.avg("passenger_count"),   2).alias("avg_passengers"),
            F.round(F.sum("trip_distance"),     2).alias("total_distance_miles"),
        )
        .withColumn("_gold_computed_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))
        .orderBy("trip_date")
    )


def hourly_demand(df: DataFrame) -> DataFrame:
    """Trip volume + avg fare by day-of-week x hour-of-day."""
    return (
        df
        .withColumn("trip_date",   F.to_date("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("day_name",    F.date_format("tpep_pickup_datetime", "EEEE"))
        .groupBy("trip_date", "day_of_week", "day_name", "hour_of_day", "time_of_day_bucket")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"),       2).alias("avg_fare"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            F.round(F.sum("fare_amount"),       2).alias("total_fare"),
        )
        .withColumn("_gold_computed_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))
        .orderBy("trip_date", "hour_of_day")
    )


def location_stats(df: DataFrame) -> DataFrame:
    """Top pickup and dropoff location zones with revenue."""
    pickup  = (
        df.groupBy(F.col("PULocationID").alias("location_id"))
        .agg(
            F.count("*").alias("pickup_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_pickup_fare"),
            F.round(F.sum("fare_amount"), 2).alias("total_pickup_revenue"),
        )
        .withColumn("location_role", F.lit("pickup"))
    )
    dropoff = (
        df.groupBy(F.col("DOLocationID").alias("location_id"))
        .agg(
            F.count("*").alias("dropoff_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_dropoff_fare"),
        )
        .withColumn("location_role", F.lit("dropoff"))
    )
    return (
        pickup.join(dropoff, ["location_id"], "full")
        .withColumn("_gold_computed_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))
    )


def payment_breakdown(df: DataFrame) -> DataFrame:
    """Revenue and trip share by payment method."""
    total_trips = df.count()
    return (
        df
        .groupBy("payment_type", "payment_type_desc")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("fare_amount"),   2).alias("total_fare"),
            F.round(F.sum("tip_amount"),    2).alias("total_tips"),
            F.round(F.sum("total_charges"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"),    2).alias("avg_tip"),
        )
        .withColumn(
            "trip_share_pct",
            F.round(F.col("trip_count") * 100.0 / total_trips, 2),
        )
        .withColumn("_gold_computed_at",
                    F.lit(datetime.now(timezone.utc).isoformat()))
        .orderBy(F.desc("trip_count"))
    )


# ── writer helper ────────────────────────────────────────────────────────────
def write_gold(df: DataFrame, table_name: str) -> str:
    path = f"s3a://{GOLD_BUCKET}/{TAXI_TYPE}/{table_name}/"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    optimize_delta(df.sparkSession, path)
    print(f"[GOLD]  {table_name:30s} -> {df.count():>10,} rows  -> {path}")
    return path


# ── main ───────────────────────────────────────────────────────────────────
def run(year: int, months: list[str]) -> None:
    spark       = get_spark("nyc-taxi-gold-aggregation")
    source_path = f"s3a://{SILVER_BUCKET}/{TAXI_TYPE}/"

    month_filter = F.col("_month").isin(months) & (F.col("_year") == str(year))
    silver_df    = spark.read.format("delta").load(source_path).where(month_filter)
    silver_df.cache()          # reused across all four aggregations

    print(f"[GOLD] Silver rows loaded: {silver_df.count():,}")

    write_gold(daily_revenue(silver_df),    "daily_revenue")
    write_gold(hourly_demand(silver_df),    "hourly_demand")
    write_gold(location_stats(silver_df),   "location_stats")
    write_gold(payment_breakdown(silver_df),"payment_breakdown")

    silver_df.unpersist()
    print("[GOLD] All tables written successfully.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi — Gold aggregation")
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
