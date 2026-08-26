"""
databricks/spark/gold/aggregate_gold.py

GOLD LAYER - Databricks Runtime + Azure ADLS Gen2 edition.

Same pattern as ingest_bronze.py / transform_silver.py: all four aggregation
functions (daily_revenue, hourly_demand, location_stats, payment_breakdown)
are byte-for-byte identical to the local version's groupBy/agg logic. Only
the I/O boundary changed:
  - s3a://nyc-taxi-silver|gold/...  ->  abfss://silver|gold@<account>.dfs.core.windows.net/
  - write_gold() OPTIMIZEs via the Unity Catalog table name
    (nyc_taxi.gold.<table>, registered once by resources/setup_unity_catalog.sql -
    this is what lets dbt's {{ source('nyc_taxi_gold', '<table>') }} in
    databricks/dbt/models/staging/*.sql actually resolve), falling back to
    path-based OPTIMIZE if that external table isn't registered yet.

Run on Databricks (spark_python_task in resources/nyc_taxi_pipeline.yml):
  aggregate_gold.py --year 2024 --months 01,02,03
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F

import inspect
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda:0)))

sys.path.insert(0, os.path.abspath(os.path.join(_script_dir, "../..")))
from spark.utils.spark_session import get_spark
from spark.utils.delta_utils import optimize_delta

# -- ADLS Gen2 path builder (matches transform_silver.py's adls() helper) --
STORAGE_ACCOUNT  = os.getenv("ADLS_STORAGE_ACCOUNT", "nyctaxilakehouse1")
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "silver")
GOLD_CONTAINER   = os.getenv("GOLD_CONTAINER",   "gold")
TAXI_TYPE        = os.getenv("TAXI_TYPE", "yellow")

UC_CATALOG     = os.getenv("UC_CATALOG", "nyc_taxi")
UC_GOLD_SCHEMA = os.getenv("UC_GOLD_SCHEMA", "gold")


def adls(container: str, path: str = "") -> str:
    """Build an abfss:// URI for Azure Data Lake Storage Gen2."""
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


# -- aggregation functions (identical to local version) --------------------
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
        .orderBy(F.desc("trip_count"))
    )


# -- writer helper -----------------------------------------------------
def write_gold(df: DataFrame, table_name: str) -> str:
    path = adls(GOLD_CONTAINER, f"{TAXI_TYPE}/{table_name}/")
    df = df.drop("_gold_computed_at").withColumn("_gold_computed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    print(f"DEBUG: Schema for {table_name}: {df.columns}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

    # Register in Unity Catalog so dbt can query it
    df.sparkSession.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
    df.sparkSession.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_GOLD_SCHEMA}")
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_GOLD_SCHEMA}.{table_name} USING DELTA LOCATION '{path}'")

    # OPTIMIZE via Unity Catalog table name first (nyc_taxi.gold.<table_name> -
    # path-based OPTIMIZE if that external table hasn't been registered yet.
    try:
        df.sparkSession.sql(f"OPTIMIZE {UC_CATALOG}.{UC_GOLD_SCHEMA}.{table_name}")
    except Exception as e:
        optimize_delta(df.sparkSession, path)
        print(f"[GOLD]  {table_name:30s} OPTIMIZE path fallback: {e}")

    print(f"[GOLD]  {table_name:30s} -> {df.count():>10,} rows  -> {path}")
    return path


# -- main ----------------------------------------------------------------
def run(year: int, months: list[str]) -> None:
    spark       = get_spark("nyc-taxi-gold-aggregation")
    source_path = adls(SILVER_CONTAINER, f"{TAXI_TYPE}/")

    month_filter = F.col("_month").isin(months) & (F.col("_year") == str(year))
    silver_df    = spark.read.format("delta").load(source_path).where(month_filter)
    # silver_df.cache()  # Removed: NOT_SUPPORTED_WITH_SERVERLESS

    print(f"[GOLD] Silver rows loaded: {silver_df.count():,}")

    write_gold(daily_revenue(silver_df),    "daily_revenue")
    write_gold(hourly_demand(silver_df),    "hourly_demand")
    write_gold(location_stats(silver_df),   "location_stats")
    write_gold(payment_breakdown(silver_df),"payment_breakdown")

    # silver_df.unpersist()  # Removed: NOT_SUPPORTED_WITH_SERVERLESS
    print("[GOLD] All tables written successfully.")
    # spark.stop() deliberately removed - cluster owns the session


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi - Gold aggregation (Databricks)")
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.months.split(",")]
    run(args.year, months)
