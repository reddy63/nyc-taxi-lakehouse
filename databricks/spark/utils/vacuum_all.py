"""
databricks/spark/utils/vacuum_all.py
Vacuum all Delta layers on ADLS Gen2. No spark.stop().
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from spark.utils.spark_session import get_spark
from spark.utils.delta_utils import vacuum_delta

STORAGE_ACCOUNT  = os.getenv("ADLS_STORAGE_ACCOUNT", "nyctaxilakehouse")
BRONZE_CONTAINER = os.getenv("BRONZE_CONTAINER", "bronze")
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "silver")
GOLD_CONTAINER   = os.getenv("GOLD_CONTAINER",  "gold")
TAXI_TYPE        = os.getenv("TAXI_TYPE",        "yellow")
RETAIN_HOURS     = int(os.getenv("VACUUM_RETAIN_HOURS", "168"))


def adls(container: str, path: str = "") -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def main() -> None:
    spark = get_spark("nyc-taxi-delta-vacuum")
    paths = [
        adls(BRONZE_CONTAINER, f"{TAXI_TYPE}/"),
        adls(SILVER_CONTAINER, f"{TAXI_TYPE}/"),
        adls(GOLD_CONTAINER,   f"{TAXI_TYPE}/daily_revenue/"),
        adls(GOLD_CONTAINER,   f"{TAXI_TYPE}/hourly_demand/"),
        adls(GOLD_CONTAINER,   f"{TAXI_TYPE}/location_stats/"),
        adls(GOLD_CONTAINER,   f"{TAXI_TYPE}/payment_breakdown/"),
    ]
    for path in paths:
        print(f"[VACUUM] {path}  (retain={RETAIN_HOURS}h)")
        try:
            vacuum_delta(spark, path, RETAIN_HOURS)
            print("[VACUUM] OK")
        except Exception as e:
            print(f"[VACUUM] WARN: {e}")
    # DATABRICKS: spark.stop() removed


if __name__ == "__main__":
    main()
