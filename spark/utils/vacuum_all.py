"""
spark/utils/vacuum_all.py
Vacuum all three Delta layers (Bronze, Silver, Gold) in one Spark job.
Run at the end of each pipeline cycle to reclaim S3 storage.
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import get_spark
from utils.delta_utils import vacuum_delta

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "nyc-taxi-bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "nyc-taxi-silver")
GOLD_BUCKET   = os.getenv("GOLD_BUCKET",   "nyc-taxi-gold")
TAXI_TYPE     = os.getenv("TAXI_TYPE",     "yellow")
RETAIN_HOURS  = int(os.getenv("VACUUM_RETAIN_HOURS", "168"))  # 7 days


def main() -> None:
    spark = get_spark("nyc-taxi-delta-vacuum")
    paths = [
        f"s3a://{BRONZE_BUCKET}/{TAXI_TYPE}/",
        f"s3a://{SILVER_BUCKET}/{TAXI_TYPE}/",
        f"s3a://{GOLD_BUCKET}/{TAXI_TYPE}/daily_revenue/",
        f"s3a://{GOLD_BUCKET}/{TAXI_TYPE}/hourly_demand/",
        f"s3a://{GOLD_BUCKET}/{TAXI_TYPE}/location_stats/",
        f"s3a://{GOLD_BUCKET}/{TAXI_TYPE}/payment_breakdown/",
    ]
    for path in paths:
        print(f"[VACUUM] {path}  (retain={RETAIN_HOURS}h)")
        try:
            vacuum_delta(spark, path, RETAIN_HOURS)
            print(f"[VACUUM] OK")
        except Exception as e:
            print(f"[VACUUM] WARN: {e}")
    spark.stop()


if __name__ == "__main__":
    main()
