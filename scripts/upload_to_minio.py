"""
scripts/upload_to_minio.py
Upload locally downloaded NYC TLC Parquet files to the MinIO landing zone.

Usage:
  python scripts/upload_to_minio.py --year 2024 --month 01
  python scripts/upload_to_minio.py --year 2024 --month 01,02,03
"""
from __future__ import annotations
import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.client import Config

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",       "http://localhost:9000")
AWS_KEY         = os.getenv("AWS_ACCESS_KEY_ID",     "minioadmin")
AWS_SECRET      = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
LANDING_BUCKET  = os.getenv("LANDING_BUCKET",        "nyc-taxi-landing")
TAXI_TYPE       = os.getenv("TAXI_TYPE",              "yellow")
DOWNLOAD_DIR    = os.getenv("DOWNLOAD_DIR",           "/tmp/nyc_taxi_raw")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url         = MINIO_ENDPOINT,
        aws_access_key_id    = AWS_KEY,
        aws_secret_access_key= AWS_SECRET,
        config               = Config(signature_version="s3v4"),
        region_name          = "us-east-1",
    )


def upload_month(s3, year: int, month: str) -> None:
    filename  = f"{TAXI_TYPE}_tripdata_{year}-{month}.parquet"
    local     = Path(DOWNLOAD_DIR) / filename
    s3_key    = f"{TAXI_TYPE}/{year}/{month}/{filename}"

    if not local.exists():
        print(f"[SKIP]  Not found locally: {local}")
        return

    file_mb = local.stat().st_size / 1_048_576
    print(f"[UP]    {filename}  ({file_mb:.1f} MB)  ->  s3://{LANDING_BUCKET}/{s3_key}")
    s3.upload_file(
        str(local),
        LANDING_BUCKET,
        s3_key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )
    print(f"[OK]    Uploaded {filename}")


def main():
    parser = argparse.ArgumentParser(description="Upload TLC Parquet to MinIO")
    parser.add_argument("--year",  type=int, required=True)
    parser.add_argument("--month", type=str, required=True,
                        help="Single or comma-separated, e.g. 01 or 01,02,03")
    args   = parser.parse_args()
    months = [m.strip().zfill(2) for m in args.month.split(",")]

    s3 = get_s3_client()
    for month in months:
        try:
            upload_month(s3, args.year, month)
        except Exception as exc:
            print(f"[ERR]   {exc}", file=sys.stderr)

    print("[DONE]  Upload complete.")


if __name__ == "__main__":
    main()
