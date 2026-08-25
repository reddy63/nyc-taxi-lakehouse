"""
dags/nyc_taxi_pipeline_dag.py

NYC Taxi Medallion Pipeline DAG
================================
Schedule : monthly (1st of each month, 06:00 IST)
Owner    : arun-reddy

Task graph:
  download_data
      │
   upload_to_landing
      │
   bronze_ingestion      ── SparkSubmitOperator
      │
   silver_transform      ── SparkSubmitOperator
      │
   gold_aggregation      ── SparkSubmitOperator
      │
   dbt_run               ── BashOperator
      │
   dbt_test              ── BashOperator
      │
   delta_vacuum          ── SparkSubmitOperator (cleanup)
"""
from __future__ import annotations
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── constants ────────────────────────────────────────────────────────────────
SPARK_CONN_ID   = "spark_default"
SPARK_MASTER    = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_KEY         = os.getenv("AWS_ACCESS_KEY_ID",     "minioadmin")
AWS_SECRET      = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
LANDING_BUCKET  = os.getenv("LANDING_BUCKET", "nyc-taxi-landing")
TAXI_TYPE       = os.getenv("TAXI_TYPE", "yellow")
JOBS_DIR        = "/opt/airflow/spark"

# Delta + S3A JARs. SparkSubmitOperator runs spark-submit locally inside the Airflow
# container (client deploy mode), so these must be paths that exist IN THAT CONTAINER
# (baked in by docker/airflow/Dockerfile), not the Spark cluster image's /opt/bitnami path.
DELTA_JARS = ",".join([
    "/opt/airflow/spark_jars/delta-spark_2.12-3.2.0.jar",
    "/opt/airflow/spark_jars/delta-storage-3.2.0.jar",
    "/opt/airflow/spark_jars/hadoop-aws-3.3.4.jar",
    "/opt/airflow/spark_jars/aws-java-sdk-bundle-1.12.262.jar",
])

SPARK_CONF = {
    "spark.sql.extensions":                    "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint":            MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":          AWS_KEY,
    "spark.hadoop.fs.s3a.secret.key":          AWS_SECRET,
    "spark.hadoop.fs.s3a.path.style.access":   "true",
    "spark.hadoop.fs.s3a.impl":                "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.adaptive.enabled":              "true",
}

# ── default args ───────────────────────────────────────────────────────────
default_args = {
    "owner":            "arun-reddy",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ── helper ───────────────────────────────────────────────────────────────────
def make_spark_submit(task_id: str, script: str, args_list: list[str]) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id          = task_id,
        conn_id          = SPARK_CONN_ID,
        application      = f"{JOBS_DIR}/{script}",
        application_args = args_list,
        conf             = SPARK_CONF,
        jars             = DELTA_JARS,
        driver_memory    = "2g",
        executor_memory  = "2g",
        num_executors    = 1,
        verbose          = True,
    )


# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id           = "nyc_taxi_medallion_pipeline",
    description      = "NYC Taxi: Landing → Bronze → Silver → Gold → dbt Marts",
    schedule_interval= "0 1 1 * *",       # monthly, 1st at 01:00 UTC
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    default_args     = default_args,
    tags             = ["nyc-taxi", "medallion", "delta-lake", "portfolio"],
    max_active_runs  = 1,
) as dag:

    # 1. Download latest TLC Parquet from TLC website
    download_data = BashOperator(
        task_id  = "download_data",
        bash_command = (
            "bash /opt/airflow/scripts/download_data.sh "
            "{{ data_interval_start.strftime('%Y') }} "
            "{{ data_interval_start.strftime('%m') }}"
        ),
    )

    # 2. Upload to MinIO landing zone
    upload_landing = BashOperator(
        task_id  = "upload_to_landing",
        bash_command = (
            "python /opt/airflow/scripts/upload_to_minio.py "
            "--year {{ data_interval_start.strftime('%Y') }} "
            "--month {{ data_interval_start.strftime('%m') }}"
        ),
    )

    # 3. Bronze ingestion (PySpark + Delta)
    bronze_ingest = make_spark_submit(
        task_id  = "bronze_ingestion",
        script   = "bronze/ingest_bronze.py",
        args_list= [
            "--year",   "{{ data_interval_start.strftime('%Y') }}",
            "--months", "{{ data_interval_start.strftime('%m') }}",
        ],
    )

    # 4. Silver transformation (clean + enrich)
    silver_transform = make_spark_submit(
        task_id  = "silver_transform",
        script   = "silver/transform_silver.py",
        args_list= [
            "--year",   "{{ data_interval_start.strftime('%Y') }}",
            "--months", "{{ data_interval_start.strftime('%m') }}",
        ],
    )

    # 5. Gold aggregation (business KPIs)
    gold_aggregate = make_spark_submit(
        task_id  = "gold_aggregation",
        script   = "gold/aggregate_gold.py",
        args_list= [
            "--year",   "{{ data_interval_start.strftime('%Y') }}",
            "--months", "{{ data_interval_start.strftime('%m') }}",
        ],
    )

    # 6. dbt run — build analytics marts
    dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = "cd /opt/airflow/dbt && dbt run --profiles-dir . --target docker",
    )

    # 7. dbt test — validate marts
    dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = "cd /opt/airflow/dbt && dbt test --profiles-dir . --target docker",
    )

    # 8. Delta vacuum — clean up old files
    delta_vacuum = make_spark_submit(
        task_id  = "delta_vacuum",
        script   = "utils/vacuum_all.py",
        args_list= [],
    )

    # ── dependencies ───────────────────────────────────────────────────────────
    (
        download_data
        >> upload_landing
        >> bronze_ingest
        >> silver_transform
        >> gold_aggregate
        >> dbt_run
        >> dbt_test
        >> delta_vacuum
    )
