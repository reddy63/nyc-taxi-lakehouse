"""
databricks/spark/utils/spark_session.py

Databricks Runtime shim for get_spark().

On Databricks the cluster already provides a fully configured SparkSession
as the `spark` builtin. Delta Lake extensions, Unity Catalog, and the
S3 credential pass-through via the cluster's IAM instance profile are all
wired automatically — no manual .config() calls are needed.

This module exposes the same get_spark() signature as the local version
so every bronze / silver / gold job imports identically; only the
databricks/ copy is used when the wheel is deployed to a Databricks job.

What changes versus the local spark_session.py
  LOCAL                          DATABRICKS
  ─────────────────────────────  ──────────────────────────────────────
  SparkSession.builder.config()  spark  (cluster builtin, no builder)
  s3a://  (Hadoop S3A)           s3://  (DBIO native driver, IAM auth)
  spark.stop()  in main()        REMOVED  (cluster manages lifecycle)
  delta-spark pip package        DBR ships Delta 3.x natively
  Manual executor/driver memory  Cluster policy controls memory
"""
from __future__ import annotations
from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "nyc-taxi-lakehouse",
    master: str | None = None,          # ignored on Databricks
    executor_memory: str = "2g",        # ignored on Databricks
    driver_memory:   str = "2g",        # ignored on Databricks
) -> SparkSession:
    """
    Return the active Databricks SparkSession.

    All keyword args are accepted (so call-sites need no changes) but are
    silently ignored — the cluster configuration controls resource allocation.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        # Fallback: create a minimal session (useful for local unit-test runs
        # that import from the databricks/ subtree directly).
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass
    return spark
