"""spark/utils/delta_utils.py — DeltaTable helpers: upsert, vacuum, optimize, history."""
from __future__ import annotations
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


def upsert_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: list[str],
    schema_evolution: bool = True,
) -> None:
    """Merge source_df into existing Delta table; creates table on first run."""
    merge_condition = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )
    if DeltaTable.isDeltaTable(spark, target_path):
        if schema_evolution:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        (
            DeltaTable.forPath(spark, target_path).alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            source_df.write.format("delta").mode("overwrite")
            .option("mergeSchema", str(schema_evolution).lower())
            .save(target_path)
        )


def vacuum_delta(spark: SparkSession, path: str, retain_hours: int = 168) -> None:
    """Remove files older than retain_hours (default = 7 days)."""
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    DeltaTable.forPath(spark, path).vacuum(retain_hours)


def optimize_delta(
    spark: SparkSession, path: str, z_order_cols: Optional[list[str]] = None
) -> None:
    """Compact small files; optionally Z-ORDER on given columns."""
    z = f" ZORDER BY ({', '.join(z_order_cols)})" if z_order_cols else ""
    spark.sql(f"OPTIMIZE delta.`{path}`" + z)


def get_delta_history(spark: SparkSession, path: str, limit: int = 10) -> DataFrame:
    """Return the last `limit` Delta transaction-log entries as a DataFrame."""
    return DeltaTable.forPath(spark, path).history(limit)
