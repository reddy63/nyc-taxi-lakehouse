"""
databricks/spark/utils/delta_utils.py

Delta helpers, migrated from spark/utils/delta_utils.py.

WHAT ACTUALLY CHANGED (this is one of the few real code differences in the
whole migration, so it's worth calling out): the local version only ever
takes a filesystem `target_path` (an s3a:// URL) and always calls
DeltaTable.forPath()/.save(path). On Databricks, Bronze/Silver/Gold become
Unity Catalog managed tables (three-part names like nyc_taxi.gold.daily_revenue -
see sources.yml, which dbt's staging models query as {{ source(...) }}), not
raw paths. So every function here now accepts EITHER a path (contains "://"
or starts with "/") OR a catalog.schema.table name, and dispatches to
DeltaTable.forName()/.saveAsTable() or DeltaTable.forPath()/.save()
accordingly. Call sites (ingest_bronze.py, transform_silver.py,
aggregate_gold.py, vacuum_all.py) don't need to know or care which kind of
target they're holding - same as locally, they just pass a string.
"""
from __future__ import annotations
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


def _is_table_name(target: str) -> bool:
    """True for 'catalog.schema.table', False for a path ('/Volumes/...',
    'abfss://...', 's3a://...', etc)."""
    return "://" not in target and not target.startswith("/") and target.count(".") == 2


def _table_exists(spark: SparkSession, target: str) -> bool:
    if _is_table_name(target):
        return spark.catalog.tableExists(target)
    return DeltaTable.isDeltaTable(spark, target)


def _delta_table(spark: SparkSession, target: str) -> DeltaTable:
    if _is_table_name(target):
        return DeltaTable.forName(spark, target)
    return DeltaTable.forPath(spark, target)


def upsert_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target: str,
    merge_keys: list[str],
    schema_evolution: bool = True,
) -> None:
    """Merge source_df into an existing Delta table/path; creates it on first run.
    `target` may be a catalog.schema.table name or a filesystem path."""
    merge_condition = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )
    if _table_exists(spark, target):
        if schema_evolution:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        (
            _delta_table(spark, target).alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = (
            source_df.write.format("delta").mode("overwrite")
            .option("mergeSchema", str(schema_evolution).lower())
        )
        if _is_table_name(target):
            writer.saveAsTable(target)
        else:
            writer.save(target)


def vacuum_delta(spark: SparkSession, target: str, retain_hours: int = 168) -> None:
    """Remove files older than retain_hours (default = 7 days)."""
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    _delta_table(spark, target).vacuum(retain_hours)


def optimize_delta(
    spark: SparkSession, target: str, z_order_cols: Optional[list[str]] = None
) -> None:
    """Compact small files; optionally Z-ORDER on given columns."""
    z = f" ZORDER BY ({', '.join(z_order_cols)})" if z_order_cols else ""
    ref = target if _is_table_name(target) else f"delta.`{target}`"
    spark.sql(f"OPTIMIZE {ref}" + z)


def get_delta_history(spark: SparkSession, target: str, limit: int = 10) -> DataFrame:
    """Return the last `limit` Delta transaction-log entries as a DataFrame."""
    return _delta_table(spark, target).history(limit)
