from __future__ import annotations
import os
from pyspark.sql import SparkSession

def get_spark(app_name: str, master=None, executor_memory='2g', driver_memory='2g') -> SparkSession:
    minio_endpoint = os.getenv('MINIO_ENDPOINT',       'http://minio:9000')
    aws_key        = os.getenv('AWS_ACCESS_KEY_ID',    'minioadmin')
    aws_secret     = os.getenv('AWS_SECRET_ACCESS_KEY','minioadmin')
    spark_master   = master or os.getenv('SPARK_MASTER','local[*]')
    spark = (
        SparkSession.builder.appName(app_name).master(spark_master)
        .config('spark.sql.extensions',           'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.hadoop.fs.s3a.endpoint',            minio_endpoint)
        .config('spark.hadoop.fs.s3a.access.key',          aws_key)
        .config('spark.hadoop.fs.s3a.secret.key',          aws_secret)
        .config('spark.hadoop.fs.s3a.path.style.access',   'true')
        .config('spark.hadoop.fs.s3a.impl',                'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled','false')
        .config('spark.executor.memory', executor_memory)
        .config('spark.driver.memory',   driver_memory)
        .config('spark.sql.adaptive.enabled',                    'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .config('spark.sql.adaptive.skewJoin.enabled',           'true')
        .config('spark.serializer','org.apache.spark.serializer.KryoSerializer')
        .config('spark.sql.parquet.int96RebaseModeInRead',    'CORRECTED')
        .config('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    return spark
