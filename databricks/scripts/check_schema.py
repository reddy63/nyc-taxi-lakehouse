import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from databricks.spark.utils.spark_session import get_spark
spark = get_spark("test")
source_path = "abfss://silver@nyctaxilakehouse1.dfs.core.windows.net/yellow/"
df = spark.read.format("delta").load(source_path)
df.printSchema()
