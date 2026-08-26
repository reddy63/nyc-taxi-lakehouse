import argparse
import os
import requests
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Download NYC Taxi Data")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--months", type=str, required=True)
    args = parser.parse_args()

    # Get Spark session and dbutils
    spark = SparkSession.builder.getOrCreate()
    
    # dbutils is available globally in Databricks notebooks/jobs
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except Exception:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]

    storage_account = os.getenv("ADLS_STORAGE_ACCOUNT", "nyctaxilakehouse")
    landing_container = os.getenv("LANDING_CONTAINER", "landing")
    taxi_type = os.getenv("TAXI_TYPE", "yellow")

    months = args.months.split(",")
    tlc_base = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    for raw_month in months:
        month = f"{int(raw_month):02d}"
        filename = f"{taxi_type}_tripdata_{args.year}-{month}.parquet"
        url = f"{tlc_base}/{filename}"
        
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, filename)
            adls_path = f"abfss://{landing_container}@{storage_account}.dfs.core.windows.net/{taxi_type}/{args.year}/{month}/{filename}"

            print(f"Downloading {url} to {local_path}...")
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded. Uploading to {adls_path} via dbutils...")
                # Use dbutils to copy from local driver to ADLS
                dbutils.fs.cp(f"file:{local_path}", adls_path)
                print(f"Successfully uploaded to ADLS.")
            else:
                print(f"Failed to download {url}. Status code: {response.status_code}")

if __name__ == "__main__":
    main()
