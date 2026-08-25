import os, sys, pytest
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

@pytest.fixture(scope='module')
def spark_local():
    pytest.importorskip('pyspark')
    pytest.importorskip('delta')
    from spark.utils.spark_session import get_spark
    spark = get_spark('test-silver', master='local[1]', executor_memory='512m', driver_memory='512m')
    yield spark
    spark.stop()

def _make_df(spark, rows):
    schema = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime',
              'trip_distance','fare_amount','passenger_count','RatecodeID',
              '_month','_year','_ingested_at','_source_file','_batch_id','_taxi_type']
    return spark.createDataFrame(rows, schema)

def test_silver_drops_null_fare(spark_local):
    from spark.silver.transform_silver import clean
    rows = [
        (1, datetime(2024,1,10,8,0), datetime(2024,1,10,8,20), 2.5, None, 2, 1,'01','2024','ts','f','b','yellow'),
        (1, datetime(2024,1,10,9,0), datetime(2024,1,10,9,20), 2.5, 10.0, 2, 1,'01','2024','ts','f','b','yellow'),
    ]
    df     = _make_df(spark_local, rows)
    result = clean(df)
    assert result.count() == 1

def test_silver_drops_negative_distance(spark_local):
    from spark.silver.transform_silver import clean
    rows = [
        (1, datetime(2024,1,10,8,0), datetime(2024,1,10,8,20), -1.0, 10.0, 2, 1,'01','2024','ts','f','b','yellow'),
        (1, datetime(2024,1,10,9,0), datetime(2024,1,10,9,20),  3.0, 10.0, 2, 1,'01','2024','ts','f','b','yellow'),
    ]
    df     = _make_df(spark_local, rows)
    result = clean(df)
    assert result.count() == 1

def test_silver_derived_columns(spark_local):
    from spark.silver.transform_silver import clean, enrich
    rows = [
        (1, datetime(2024,1,10,8,0), datetime(2024,1,10,8,30), 5.0, 15.0, 2, 1,'01','2024','ts','f','b','yellow'),
    ]
    df     = _make_df(spark_local, rows)
    result = enrich(clean(df))
    row    = result.first()
    assert row.trip_duration_minutes == pytest.approx(30.0, abs=0.1)
    assert row.speed_mph > 0
    assert row.fare_per_mile > 0
    assert row.time_of_day_bucket == 'Morning'
