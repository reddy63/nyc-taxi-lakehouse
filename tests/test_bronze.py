import os, sys, pytest
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

class TestBuildLandingPaths:
    def test_single_month(self):
        from spark.bronze.ingest_bronze import build_landing_paths
        paths = build_landing_paths('yellow', 2024, ['01'])
        assert len(paths) == 1
        assert '2024' in paths[0] and '01' in paths[0]
        assert paths[0].startswith('s3a://')

    def test_multiple_months(self):
        from spark.bronze.ingest_bronze import build_landing_paths
        paths = build_landing_paths('yellow', 2024, ['01','02','03'])
        assert len(paths) == 3
        months = [p.split('/')[-2] for p in paths]
        assert months == ['01','02','03']

    def test_taxi_type_in_path(self):
        from spark.bronze.ingest_bronze import build_landing_paths
        paths = build_landing_paths('green', 2024, ['06'])
        assert 'green' in paths[0]

REQUIRED_BRONZE_COLS = ['_ingested_at','_source_file','_batch_id','_taxi_type','_year','_month']

@pytest.fixture(scope='module')
def spark_local():
    pytest.importorskip('pyspark')
    pytest.importorskip('delta')
    from spark.utils.spark_session import get_spark
    spark = get_spark('test-bronze', master='local[1]', executor_memory='512m', driver_memory='512m')
    yield spark
    spark.stop()

def test_bronze_metadata_columns(spark_local):
    from spark.bronze.ingest_bronze import add_bronze_metadata
    raw = spark_local.createDataFrame(
        [(1, datetime(2024,1,15,8,0), datetime(2024,1,15,8,20), 3.5, 12.5)],
        ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','trip_distance','fare_amount'])
    result = add_bronze_metadata(raw, 's3a://test/', 'batch-001')
    for col in REQUIRED_BRONZE_COLS:
        assert col in result.columns, f'Missing: {col}'

def test_bronze_batch_id_consistent(spark_local):
    from spark.bronze.ingest_bronze import add_bronze_metadata
    raw = spark_local.createDataFrame(
        [(1, datetime(2024,1,10,9,0), datetime(2024,1,10,9,30), 2.0, 8.0),
         (2, datetime(2024,1,11,10,0), datetime(2024,1,11,10,25), 1.5, 7.0)],
        ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','trip_distance','fare_amount'])
    result = add_bronze_metadata(raw, 's3a://test/', 'abc-123')
    ids = [r._batch_id for r in result.collect()]
    assert all(i == 'abc-123' for i in ids)
