import sys
import os

# Add the root directory of your project to the sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import pytest
from pyspark.sql import SparkSession
from pipeline.etl_pipeline import ETLPipeline

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[2]").appName("TestSession").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def etl_pipeline(spark_session):
    return ETLPipeline(spark_session)

def test_extract(etl_pipeline):
    input_path = "tests/sample_data.csv"  # Provide a sample CSV file for testing
    data = etl_pipeline.extract(input_path)
    assert data.count() > 0

def test_transform(etl_pipeline):
    # Sample data for testing transformation
    sample_data = [("Alice", 25), ("Bob", 17), ("Charlie", 30)]
    schema = ["Name", "Age"]
    input_df = etl_pipeline.spark.createDataFrame(sample_data, schema=schema)

    transformed_data = etl_pipeline.transform(input_df)
    assert transformed_data.count() == 2  # Two people are older than 18

# Add more tests for other components and scenarios

# Run the tests: pytest test_etl_pipeline.py
