import sys
import os
from functions.functions import compare_data_content

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

"""
@pytest.fixture
def etl_pipeline(spark_session):
    return ETLPipeline(spark_session)
"""

def test_extract(etl_pipeline):
    input_path = "tests/sample_data.csv"  # Provide a sample CSV file for testing
    data = etl_pipeline.extract(input_path)
    assert data.count() > 0

def test_transform(etl_pipeline):
    # Sample data for testing transformation
    input_path = "tests/sample_data.csv"
    data = etl_pipeline.extract(input_path)
    transformed_data = etl_pipeline.transform(data)
    sample_output_path = "tests/sample_output"
    sample_output_data = etl_pipeline.extract(sample_output_path)
    assert compare_data_content(transformed_data, sample_output_data) == True

# Add more tests for other components and scenarios

# Run the tests: pytest test_etl_pipeline.py
