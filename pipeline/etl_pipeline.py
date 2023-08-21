from pyspark.sql import SparkSession
from modules.slow_task_notification import slow_task_notification

class ETLPipeline:
    def __init__(self, spark_session):
        self.spark = spark_session

    def extract(self, input_path):
        return self.spark.read.csv(input_path, header=True, inferSchema=True)

    def transform(self, data):
        transformed_data = data.select("Name", "Age").filter(data.Age > 18)
        return transformed_data

    def load(self, transformed_data, output_path):
        transformed_data.write.mode("overwrite").csv(output_path, header=True)

@slow_task_notification(10, "TEAMS_WEBHOOK_URL")
def main():
    spark = SparkSession.builder.appName("ETLPipeline").getOrCreate()
    etl_pipeline = ETLPipeline(spark)

    input_path = "input_data.csv"
    output_path = "output_data"

    data = etl_pipeline.extract(input_path)
    transformed_data = etl_pipeline.transform(data)
    etl_pipeline.load(transformed_data, output_path)

if __name__ == "__main__":
    main()
