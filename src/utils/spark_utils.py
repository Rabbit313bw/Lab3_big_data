
from pyspark.sql import SparkSession

def create_spark_session(app_name="BigDataLab"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
