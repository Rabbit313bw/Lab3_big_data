import sys
from pathlib import Path

root_path = Path(__file__).parent.parent.parent

sys.path.append(str(root_path))

from src.utils.spark_utils import create_spark_session

spark = create_spark_session()
df = spark.read.parquet("./data/bronze/taxi")

df_clean = df.dropna().filter("fare_amount > 0 and trip_distance > 0")

df_clean.write.mode("overwrite").parquet("./data/silver/taxi_clean")