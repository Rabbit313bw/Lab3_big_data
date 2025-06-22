import sys
from pathlib import Path

root_path = Path(__file__).parent.parent.parent

sys.path.append(str(root_path))

from src.utils.spark_utils import create_spark_session

spark = create_spark_session()

df = spark.read.parquet("data/nyc_taxi.parquet")
df.write.mode("overwrite").parquet("./data/bronze/taxi")
