import sys
from pathlib import Path

root_path = Path(__file__).parent.parent.parent

sys.path.append(str(root_path))

from src.utils.spark_utils import create_spark_session

spark = create_spark_session()
df = spark.read.parquet("./data/silver/taxi_clean")

agg = df.groupBy("payment_type") \
        .agg({"fare_amount": "avg", "tip_amount": "avg"}) \
        .withColumnRenamed("avg(fare_amount)", "avg_fare") \
        .withColumnRenamed("avg(tip_amount)", "avg_tip")

agg = agg.repartition(1)
agg.write.mode("overwrite").parquet("./data/gold/taxi_agg")