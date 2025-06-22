import sys
from pathlib import Path

root_path = Path(__file__).parent.parent.parent

sys.path.append(str(root_path))

import mlflow
import mlflow.spark
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from src.utils.spark_utils import create_spark_session

spark = create_spark_session()
df = spark.read.parquet("./data/gold/taxi_agg")

indexer = StringIndexer(inputCol="payment_type", outputCol="payment_indexed")
df = indexer.fit(df).transform(df)

vec = VectorAssembler(inputCols=["payment_indexed"], outputCol="features")
data = vec.transform(df)

lr = LinearRegression(featuresCol="features", labelCol="avg_tip")

with mlflow.start_run():
    model = lr.fit(data)
    predictions = model.transform(data)
    
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_metric("rmse", model.summary.rootMeanSquaredError)
