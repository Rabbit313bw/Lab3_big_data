#!/bin/bash
pip install -r requirements.txt

echo "Этап 1: Bronze слой..."
python3 src/etl/ingest_to_bronze.py

echo "Этап 2: Silver слой..."
python3 src/etl/bronze_to_silver.py

echo "Этап 3: Gold слой..."
python3 src/etl/silver_to_gold.py

echo "Этап 4: Обучение модели..."
python3 src/ml/train_model.py