#!/bin/bash
set -e

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -rm -r -f /data || true
hdfs dfs -rm -r -f /input/data || true

if [ -f a.parquet ]; then
  echo "Found /app/a.parquet. Generating 1000 documents from parquet."
  rm -rf data
  mkdir -p data
  hdfs dfs -put -f a.parquet / || true
  spark-submit prepare_data.py
elif ls data/*.txt >/dev/null 2>&1; then
  echo "a.parquet not found. Using existing txt files from /app/data."
else
  echo "ERROR: neither /app/a.parquet nor /app/data/*.txt found."
  exit 1
fi

hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/*.txt /data/

spark-submit build_input_data.py

echo "HDFS /data:"
hdfs dfs -ls /data | head

echo "HDFS /input/data:"
hdfs dfs -ls /input/data

echo "Data preparation done."