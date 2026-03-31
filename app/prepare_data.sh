#!/bin/bash
set -e

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

if [ ! -f a.parquet ]; then
  echo "Place a.parquet into /app first"
  exit 1
fi

rm -rf data
mkdir -p data

hdfs dfs -rm -r -f /data || true
hdfs dfs -rm -r -f /input/data || true
hdfs dfs -put -f a.parquet / || true

spark-submit prepare_data.py

hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/*.txt /data/

spark-submit - <<'PY'
import os
import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("build-input-data").getOrCreate()
sc = spark.sparkContext

def transform(item):
    path, text = item
    name = os.path.basename(path)
    if name.endswith(".txt"):
        name = name[:-4]
    doc_id, doc_title = name.split("_", 1)
    clean_text = re.sub(r"[\r\n\t]+", " ", text).strip()
    return f"{doc_id}\t{doc_title}\t{clean_text}"

rdd = sc.wholeTextFiles("hdfs:///data/*.txt").map(transform).coalesce(1)
rdd.saveAsTextFile("hdfs:///input/data")
spark.stop()
PY

echo "HDFS /data:"
hdfs dfs -ls /data | head

echo "HDFS /input/data:"
hdfs dfs -ls /input/data

echo "done data preparation!"