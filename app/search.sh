#!/bin/bash
set -e

QUERY="$*"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --driver-memory 1g \
  --archives /app/.venv.tar.gz#.venv \
  query.py "$QUERY"