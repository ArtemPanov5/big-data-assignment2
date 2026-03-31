#!/bin/bash
set -e

cd /app

echo "[1/7] Restart SSH"
service ssh restart

echo "[2/7] Start Hadoop/YARN services"
bash start-services.sh

echo "[3/7] Create virtual environment"
python3 -m venv .venv
source .venv/bin/activate

echo "[4/7] Install Python dependencies"
pip install --upgrade pip
pip install -r requirements.txt

echo "[5/7] Pack virtual environment for Spark executors"
rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

echo "[6/7] Prepare data and build index"
bash prepare_data.sh
bash index.sh

echo "[7/7] Run demo search"
bash search.sh "${SEARCH_QUERY:-artificial intelligence}"

echo "Pipeline finished successfully. Keeping container alive for inspection."
tail -f /dev/null