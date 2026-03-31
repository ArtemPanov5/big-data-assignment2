#!/bin/bash
set -e

INPUT_PATH="${1:-/input/data}"
RAW_OUT="/indexer/raw"
POSTINGS_OUT="/indexer/postings"
VOCAB_OUT="/indexer/vocabulary"
DOCS_OUT="/indexer/documents"
STATS_OUT="/indexer/stats"

STREAMING_JAR=$(find "$HADOOP_HOME" -name 'hadoop-streaming*.jar' | head -n 1)

if [ -z "$STREAMING_JAR" ]; then
  echo "Cannot find hadoop-streaming jar"
  exit 1
fi

hdfs dfs -rm -r -f /indexer || true

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="simple-search-engine-indexer" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$RAW_OUT"

TMP_DIR="/tmp/indexer_split"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"

hdfs dfs -cat ${RAW_OUT}/part-* | awk -F'\t' '$1=="POSTING"{print $2"\t"$3"\t"$4"\t"$5"\t"$6}' > "${TMP_DIR}/postings.tsv"
hdfs dfs -cat ${RAW_OUT}/part-* | awk -F'\t' '$1=="VOCAB"{print $2"\t"$3}' > "${TMP_DIR}/vocabulary.tsv"
hdfs dfs -cat ${RAW_OUT}/part-* | awk -F'\t' '$1=="DOC"{print $2"\t"$3"\t"$4}' > "${TMP_DIR}/documents.tsv"
hdfs dfs -cat ${RAW_OUT}/part-* | awk -F'\t' '$1=="STAT"{print $2"\t"$3}' > "${TMP_DIR}/stats.tsv"

hdfs dfs -mkdir -p "$POSTINGS_OUT" "$VOCAB_OUT" "$DOCS_OUT" "$STATS_OUT"

hdfs dfs -put -f "${TMP_DIR}/postings.tsv" "${POSTINGS_OUT}/part-00000"
hdfs dfs -put -f "${TMP_DIR}/vocabulary.tsv" "${VOCAB_OUT}/part-00000"
hdfs dfs -put -f "${TMP_DIR}/documents.tsv" "${DOCS_OUT}/part-00000"
hdfs dfs -put -f "${TMP_DIR}/stats.tsv" "${STATS_OUT}/part-00000"

echo "Index created successfully"
hdfs dfs -ls /indexer