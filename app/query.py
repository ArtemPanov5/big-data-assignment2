import math
import re
import sys
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

TOKEN_RE = re.compile(r"[a-z0-9]+")
K1 = 1.2
B = 0.75

def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())

query = " ".join(sys.argv[1:]).strip()
terms = tokenize(query)

if not terms:
    print("Empty query")
    sys.exit(0)

cluster = Cluster(["cassandra-server"])
session = cluster.connect("search_engine")

doc_count_row = session.execute(
    "SELECT value FROM corpus_stats WHERE name='DOC_COUNT'"
).one()
total_dl_row = session.execute(
    "SELECT value FROM corpus_stats WHERE name='TOTAL_DL'"
).one()

if not doc_count_row or not total_dl_row:
    print("Missing corpus stats in Cassandra")
    sys.exit(1)

N = float(doc_count_row.value)
avgdl = float(total_dl_row.value) / N if N > 0 else 0.0

records = []

for term in terms:
    vocab_row = session.execute(
        "SELECT df FROM vocabulary WHERE term=%s", [term]
    ).one()

    if not vocab_row:
        continue

    df = float(vocab_row.df)
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)

    postings = session.execute(
        "SELECT doc_id, title, tf, dl FROM postings WHERE term=%s", [term]
    )

    for row in postings:
        records.append((row.doc_id, row.title, float(row.tf), float(row.dl), idf))

if not records:
    print("No matching documents")
    sys.exit(0)

spark = SparkSession.builder.appName("bm25-query").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(records)

def score_record(rec):
    doc_id, title, tf, dl, idf = rec
    denom = tf + K1 * (1.0 - B + B * (dl / avgdl))
    score = idf * ((tf * (K1 + 1.0)) / denom)
    return (doc_id, (title, score))

def reduce_scores(a, b):
    return (a[0], a[1] + b[1])

top10 = (
    rdd.map(score_record)
       .reduceByKey(reduce_scores)
       .map(lambda x: (x[1][1], x[0], x[1][0]))
       .sortBy(lambda x: -x[0])
       .take(10)
)

for score, doc_id, title in top10:
    print(f"{doc_id}\t{title}")

spark.stop()