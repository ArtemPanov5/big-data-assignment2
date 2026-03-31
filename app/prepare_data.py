import re
import unicodedata
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("data preparation") \
    .getOrCreate()

INPUT_PARQUET = "hdfs:///a.parquet"
N = 1000


def normalize_title(title: str) -> str:
    title = title or "untitled"
    title = unicodedata.normalize("NFKD", title)
    title = title.encode("ascii", "ignore").decode("ascii")
    title = sanitize_filename(title)
    title = title.replace(" ", "_")
    title = re.sub(r"[^A-Za-z0-9._-]+", "_", title)
    title = re.sub(r"_+", "_", title).strip("._")
    return title or "untitled"


df = (
    spark.read.parquet(INPUT_PARQUET)
    .select("id", "title", "text")
    .filter("text is not null and length(trim(text)) > 0")
    .sample(fraction=0.01, seed=0)
    .limit(N)
)


def create_doc(row):
    doc_id = str(row["id"])
    doc_title = normalize_title(row["title"])
    filename = f"data/{doc_id}_{doc_title}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])


for row in df.toLocalIterator():
    create_doc(row)

spark.stop()