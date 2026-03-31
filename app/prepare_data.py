from pathlib import Path
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim

spark = (
    SparkSession.builder
    .appName("data preparation")
    .master("local[*]")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
)

INPUT_PARQUET = "/app/a.parquet"
OUTPUT_DIR = Path("/app/data")
N = 1000

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

df = (
    spark.read.parquet(INPUT_PARQUET)
    .select("id", "title", "text")
    .filter(col("text").isNotNull())
    .filter(length(trim(col("text"))) > 0)
    .limit(N)
)

def normalize_title(title: str) -> str:
    title = title or "untitled"
    return sanitize_filename(title).replace(" ", "_")

for row in df.toLocalIterator():
    doc_id = str(row["id"])
    title = normalize_title(row["title"])
    text = row["text"].strip()
    path = OUTPUT_DIR / f"{doc_id}_{title}.txt"
    path.write_text(text, encoding="utf-8")

spark.stop()