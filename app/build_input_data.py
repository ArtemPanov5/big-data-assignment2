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