from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import subprocess

KEYSPACE = "search_engine"

def hdfs_cat(pattern: str):
    result = subprocess.run(
        ["bash", "-lc", f"hdfs dfs -cat {pattern}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]

cluster = Cluster(["cassandra-server"])
session = cluster.connect()

session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
""")
session.set_keyspace(KEYSPACE)

session.execute("""
CREATE TABLE IF NOT EXISTS vocabulary (
    term text PRIMARY KEY,
    df int
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS documents (
    doc_id text PRIMARY KEY,
    title text,
    dl int
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS postings (
    term text,
    doc_id text,
    title text,
    tf int,
    dl int,
    PRIMARY KEY (term, doc_id)
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS corpus_stats (
    name text PRIMARY KEY,
    value double
)
""")

session.execute("TRUNCATE vocabulary")
session.execute("TRUNCATE documents")
session.execute("TRUNCATE postings")
session.execute("TRUNCATE corpus_stats")

insert_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
insert_doc = session.prepare("INSERT INTO documents (doc_id, title, dl) VALUES (?, ?, ?)")
insert_posting = session.prepare("INSERT INTO postings (term, doc_id, title, tf, dl) VALUES (?, ?, ?, ?, ?)")
insert_stat = session.prepare("INSERT INTO corpus_stats (name, value) VALUES (?, ?)")

for line in hdfs_cat("/indexer/vocabulary/*"):
    term, df = line.split("\t")
    session.execute(insert_vocab, (term, int(df)))

for line in hdfs_cat("/indexer/documents/*"):
    doc_id, title, dl = line.split("\t")
    session.execute(insert_doc, (doc_id, title, int(dl)))

for line in hdfs_cat("/indexer/postings/*"):
    term, doc_id, title, tf, dl = line.split("\t")
    session.execute(insert_posting, (term, doc_id, title, int(tf), int(dl)))

for line in hdfs_cat("/indexer/stats/*"):
    name, value = line.split("\t")
    session.execute(insert_stat, (name, float(value)))

print("Index loaded into Cassandra successfully")