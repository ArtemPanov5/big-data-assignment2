#!/usr/bin/env python3
import re
import sys
from collections import Counter

TOKEN_RE = re.compile(r"[a-z0-9]+")

def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())

for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    tokens = tokenize(text)

    if not tokens:
        continue

    dl = len(tokens)
    counts = Counter(tokens)

    print(f"STAT\tDOC_COUNT\t1")
    print(f"STAT\tTOTAL_DL\t{dl}")
    print(f"DOC\t{doc_id}\t{title}\t{dl}")

    for term, tf in counts.items():
        print(f"TERM\t{term}\t{doc_id}\t{title}\t{tf}\t{dl}")