#!/usr/bin/env python3
import sys

current_type = None
current_key = None

term_postings = []
doc_title = None
doc_dl = None
stat_sum = 0

def flush():
    global current_type, current_key, term_postings, doc_title, doc_dl, stat_sum

    if current_type == "TERM":
        df = len(term_postings)
        print(f"VOCAB\t{current_key}\t{df}")
        for doc_id, title, tf, dl in term_postings:
            print(f"POSTING\t{current_key}\t{doc_id}\t{title}\t{tf}\t{dl}")

    elif current_type == "DOC":
        if doc_title is not None and doc_dl is not None:
            print(f"DOC\t{current_key}\t{doc_title}\t{doc_dl}")

    elif current_type == "STAT":
        print(f"STAT\t{current_key}\t{stat_sum}")

    term_postings = []
    doc_title = None
    doc_dl = None
    stat_sum = 0

for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    parts = line.split("\t")

    rec_type = parts[0]

    if rec_type == "TERM":
        _, term, doc_id, title, tf, dl = parts
        key = term
    elif rec_type == "DOC":
        _, doc_id, title, dl = parts
        key = doc_id
    elif rec_type == "STAT":
        _, stat_name, value = parts
        key = stat_name
    else:
        continue

    if current_type is not None and (rec_type != current_type or key != current_key):
        flush()

    current_type = rec_type
    current_key = key

    if rec_type == "TERM":
        term_postings.append((doc_id, title, int(tf), int(dl)))
    elif rec_type == "DOC":
        doc_title = title
        doc_dl = int(dl)
    elif rec_type == "STAT":
        stat_sum += int(value)

if current_type is not None:
    flush()