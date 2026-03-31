#!/bin/bash
set -e

INPUT_PATH="${1:-/input/data}"

bash create_index.sh "$INPUT_PATH"
bash store_index.sh