#!/bin/sh
set -e

SOURCE="${1:-api}"
CSV_PATH="${2:-}"

if [ "$SOURCE" = "csv" ] && [ -z "$CSV_PATH" ]; then
  echo "Usage: scripts/ingest.sh csv /path/to/file.csv"
  exit 1
fi

if [ "$SOURCE" = "csv" ]; then
  doaj ingest --source csv --csv "$CSV_PATH"
else
  doaj ingest --source api
fi
