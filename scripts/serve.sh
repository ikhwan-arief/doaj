#!/bin/sh
set -e

PORT="${1:-8000}"
DIR="${2:-web}"

if [ ! -d "$DIR" ]; then
  echo "Directory '$DIR' does not exist."
  echo "Usage: scripts/serve.sh [port] [dir]"
  exit 1
fi

echo "Serving $DIR at http://localhost:$PORT"
python3 -m http.server "$PORT" --directory "$DIR"
