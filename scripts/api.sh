#!/bin/sh
set -e

HOST="${1:-127.0.0.1}"
PORT="${2:-8001}"

uvicorn doaj.api:app --host "$HOST" --port "$PORT" --reload
