from __future__ import annotations

import argparse
import functools
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Sequence

import uvicorn

from .config import load_settings
from .ingest import ingest


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()

    parser = argparse.ArgumentParser(prog="doaj", description="DOAJ dashboard tooling")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("ingest", help="Fetch data and build metrics cache from the DOAJ API")

    api_parser = subparsers.add_parser("api", help="Run the local dashboard API")
    api_parser.add_argument("--host", default="127.0.0.1")
    api_parser.add_argument("--port", type=int, default=settings.api_port)
    api_parser.add_argument("--reload", action="store_true")

    serve_parser = subparsers.add_parser("serve", help="Serve the HTML dashboard")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--dir", dest="directory", default="web")

    return parser


def run_api(host: str, port: int, reload: bool) -> None:
    uvicorn.run("doaj.api:app", host=host, port=port, reload=reload)


def run_frontend(directory: str, port: int) -> None:
    path = Path(directory)
    if not path.exists():
        raise SystemExit(f"Directory not found: {directory}")

    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(path))
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    print(f"Serving {directory} at http://localhost:{port}")
    server.serve_forever()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "ingest":
        metrics_path = ingest()
        print(f"Metrics written to {metrics_path}")
        return 0

    if args.command == "api":
        run_api(args.host, args.port, args.reload)
        return 0

    if args.command == "serve":
        run_frontend(args.directory, args.port)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
