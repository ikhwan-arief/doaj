from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import json

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import load_settings


@lru_cache(maxsize=1)
def _load_metrics() -> dict:
    settings = load_settings()
    metrics_path = settings.metrics_dir / "metrics.json"
    sample_path = settings.metrics_dir / "sample.json"

    path = metrics_path if metrics_path.exists() else sample_path
    if not path.exists():
        return {"summary": {"total": 0, "generated_at": None, "source": "empty"}, "metrics": {}}

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def create_app() -> FastAPI:
    settings = load_settings()
    app = FastAPI(title="DOAJ Dashboard API")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/api/summary")
    def summary(refresh: bool = False) -> dict:
        if refresh:
            _load_metrics.cache_clear()
        return _load_metrics().get("summary", {})

    @app.get("/api/metrics")
    def metrics(refresh: bool = False) -> dict:
        if refresh:
            _load_metrics.cache_clear()
        return _load_metrics().get("metrics", {})

    return app


app = create_app()
