from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

import polars as pl


def _to_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace("|", ";").split(";")]
        return [part for part in parts if part]
    return [str(value).strip()]


def _count_by(df: pl.DataFrame, column: str, top_n: int | None = None) -> list[dict[str, Any]]:
    if column not in df.columns:
        return []
    series = df[column].drop_nulls()
    counts = series.value_counts().sort("counts", descending=True)
    if top_n:
        counts = counts.head(top_n)
    return [
        {"key": str(row[0]), "value": int(row[1])}
        for row in counts.iter_rows()
        if row[0] not in ("", "None")
    ]


def _count_list(df: pl.DataFrame, column: str, top_n: int | None = None) -> list[dict[str, Any]]:
    if column not in df.columns:
        return []
    counter: Counter[str] = Counter()
    for value in df[column].drop_nulls().to_list():
        counter.update(_to_list(value))
    results = counter.most_common(top_n)
    return [{"key": key, "value": value} for key, value in results if key]


def compute_metrics(df: pl.DataFrame, source: str) -> dict[str, Any]:
    summary = {
        "total": df.height,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
    }

    metrics = {
        "by_country": _count_by(df, "country", top_n=200),
        "by_year": _count_by(df, "year"),
        "by_license": _count_by(df, "license", top_n=20),
        "by_language": _count_list(df, "language", top_n=30),
        "by_subject": _count_list(df, "subject", top_n=30),
        "top_publishers": _count_by(df, "publisher", top_n=30),
    }

    return {"summary": summary, "metrics": metrics}
