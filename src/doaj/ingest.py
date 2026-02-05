from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import re

import polars as pl

from .config import Settings, load_settings
from .metrics import compute_metrics
from .sources import DoajApiSource


YEAR_RE = re.compile(r"(19|20)\d{2}")


def _to_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace("|", ";").split(";")]
        return [part for part in parts if part]
    return [str(value).strip()]


def _first(value: Any) -> Any:
    if isinstance(value, list) and value:
        return value[0]
    return value


def _extract_year(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, int):
            return str(value)
        if isinstance(value, str):
            match = YEAR_RE.search(value)
            if match:
                return match.group(0)
    return None


def _license_value(license_field: Any) -> str | None:
    if not license_field:
        return None
    if isinstance(license_field, list):
        license_field = license_field[0] if license_field else None
    if isinstance(license_field, dict):
        return (
            license_field.get("type")
            or license_field.get("title")
            or license_field.get("url")
        )
    return str(license_field)


def normalize_journal_record(record: dict[str, Any]) -> dict[str, Any]:
    bib = record.get("bibjson") or record.get("bib_json") or {}
    subjects = []
    raw_subjects = bib.get("subject") or bib.get("subjects")
    if isinstance(raw_subjects, list):
        for subject in raw_subjects:
            if isinstance(subject, dict):
                term = subject.get("term") or subject.get("name")
                if term:
                    subjects.append(term)
            elif subject:
                subjects.append(str(subject))
    elif raw_subjects:
        subjects.extend(_to_list(raw_subjects))

    languages = bib.get("language") or record.get("language")
    license_value = _license_value(bib.get("license") or record.get("license"))

    publisher = bib.get("publisher") or record.get("publisher")
    publisher_name = None
    publisher_country = None
    if isinstance(publisher, dict):
        publisher_name = publisher.get("name") or publisher.get("publisher")
        publisher_country = publisher.get("country")
    elif publisher:
        publisher_name = str(publisher)

    year = _extract_year(
        record.get("created_date"),
        record.get("createdAt"),
        record.get("last_updated"),
        bib.get("year"),
        record.get("year"),
    )

    return {
        "id": record.get("id") or record.get("identifier"),
        "title": bib.get("title") or record.get("title"),
        "country": bib.get("country") or record.get("country") or publisher_country,
        "language": _to_list(languages),
        "license": license_value,
        "publisher": publisher_name,
        "year": year,
        "subject": subjects,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def ingest_from_api(settings: Settings) -> Path:
    source = DoajApiSource(settings)
    records = source.fetch_records()
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = settings.raw_dir / "journals.jsonl"
    source.save_raw(records, raw_path)

    normalized = [normalize_journal_record(record) for record in records]
    df = pl.DataFrame(normalized)

    metrics_payload = compute_metrics(df, source="doaj_api")
    metrics_path = settings.metrics_dir / "metrics.json"
    _write_json(metrics_path, metrics_payload)
    return metrics_path


def ingest() -> Path:
    settings = load_settings()
    settings.metrics_dir.mkdir(parents=True, exist_ok=True)
    return ingest_from_api(settings)
