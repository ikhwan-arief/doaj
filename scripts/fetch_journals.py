#!/usr/bin/env python3
"""Fetch all DOAJ journal records and write normalized snapshots for the dashboard.

- Hits DOAJ API v4 search endpoint and paginates.
- Throttles to respect ~2 requests/second guidance.
- Outputs:
    docs/data/journals.json     (normalized records for client-side filtering)
    docs/data/aggregates.json   (precomputed counts for quick display)
    docs/data/meta.json         (fetch timestamp and source latest update)

Env vars:
- DOAJ_BASE_URL (optional): override API base, default https://doaj.org/api/v4
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

BASE_URL = os.environ.get("DOAJ_BASE_URL", "https://doaj.org/api/v4")
PAGE_SIZE = 100
THROTTLE_SECONDS = 0.6  # ~1.6 rps to stay below 2 rps average
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "data")


def fetch_page(page: int) -> Dict[str, Any]:
    url = f"{BASE_URL}/search/journals/{{}}"
    params = {
        "page": page,
        "pageSize": PAGE_SIZE,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    admin = rec.get("admin", {})
    bj = rec.get("bibjson", {})

    apc = bj.get("apc", {}) or {}
    license_block = bj.get("license", {}) or {}
    copyright_block = bj.get("copyright", {}) or {}
    waiver_block = bj.get("waiver", {}) or {}
    preservation_block = bj.get("preservation", {}) or {}
    pid_block = bj.get("pid_scheme", {}) or {}
    publisher = bj.get("publisher", {}) or {}
    subjects = bj.get("subject", []) or []

    def _safe_list(val: Any) -> List[Any]:
        if val is None:
            return []
        if isinstance(val, list):
            return val
        return [val]

    def _preservation_service(val: Any) -> Optional[str]:
        if not val:
            return None
        if isinstance(val, list):
            return val[0] if val else None
        return str(val)

    return {
        "id": rec.get("id"),
        "title": bj.get("title"),
        "publisher": publisher.get("name"),
        "country": publisher.get("country"),
        "pissn": bj.get("pissn"),
        "eissn": bj.get("eissn"),
        "apc_has": bool(apc.get("has_apc")) if apc.get("has_apc") is not None else None,
        "apc_max_price": (apc.get("max", {}) or {}).get("price"),
        "apc_max_currency": (apc.get("max", {}) or {}).get("currency"),
        "waiver_has": bool(waiver_block.get("has_waiver")) if waiver_block.get("has_waiver") is not None else None,
        "license_type": license_block.get("type"),
        "license_url": license_block.get("url"),
        "license_BY": bool(license_block.get("BY")) if license_block.get("BY") is not None else None,
        "license_NC": bool(license_block.get("NC")) if license_block.get("NC") is not None else None,
        "license_ND": bool(license_block.get("ND")) if license_block.get("ND") is not None else None,
        "license_SA": bool(license_block.get("SA")) if license_block.get("SA") is not None else None,
        "author_retains": bool(copyright_block.get("author_retains")) if copyright_block.get("author_retains") is not None else None,
        "preservation_has": bool(preservation_block.get("has_preservation")) if preservation_block.get("has_preservation") is not None else None,
        "preservation_service": _preservation_service(preservation_block.get("service")),
        "pid_has": bool(pid_block.get("has_pid_scheme")) if pid_block.get("has_pid_scheme") is not None else None,
        "pid_scheme": pid_block.get("scheme"),
        "subject_terms": [s.get("term") for s in subjects if s.get("term")],
        "language": _safe_list(bj.get("language")),
        "keywords": _safe_list(bj.get("keywords")),
        "oa_start": bj.get("oa_start"),
        "created_date": rec.get("created_date"),
        "last_updated": rec.get("last_updated"),
        "last_manual_update": rec.get("last_manual_update"),
        "in_doaj": bool(admin.get("in_doaj")) if admin.get("in_doaj") is not None else None,
    }


def aggregate(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    country = Counter(r.get("country") for r in records if r.get("country"))
    license_type = Counter(r.get("license_type") for r in records if r.get("license_type"))
    apc = Counter("yes" if r.get("apc_has") else "no" for r in records if r.get("apc_has") is not None)
    waiver = Counter("yes" if r.get("waiver_has") else "no" for r in records if r.get("waiver_has") is not None)
    preservation = Counter("yes" if r.get("preservation_has") else "no" for r in records if r.get("preservation_has") is not None)
    pid = Counter("yes" if r.get("pid_has") else "no" for r in records if r.get("pid_has") is not None)
    author_retains = Counter("yes" if r.get("author_retains") else "no" for r in records if r.get("author_retains") is not None)

    subjects = Counter()
    for r in records:
        for term in r.get("subject_terms", []) or []:
            subjects[term] += 1

    created_year = Counter()
    for r in records:
        dt = r.get("created_date")
        if dt:
            try:
                year = datetime.fromisoformat(dt.replace("Z", "+00:00")).year
                created_year[year] += 1
            except Exception:
                continue

    last_updated_max = None
    for r in records:
        lu = r.get("last_updated")
        if not lu:
            continue
        try:
            parsed = datetime.fromisoformat(lu.replace("Z", "+00:00"))
        except Exception:
            continue
        if last_updated_max is None or parsed > last_updated_max:
            last_updated_max = parsed

    return {
        "total_journals": len(records),
        "by_country": dict(country),
        "by_license_type": dict(license_type),
        "apc": dict(apc),
        "waiver": dict(waiver),
        "preservation": dict(preservation),
        "pid": dict(pid),
        "author_retains": dict(author_retains),
        "subjects_top": subjects.most_common(100),
        "created_year": dict(created_year),
        "last_updated_max": last_updated_max.isoformat() if last_updated_max else None,
    }


def main() -> int:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    records: List[Dict[str, Any]] = []

    # First page to get total
    first = fetch_page(1)
    total = first.get("total", 0)
    records.extend(first.get("results", []))

    total_pages = math.ceil(total / PAGE_SIZE) if total else 1
    print(f"Found total {total} journals; fetching {total_pages} pages", file=sys.stderr)

    for page in range(2, total_pages + 1):
        time.sleep(THROTTLE_SECONDS)
        payload = fetch_page(page)
        page_results = payload.get("results", [])
        if not page_results:
            break
        records.extend(page_results)
        print(f"Fetched page {page}/{total_pages}", file=sys.stderr)

    normalized = [normalize_record(r) for r in records]
    agg = aggregate(normalized)

    fetched_at = datetime.now(timezone.utc).isoformat()

    # Write outputs
    journals_path = os.path.join(OUTPUT_DIR, "journals.json")
    aggregates_path = os.path.join(OUTPUT_DIR, "aggregates.json")
    meta_path = os.path.join(OUTPUT_DIR, "meta.json")

    with open(journals_path, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False)

    with open(aggregates_path, "w", encoding="utf-8") as f:
        json.dump(agg, f, ensure_ascii=False)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"fetched_at": fetched_at, "source_last_updated_max": agg.get("last_updated_max")}, f, ensure_ascii=False)

    print(f"Wrote {journals_path} ({len(normalized)} records)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
