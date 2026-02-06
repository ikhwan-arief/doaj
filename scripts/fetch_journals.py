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
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = os.environ.get("DOAJ_BASE_URL", "https://doaj.org/api/v4")
PAGE_SIZE = 100
THROTTLE_SECONDS = 0.6  # ~1.6 rps to stay below 2 rps average
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "data")
ALL_QUERY_CANDIDATES = ("*:*", "*", "{}")
MAX_RESULTS_PER_QUERY = 1000


def build_session() -> requests.Session:
    """Use retries for transient network/rate-limit responses."""
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "doaj-dashboard-fetch/1.0",
        }
    )
    return session


def fetch_page(session: requests.Session, page: int, query: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/search/journals/{quote(query, safe='')}"
    params = {
        "page": page,
        "pageSize": PAGE_SIZE,
    }
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def discover_query(session: requests.Session) -> str:
    """Different API deployments can accept different 'match all' query strings."""
    errors: List[str] = []
    for query in ALL_QUERY_CANDIDATES:
        try:
            payload = fetch_page(session, page=1, query=query)
            if isinstance(payload.get("results", []), list):
                return query
            errors.append(f"{query}: unexpected payload shape")
        except requests.RequestException as err:
            errors.append(f"{query}: {err}")
    raise RuntimeError("Unable to discover DOAJ all-record query. " + " | ".join(errors))


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    admin = rec.get("admin", {})
    bj = rec.get("bibjson", {})

    def _safe_list(val: Any) -> List[Any]:
        if val is None:
            return []
        if isinstance(val, list):
            return val
        return [val]

    def _first_dict(val: Any) -> Dict[str, Any]:
        if isinstance(val, dict):
            return val
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    return item
        return {}

    def _preservation_service(val: Any) -> Optional[str]:
        if not val:
            return None
        if isinstance(val, list):
            return val[0] if val else None
        return str(val)

    apc = _first_dict(bj.get("apc"))
    license_raw = bj.get("license")
    license_block = _first_dict(license_raw)
    copyright_block = _first_dict(bj.get("copyright"))
    waiver_block = _first_dict(bj.get("waiver"))
    preservation_block = _first_dict(bj.get("preservation"))
    pid_block = _first_dict(bj.get("pid_scheme"))

    publisher_raw = bj.get("publisher")
    publisher_block = _first_dict(publisher_raw)
    publisher_name = publisher_block.get("name")
    publisher_country = publisher_block.get("country")
    if not publisher_name and isinstance(publisher_raw, str):
        publisher_name = publisher_raw

    subjects = _safe_list(bj.get("subject"))
    subject_terms: List[str] = []
    for item in subjects:
        if isinstance(item, dict):
            term = item.get("term")
            if term:
                subject_terms.append(str(term))
        elif item:
            subject_terms.append(str(item))

    license_type = license_block.get("type")
    license_url = license_block.get("url")
    if not license_type and isinstance(license_raw, str):
        license_type = license_raw

    return {
        "id": rec.get("id"),
        "title": bj.get("title"),
        "publisher": publisher_name,
        "country": publisher_country,
        "pissn": bj.get("pissn"),
        "eissn": bj.get("eissn"),
        "apc_has": bool(apc.get("has_apc")) if apc.get("has_apc") is not None else None,
        "apc_max_price": (apc.get("max", {}) or {}).get("price"),
        "apc_max_currency": (apc.get("max", {}) or {}).get("currency"),
        "waiver_has": bool(waiver_block.get("has_waiver")) if waiver_block.get("has_waiver") is not None else None,
        "license_type": license_type,
        "license_url": license_url,
        "license_BY": bool(license_block.get("BY")) if license_block.get("BY") is not None else None,
        "license_NC": bool(license_block.get("NC")) if license_block.get("NC") is not None else None,
        "license_ND": bool(license_block.get("ND")) if license_block.get("ND") is not None else None,
        "license_SA": bool(license_block.get("SA")) if license_block.get("SA") is not None else None,
        "author_retains": bool(copyright_block.get("author_retains")) if copyright_block.get("author_retains") is not None else None,
        "preservation_has": bool(preservation_block.get("has_preservation")) if preservation_block.get("has_preservation") is not None else None,
        "preservation_service": _preservation_service(preservation_block.get("service")),
        "pid_has": bool(pid_block.get("has_pid_scheme")) if pid_block.get("has_pid_scheme") is not None else None,
        "pid_scheme": pid_block.get("scheme"),
        "subject_terms": subject_terms,
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
    session = build_session()

    records: List[Dict[str, Any]] = []

    # First page to get total
    all_query = discover_query(session)
    print(f"Using all-record query: {all_query}", file=sys.stderr)
    first = fetch_page(session, page=1, query=all_query)
    total = first.get("total", 0)
    records.extend(first.get("results", []))

    target_total = min(total, MAX_RESULTS_PER_QUERY) if total else len(records)
    total_pages = math.ceil(target_total / PAGE_SIZE) if target_total else 1
    is_capped = bool(total and total > MAX_RESULTS_PER_QUERY)
    if is_capped:
        print(
            f"Found total {total} journals; API result window limit detected. "
            f"Fetching first {target_total} journals ({total_pages} pages).",
            file=sys.stderr,
        )
    else:
        print(f"Found total {total} journals; fetching {total_pages} pages", file=sys.stderr)

    for page in range(2, total_pages + 1):
        time.sleep(THROTTLE_SECONDS)
        try:
            payload = fetch_page(session, page=page, query=all_query)
        except requests.HTTPError as err:
            # Some DOAJ deployments return 400 when page window exceeds limit.
            if err.response is not None and err.response.status_code == 400:
                print(
                    f"Stopped at page {page} due to API result window limit (HTTP 400).",
                    file=sys.stderr,
                )
                break
            raise
        page_results = payload.get("results", [])
        if not page_results:
            break
        records.extend(page_results)
        print(f"Fetched page {page}/{total_pages}", file=sys.stderr)
        if len(records) >= target_total:
            break

    normalized = [normalize_record(r) for r in records[:target_total]]
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
        json.dump(
            {
                "fetched_at": fetched_at,
                "source_last_updated_max": agg.get("last_updated_max"),
                "source_total": total,
                "fetched_total": len(normalized),
                "result_cap": MAX_RESULTS_PER_QUERY,
                "is_capped": is_capped,
            },
            f,
            ensure_ascii=False,
        )

    print(f"Wrote {journals_path} ({len(normalized)} records)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
