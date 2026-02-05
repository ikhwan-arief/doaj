from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import time
import urllib.parse

import requests

from .config import Settings


@dataclass
class ApiPage:
    records: list[dict[str, Any]]
    total: int | None


class DoajApiSource:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _headers(self) -> dict[str, str]:
        if not self.settings.api_key:
            return {}
        if self.settings.api_key_prefix:
            value = f"{self.settings.api_key_prefix} {self.settings.api_key}"
        else:
            value = self.settings.api_key
        return {self.settings.api_key_header: value}

    def _build_url(self, query: str, page: int) -> tuple[str, dict[str, Any]]:
        base = self.settings.api_base.rstrip("/")
        endpoint = self.settings.journals_endpoint.strip("/")
        params = {
            self.settings.page_param: page,
            self.settings.page_size_param: self.settings.page_size,
        }

        if self.settings.query_in_path:
            encoded_query = urllib.parse.quote(query, safe="")
            url = f"{base}/{endpoint}/{encoded_query}"
        else:
            url = f"{base}/{endpoint}"
            params[self.settings.query_param] = query

        return url, params

    def _fetch_page(self, query: str, page: int) -> ApiPage:
        url, params = self._build_url(query, page)
        response = requests.get(url, headers=self._headers(), params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        records = payload.get("results") or payload.get("data") or payload.get("items") or []
        total = payload.get("total")
        return ApiPage(records=records, total=total)

    def fetch_records(self, query: str | None = None) -> list[dict[str, Any]]:
        query = query or self.settings.query
        records: list[dict[str, Any]] = []
        page = 1
        total = None

        while True:
            page_data = self._fetch_page(query, page)
            records.extend(page_data.records)
            total = page_data.total if page_data.total is not None else total

            if not page_data.records:
                break

            if total is not None:
                if len(records) >= total:
                    break

            page += 1
            time.sleep(0.2)

        return records

    def save_raw(self, records: list[dict[str, Any]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")

