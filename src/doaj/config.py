from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import tomllib

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    api_base: str
    api_key: str | None
    api_key_header: str
    api_key_prefix: str
    journals_endpoint: str
    query: str
    query_param: str
    page_param: str
    page_size_param: str
    page_size: int
    query_in_path: bool
    data_dir: Path
    schema_path: Path
    api_port: int
    cors_origins: list[str]

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def metrics_dir(self) -> Path:
        return self.data_dir / "metrics"


@dataclass(frozen=True)
class Schema:
    columns: dict[str, str]
    list_columns: dict[str, bool]

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def load_settings() -> Settings:
    load_dotenv()
    cors_raw = os.getenv("DOAJ_CORS_ORIGINS", "http://localhost:8000")
    cors_origins = [origin.strip() for origin in cors_raw.split(",") if origin.strip()]

    data_dir = Path(os.getenv("DOAJ_DATA_DIR", "data"))
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    data_dir = data_dir.resolve()

    schema_path = Path(os.getenv("DOAJ_SCHEMA_PATH", "config/schema.toml"))
    if not schema_path.is_absolute():
        schema_path = PROJECT_ROOT / schema_path
    schema_path = schema_path.resolve()

    return Settings(
        api_base=os.getenv("DOAJ_API_BASE", "https://doaj.org/api/v4").rstrip("/"),
        api_key=os.getenv("DOAJ_API_KEY") or None,
        api_key_header=os.getenv("DOAJ_API_KEY_HEADER", "Authorization"),
        api_key_prefix=os.getenv("DOAJ_API_KEY_PREFIX", "Bearer"),
        journals_endpoint=os.getenv("DOAJ_JOURNALS_ENDPOINT", "search/journals").strip("/"),
        query=os.getenv("DOAJ_QUERY", "*:*"),
        query_param=os.getenv("DOAJ_QUERY_PARAM", "q"),
        page_param=os.getenv("DOAJ_PAGE_PARAM", "page"),
        page_size_param=os.getenv("DOAJ_PAGE_SIZE_PARAM", "pageSize"),
        page_size=_env_int("DOAJ_PAGE_SIZE", 100),
        query_in_path=_env_bool("DOAJ_QUERY_IN_PATH", False),
        data_dir=data_dir,
        schema_path=schema_path,
        api_port=_env_int("DOAJ_API_PORT", 8001),
        cors_origins=cors_origins,
    )


def load_schema(path: Path) -> Schema:
    if not path.exists():
        return Schema(columns={}, list_columns={})

    with path.open("rb") as handle:
        data = tomllib.load(handle)

    columns = data.get("columns", {})
    list_columns = data.get("list_columns", {})

    return Schema(columns=columns, list_columns=list_columns)
