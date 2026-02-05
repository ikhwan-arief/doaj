from doaj import __version__
from doaj.main import build_parser


def test_version_is_set():
    assert __version__


def test_parser_has_commands():
    parser = build_parser()
    subparsers = parser._subparsers._group_actions[0]
    assert {"ingest", "api", "serve"}.issubset(subparsers.choices)
