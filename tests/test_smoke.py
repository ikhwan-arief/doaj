from doaj import __version__
from doaj.main import main


def test_version_is_set():
    assert __version__


def test_main_runs(capsys):
    main()
    captured = capsys.readouterr()
    assert "DOAJ" in captured.out
