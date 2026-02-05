# DOAJ

## Python setup
Create a virtual environment and install dev tools:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Run tests and lint:

```bash
pytest
ruff check .
```

## Run HTML locally
1. Put HTML files in `web/`.
2. Start the server with `./scripts/serve.sh`.
3. Open `http://localhost:8000/` in your browser.

You can pass a custom port or directory:
- `./scripts/serve.sh 8080`
- `./scripts/serve.sh 8000 web`
