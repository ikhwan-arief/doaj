# DOAJ Dashboard

## Setup
Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Configure API access
Copy the template and set your API key (read-only endpoints may not require one):

```bash
cp .env.example .env
```

Edit `.env` and update `DOAJ_API_KEY` if needed.

## Build metrics cache
Pull data from the DOAJ API:

```bash
doaj ingest
```

## Run the dashboard
Start the API server:

```bash
doaj api --reload
```

Serve the HTML frontend in another terminal:

```bash
doaj serve
```

Open `http://localhost:8000/` in your browser.

## Development checks
```bash
pytest
ruff check .
```

## Notes
- Cached metrics are stored in `data/metrics/metrics.json` (ignored by git).
- The API uses `data/metrics/sample.json` if no cache is available.
- The DOAJ public search API limits results to 1000 records; the ingest step caps to that limit.
