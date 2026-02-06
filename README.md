# Open Access Journals in DOAJ

Dashboard repository for exploring journal-level records from the DOAJ public data dump (CSV), refreshed hourly.

## What this repository is

This project builds and publishes a public dashboard that helps users discover and analyze DOAJ journal metadata with:

- summary KPIs (journals, countries, articles, languages, no APC),
- interactive filters,
- charts (countries, subjects, licenses, peer-review, timeline, APC insights),
- map visualization by publisher country,
- paginated journal table with search and sorting.

The dashboard is designed for GitHub Pages and uses static JSON files generated from the DOAJ CSV dump.

## Data source

- Source: DOAJ public CSV dump  
  `https://doaj.org/csv`
- Data update cadence: hourly (automated via GitHub Actions).
- Data license from source: CC0 (DOAJ public data dump).

## Repository structure

- `docs/index.html`  
  Main dashboard application (HTML/CSS/JS).
- `docs/data/journals.json`  
  Normalized journal records used by the dashboard.
- `docs/data/aggregates.json`  
  Precomputed aggregates for summary/meta use.
- `docs/data/meta.json`  
  Source and fetch metadata.
- `scripts/fetch_journals.py`  
  CSV fetch + normalization script.
- `.github/workflows/fetch-journals.yml`  
  Hourly data refresh workflow.
- `.github/workflows/deploy-pages.yml`  
  GitHub Pages deployment workflow.
- `.github/workflows/backup-repository.yml`  
  Scheduled/manual backup workflow (zip + git bundle artifact).

## How data flows

1. `fetch-journals.yml` runs hourly (and can be run manually).
2. It executes `python scripts/fetch_journals.py`.
3. Script downloads DOAJ CSV and writes:
   - `docs/data/journals.json`
   - `docs/data/aggregates.json`
   - `docs/data/meta.json`
4. If files changed, workflow commits `docs/data/*` to `main`.
5. `deploy-pages.yml` publishes `docs/` to GitHub Pages.

## How to use (for visitors)

1. Open the published dashboard page.
2. Use left-side filters and journal table controls.
3. Use APC chart controls:
   - Continent selector
   - Currency selector (EUR/USD)
4. Journal table APC column and APC chart update based on selected currency.

## How to run locally

### 1) Refresh data locally

```bash
python3 scripts/fetch_journals.py
```

Optional source override:

```bash
DOAJ_CSV_URL="https://doaj.org/csv" python3 scripts/fetch_journals.py
```

### 2) Serve the dashboard locally

From repository root:

```bash
python3 -m http.server 8000
```

Open:

`http://localhost:8000/docs/`

## How to publish/update on GitHub

### Required repository settings

- Actions:
  - Allow workflows
  - Workflow permissions: Read and write
- Pages:
  - Source: GitHub Actions

### Manual runs

- Run `Fetch DOAJ journals (hourly)` to refresh data now.
- Run `Deploy dashboard to GitHub Pages` to publish now.
- Run `Backup DOAJ repository` to generate backup artifacts now.

## Backup and recovery

The backup workflow uploads artifacts:

- snapshot zip (tracked files at current HEAD),
- full history git bundle (`.bundle`) + metadata text file.

To restore history from bundle:

```bash
git clone <bundle-file-path> restored-repo
```

## Notes

- This project currently uses the CSV source only (no DOAJ API key needed).
- Exchange rates for APC conversion are loaded live in the browser when the page opens.
  - Primary: Frankfurter API (`https://www.frankfurter.app/`)
  - Fallback: open.er-api.com (`https://open.er-api.com/`)
- If data appears stale, re-run fetch and deploy workflows, then refresh the page.

## License

This repository uses dual licensing:

- Code (`scripts/**`, `.github/workflows/**`, code in `docs/index.html`):  
  PolyForm Noncommercial 1.0.0  
  https://polyformproject.org/licenses/noncommercial/1.0.0/  
  See `LICENSE-CODE.md`

- Content/documentation (including this README and narrative UI text):  
  CC BY-NC-SA 4.0  
  https://creativecommons.org/licenses/by-nc-sa/4.0/  
  See `LICENSE-CONTENT.md`

- Source dataset in `docs/data/**` is derived from DOAJ public dump (CC0):  
  https://doaj.org/docs/public-data-dump/  
  https://creativecommons.org/publicdomain/zero/1.0/

For repository-wide details, see `LICENSE`.
