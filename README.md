# Game Catalog Builder

A Python tool for enriching video game catalogs with metadata from multiple providers including IGDB, RAWG, Steam, SteamSpy, HowLongToBeat, and Wikidata.

## Features

- **IGDB Integration**: Fetch game metadata from IGDB (genres, themes, game modes, perspectives, franchises, engines, companies)
- **RAWG Integration**: Get game information including year, genres, platforms, tags, ratings, and Metacritic scores
- **Steam Integration**: Retrieve Steam App IDs, tags, reviews, prices, categories, developers, and publishers
- **SteamSpy Integration**: Fetch ownership statistics, player counts, and playtime data
- **HowLongToBeat Integration**: Get completion time estimates (main story, extra content, completionist)
- **Wikidata Integration**: Fetch cross-platform identity metadata (release year, platforms, developer/publisher, genres/series)
- **Wikipedia signals (official APIs)**: Pageviews (30/90/365d + launch-window proxies) and a short summary extract for faster “is this the right game?” review
- **Fuzzy Matching**: Intelligent game name matching across different APIs
- **Caching**: Built-in caching to avoid redundant API calls
- **Rate Limiting**: Automatic rate limiting to respect API limits

## Setup

### Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

### Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd game-catalog-builder
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate
   ```

3. Install dependencies:
   ```bash
   python -m pip install -e .
   # or:
   python -m pip install -r requirements.txt
   ```

For local development tools (linting/type-checking/tests):

```bash
python -m pip install -e ".[dev]"
python -m pytest
```

   **Note**: Always activate the virtual environment (step 2) before running the tool. After activation, `python` will use the venv's Python automatically.

### Configuration

1. Create a `data/credentials.yaml` file with your API credentials (start from `data/credentials.example.yaml`):

   ```yaml
   # API Credentials
   # This file contains sensitive credentials and should not be committed to version control

   igdb:
     client_id: "YOUR_IGDB_CLIENT_ID"
     client_secret: "YOUR_IGDB_CLIENT_SECRET"

   rawg:
     api_key: "YOUR_RAWG_API_KEY"

   ```

2. **IGDB Credentials**: 
   - Sign up at [IGDB API](https://www.igdb.com/api)
   - Create a Twitch application to get Client ID and Client Secret

3. **RAWG API Key**:
   - Sign up at [RAWG API](https://rawg.io/apidocs)
   - Get your API key from the dashboard

> **Note**: The `data/credentials.yaml` file is ignored by git and will not be committed to version control.

### Provider JSON examples

To capture full provider responses (useful for deciding what additional fields to extract), run:

```bash
python -m game_catalog_builder.tools.fetch_provider_examples "Doom (2016)"
```

This writes example files under `docs/examples/doom-2016/` (slugified from the input name).

## Usage

**Important**: Make sure your virtual environment is activated (see Setup step 2) before running commands.

### Network failures vs “not found”

- Provider “Not found …” warnings mean the provider responded but the game was not matched above the similarity threshold.
- Network/HTTP failures are logged distinctly (`[NETWORK] ...`, `[HTTP] ...`), and on cache misses the tool fails fast so you don’t silently generate “not found” results while offline.
- When a provider returns `429 Too Many Requests`, retries honor `Retry-After` when available. Provider cache stats append a `429=...` suffix only when it occurred.
- Provider clients reuse a persistent `requests.Session()` (connection pooling) to reduce TLS/handshake overhead on long runs.
- Logging defaults to `INFO` with periodic progress lines; per-row provider “Processing …” lines are `DEBUG` (enable with `--debug`).

### Recommended Workflow (Spreadsheet Round-Trip)

This project supports a round-trip workflow where you edit the enriched CSV in a spreadsheet, then
sync only user-editable fields back into the canonical catalog.

```bash
# 1) import: create/update a stable, RowId-based catalog (source of truth) and match provider IDs
python run.py import path/to/exported_user_sheet.csv --out data/input/Games_Catalog.csv

# 2) resolve (optional): third pass to repin-or-unpin likely-wrong pins based on diagnostics
# (dry-run by default; pass --apply to persist changes)
python run.py resolve --catalog data/input/Games_Catalog.csv --out data/input/Games_Catalog.csv --apply

# 3) enrich: generate provider outputs + an editable enriched sheet (does not modify the catalog)
python run.py enrich data/input/Games_Catalog.csv --source all

# 4) edit: open `data/output/Games_Enriched.csv` in Google Sheets / Excel and edit your user fields

# 5) sync: copy user-editable columns (and pinned provider IDs) back into the catalog by RowId
python run.py sync data/input/Games_Catalog.csv data/output/Games_Enriched.csv

# 6) enrich again (optional): regenerate public data after edits / pinned ID fixes
python run.py enrich data/input/Games_Catalog.csv --source all
```

Your original export does not need a `RowId` column. If it’s missing, `import` will generate stable
RowIds in `Games_Catalog.csv`. If your export already includes `RowId`, it must be unique.

`import` also adds:
- Provider ID columns (`RAWG_ID`, `IGDB_ID`, `Steam_AppID`, `HLTB_ID`, `Wikidata_QID`) so you can pin matches.
- An optional `HLTB_Query` override (used only when `HLTB_ID` is empty) for tricky HLTB searches.
- A `YearHint` column you can fill (e.g. `1999`) to disambiguate titles like reboots/remakes.
- A small set of diagnostic columns so you can adjust IDs before enrichment:
  - `RAWG_MatchedName`, `RAWG_MatchScore`, etc
  - For HLTB, the importer also captures extra match context:
    - `HLTB_MatchedYear`, `HLTB_MatchedPlatforms`
  - `ReviewTags` (compact reasons to review)
  - `MatchConfidence` (`HIGH` / `MEDIUM` / `LOW`)

`ReviewTags` includes a small set of high-signal tags to make review actionable without adding lots of columns:
- Consensus/outliers: `provider_consensus:*`, `provider_outlier:*`, `provider_no_consensus`
- Metadata outliers: `year_outlier:*`, `platform_outlier:*` (and `*_no_consensus`)
- Actionable rollups: `likely_wrong:*`, `ambiguous_title_year`

Import notes:
- `import` emits warnings and diagnostics but does not auto-unpin pins.
- `resolve` is the optional third pass that can repin-or-unpin likely-wrong pins (and conservatively retry repins).

It refreshes match diagnostics by fetching the provider name for any pinned IDs. Evaluation columns
are not carried into `Games_Enriched.csv`. `sync` writes back a clean catalog without evaluation
columns.

If you want a clean catalog right away (no diagnostic columns), run:

```bash
python run.py import path/to/exported_user_sheet.csv --out data/input/Games_Catalog.csv --no-diagnostics
```

Both `import` and `enrich` support “in place” runs (e.g. `import X.csv --out X.csv`, or
`enrich X.csv --merge-output X.csv`). For `enrich`, provider/public columns are always regenerated.

### Provider selection

Provider lists are comma-separated via `--source`:

```bash
# all providers
./.venv/bin/python run.py enrich data/input/Games_Catalog.csv --source all

# core providers (fastest/highest value): IGDB + RAWG + Steam
./.venv/bin/python run.py import data/input/Games_User.csv --out data/input/Games_Catalog.csv --source core

# explicit list
./.venv/bin/python run.py import data/input/Games_User.csv --out data/input/Games_Catalog.csv --source igdb,rawg,steam

# optional sources
./.venv/bin/python run.py enrich data/input/Games_Catalog.csv --source wikidata
```

### Production tiers (AAA/AA/Indie)

`Production_Tier` / `Production_TierReason` are computed from a manually curated mapping file:

- `data/production_tiers.yaml` (local file; git-ignored)

Start by copying the checked-in baseline:

```bash
cp data/production_tiers.example.yaml data/production_tiers.yaml
```

To (optionally) prune/normalize your mapping file based on your current enriched CSV (offline; no network):

```bash
./.venv/bin/python run.py collect-production-tiers data/output/Games_Enriched.csv --out data/production_tiers.yaml --base data/production_tiers.yaml
```

For a “TODO list” file to hand to a chatbot, generate only the missing tiers. The TODO file includes
catalog-dependent `count`/`examples` to help prioritize:

```bash
./.venv/bin/python run.py collect-production-tiers data/output/Games_Enriched.csv \\
  --only-missing --out data/production_tiers.todo.yaml --base data/production_tiers.yaml
```

Fill in `tier` values (`AAA` / `AA` / `Indie`) in `data/production_tiers.yaml`, then re-run `enrich`.

Steam notes:
- Steam `appdetails` requests use `l=english&cc=us` (some AppIDs return `success=false` without a country code).
- When `Steam_AppID` is empty but IGDB/RAWG exposes it, the importer may infer and pin it automatically.

### Signals (reach / now / ratings)

The enriched output also includes a small set of derived “signals” intended for sorting and prioritization:

- Reach: `Reach_SteamSpyOwners_*`, `Reach_SteamReviews`, `Reach_RAWGRatingsCount`, `Reach_IGDBRatingCount`, `Reach_IGDBAggregatedRatingCount`, and a blended `Reach_Composite`.
- Now (recent interest): `Now_SteamSpyPlayers2Weeks`, `Now_SteamSpyPlaytime*2Weeks`, `SteamSpy_CCU` (when present), and a blended `Now_Composite`.
- Ratings: `CommunityRating_Composite_100` and `CriticRating_Composite_100` (best-effort blend across available sources).
- Launch interest: `Launch_Interest_100` (best-effort, derived from existing counts/signals).

Limitations:
- These are **platform-biased** toward Steam/PC and Wikipedia availability; they are meant to be useful, not universally comparable across all platforms.
- “Now” coverage is limited without additional first-party sources; we currently avoid scraping.
- Wikidata “facts” like units sold/budget/awards are too sparse/inconsistent to treat as first-class numeric columns.

### Experiments (subsets / debugging)

All commands support `--run-dir`, which points at a folder containing:

- `input/`
- `output/`
- `cache/`
- `logs/`

Default `--run-dir` is `data/`. For experiments, create a folder under `experiments/<name>/` and run with `--run-dir experiments/<name>`.

Overrides:
- `--cache` overrides `<run-dir>/cache/`
- `--output` / `--output-dir` overrides `<run-dir>/output/` (command-dependent)
- `--log-file` writes to an explicit path; `--logs-dir` overrides `<run-dir>/logs/` for autogenerated log filenames

## Docs

- `docs/how-it-works.md`: pipeline, caching, validation
- `docs/providers/README.md`: provider schemas/fields index
- `docs/providers/provider-fields.md`: provider field/column reference (practical)
- `docs/providers/provider-field-reference.md`: field reference (catalog + examples)
- `docs/providers/provider-json-schema.md`: example capture file conventions

### CLI help

The CLI is subcommand-based:

```bash
python run.py --help
python run.py import --help
python run.py enrich --help
python run.py sync --help
```

### Files and folders

- Main inputs: `data/input/Games_User.csv` → `data/input/Games_Catalog.csv`
- Main output: `data/output/Games_Enriched.csv`
- Logs: `data/logs/log-<timestamp>-<command>.log`
- Provider caches: `data/cache/*.json`

### Caching

Provider caches are stored under `data/cache/`:

- `by_query`: query → lightweight candidate lists (including negative caching for not-found).
- `by_id`: provider ID → raw provider payload.

Enriched outputs also include unified provider score columns (0–100 where available):
- `Score_RAWG_100`, `Score_IGDB_100`, `Score_SteamSpy_100`, `Score_HLTB_100`
- Provider-specific Metacritic scores when available: `RAWG_Metacritic`, `Steam_Metacritic`

If you delete a cache file under `data/cache/`, the tool will refetch it as needed.

## Project Structure

```
game-catalog-builder/
├── game_catalog_builder/      # Main package
│   ├── __init__.py
│   ├── cli.py                 # Command-line interface
│   ├── clients/              # API clients
│   │   ├── __init__.py
│   │   ├── hltb_client.py
│   │   ├── igdb_client.py
│   │   ├── rawg_client.py
│   │   ├── steam_client.py
│   │   ├── steamspy_client.py
│   │   ├── wikidata_client.py
│   │   ├── wikipedia_pageviews_client.py
│   │   └── wikipedia_summary_client.py
│   ├── tools/                # Maintenance tools (examples, schemas, production tiers)
│   └── utils/                # Utilities
│       ├── __init__.py
│       ├── merger.py
│       ├── signals.py
│       └── utilities.py
├── data/
│   ├── input/                 # Main catalog inputs (ignored; keep folder)
│   ├── output/                # Main catalog outputs (ignored; keep folder)
│   ├── cache/                 # Main provider caches (ignored; keep folder)
│   ├── logs/                  # Execution logs (ignored; keep folder)
│   └── experiments/           # Subsets/debug runs (ignored; keep folder)
├── run.py                     # Entry point
├── pyproject.toml            # Project metadata
├── requirements.txt          # Python dependencies
├── data/credentials.yaml     # API credentials (not in git)
└── README.md                 # This file
```

## Data Fields

The tool adds various columns to your CSV:

### IGDB Fields
- `IGDB_ID`: IGDB game ID
- `IGDB_Genres`: Game genres
- `IGDB_Themes`: Game themes
- `IGDB_GameModes`: Available game modes
- `IGDB_Perspectives`: Camera perspectives
- `IGDB_Franchise`: Franchise name
- `IGDB_Engine`: Game engine
- `IGDB_Companies`: Development companies

### RAWG Fields
- `RAWG_ID`: RAWG game ID
- `RAWG_Year`: Release year
- `RAWG_Genre`: Primary genre
- `RAWG_Genre2`: Secondary genre
- `RAWG_Platforms`: Available platforms
- `RAWG_Tags`: Game tags
- `RAWG_Rating`: User rating
- `RAWG_RatingsCount`: Number of ratings
- `RAWG_Metacritic`: Metacritic score

### Steam Fields
- `Steam_AppID`: Steam application ID
- `Steam_Tags`: User-defined tags
- `Steam_ReviewCount`: Number of reviews
- `Steam_ReviewPercent`: Positive review percentage
- `Steam_Price`: Game price
- `Steam_Categories`: Game categories

### SteamSpy Fields
- `SteamSpy_Owners`: Estimated owner count
- `SteamSpy_Players`: Estimated player count
- `SteamSpy_CCU`: Peak concurrent users
- `SteamSpy_PlaytimeAvg`: Average playtime

### HowLongToBeat Fields
- `HLTB_Main`: Main story completion time
- `HLTB_Extra`: Main + extra completion time
- `HLTB_Completionist`: 100% completion time

## Dependencies

See `requirements.txt` for the complete list. Main dependencies include:

- `pandas`: Data manipulation
- `requests`: HTTP requests
- `pyyaml`: YAML file parsing
- `rapidfuzz`: Fast fuzzy string matching
- `howlongtobeatpy`: HowLongToBeat API client

## Development

### Installing in Development Mode

```bash
# Create and use a local venv (recommended)
python3 -m venv .venv
source .venv/bin/activate

# Install in editable mode, including dev tools (ruff/pytest/mypy)
python -m pip install -e ".[dev]"
```

This installs the package in editable mode, allowing you to modify the code without reinstalling.

### Running Tests

```bash
python -m pytest -q
```

### Formatting and Linting

```bash
# Format code
python -m ruff format .

# Lint (includes import sorting checks)
python -m ruff check .
```

## License

See `LICENSE`.

## Contributing

[Add contribution guidelines here]
