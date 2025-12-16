# Game Catalog Builder

A Python tool for enriching video game catalogs with metadata from multiple APIs including IGDB, RAWG, Steam, SteamSpy, and HowLongToBeat.

## Features

- **IGDB Integration**: Fetch game metadata from IGDB (genres, themes, game modes, perspectives, franchises, engines, companies)
- **RAWG Integration**: Get game information including year, genres, platforms, tags, ratings, and Metacritic scores
- **Steam Integration**: Retrieve Steam App IDs, tags, reviews, prices, and categories
- **SteamSpy Integration**: Fetch ownership statistics, player counts, and playtime data
- **HowLongToBeat Integration**: Get completion time estimates (main story, extra content, completionist)
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
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate
   ```

3. Install dependencies:
   ```bash
   pip install -e .
   # or:
   pip install -r requirements.txt
   ```

For local development tools (linting/type-checking/tests):

```bash
pip install -e ".[dev]"
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
python -m game_catalog_builder.fetch_provider_examples "Doom (2016)"
```

This writes example files under `docs/examples/doom/`.

## Usage

**Important**: Make sure your virtual environment is activated (see Setup step 2) before running commands.

## Docs

- `docs/how-it-works.md`: pipeline, caching, validation
- `docs/provider-fields.md`: provider field/column reference (practical)
- `docs/provider-field-reference.md`: field reference (catalog + examples)
- `docs/provider-json-schema.md`: example capture file conventions

### Basic Usage

Process all sources with a single command:

```bash
python run.py data/input/Games_Personal.csv
```

This will:
- Process all API sources (IGDB, RAWG, Steam, SteamSpy, HLTB)
- Save individual results to `data/output/`
- Automatically merge all results into `Games_Final.csv`

### Process Specific Sources

Process only specific API sources:

```bash
# Process only IGDB
python run.py data/input/Games_Personal.csv --source igdb

# Process only RAWG
python run.py data/input/Games_Personal.csv --source rawg

# Process only Steam
python run.py data/input/Games_Personal.csv --source steam

# Process SteamSpy (requires Steam data first)
python run.py data/input/Games_Personal.csv --source steamspy

# Process only HowLongToBeat
python run.py data/input/Games_Personal.csv --source hltb
```

### Custom Output and Cache Directories

```bash
# Specify custom output directory
python run.py data/input/Games_Personal.csv --output my_output/

# Specify custom cache directory
python run.py data/input/Games_Personal.csv --cache my_cache/

# Use custom credentials file (default: data/credentials.yaml)
python run.py data/input/Games_Personal.csv --credentials my_credentials.yaml
```

### Merge Only

If you've already processed files and just want to merge:

```bash
python run.py data/input/Games_Personal.csv --source all --merge
```

### Validation Report

Generate a cross-provider consistency report (title/year/platform + Steam AppID cross-check), including a suggested canonical title when providers disagree:

```bash
python run.py data/input/Games_Personal.csv --merge --validate
```

The report includes `ReviewTitle` (a broader “needs review” flag) and `SuggestedRenamePersonalName` (a stricter/high-confidence rename suggestion).

### Identity Map (Stage-1 style review)

Generate a row-by-row identity mapping table with provider IDs, matched names, and fuzzy match scores:

```bash
python run.py data/input/Games_Personal.csv --merge --validate --identity-map
```

### Command-Line Options

```
positional arguments:
  input                 Input CSV file with game catalog

optional arguments:
  --output OUTPUT       Output directory for generated files (default: data/output)
  --cache CACHE        Cache directory for API responses (default: data/cache)
  --credentials CREDENTIALS
                       Path to credentials.yaml file (default: data/credentials.yaml)
  --source {igdb,rawg,steam,steamspy,hltb,all}
                       Which API source to process (default: all)
  --merge              Merge all processed files into a final CSV
  --merge-output MERGE_OUTPUT
                       Output file for merged results (default: data/output/Games_Final.csv)
  --log-file LOG_FILE  Log file path (default: data/output/enrichment.log)
  --validate           Generate a cross-provider validation report (default: off)
  --validate-output VALIDATE_OUTPUT
                       Output file for validation report (default: data/output/Validation_Report.csv)
  --debug              Enable DEBUG logging (default: INFO)
```

### Input/Output

- **Input**: Any CSV file with a "Name" column containing game names
- **Output**: Generated files are saved in the output directory (default: `data/output/`):
  - `Games_IGDB.csv`
  - `Games_RAWG.csv`
  - `Games_Steam.csv`
  - `Games_SteamSpy.csv`
  - `Games_HLTB.csv`
  - `Games_Final.csv` (merged result)

The tool will:
- Create output directories if they don't exist
- Resume processing from where it left off (skips already processed rows)
- Save progress incrementally every 10 processed games

### Caching

Provider caches are stored under `data/cache/` and are keyed by provider IDs when available, with a separate name-to-id mapping to avoid repeated searches on reruns.

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
│   │   └── steamspy_client.py
│   └── utils/                # Utilities
│       ├── __init__.py
│       ├── merger.py
│       └── utilities.py
├── data/
│   ├── input/                 # Input CSVs (ignored; keep folder)
│   ├── output/                # Generated outputs + logs (ignored; keep folder)
│   └── cache/                 # API caches (ignored; keep folder)
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
pip install -e .
```

This installs the package in editable mode, allowing you to modify the code without reinstalling.

### Running Tests

```bash
python -m unittest discover tests
```

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
