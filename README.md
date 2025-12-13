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
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

   **Note**: Always activate the virtual environment (step 2) before running the tool. After activation, `python` will use the venv's Python automatically.

### Configuration

1. Create a `credentials.yaml` file in the project root with your API credentials:

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

> **Note**: The `credentials.yaml` file is already in `.gitignore` and will not be committed to version control.

## Usage

**Important**: Make sure your virtual environment is activated (see Setup step 2) before running commands.

### Basic Usage

Process all sources with a single command:

```bash
python run.py data/input/Games_Personal.csv
```

This will:
- Process all API sources (IGDB, RAWG, Steam, SteamSpy, HLTB)
- Save individual results to `data/processed/`
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

# Use custom credentials file (default: credentials.yaml in project root)
python run.py data/input/Games_Personal.csv --credentials my_credentials.yaml
```

### Merge Only

If you've already processed files and just want to merge:

```bash
python run.py data/input/Games_Personal.csv --source all --merge
```

### Command-Line Options

```
positional arguments:
  input                 Input CSV file with game catalog

optional arguments:
  --output OUTPUT       Output directory for processed files (default: data/processed)
  --cache CACHE        Cache directory for API responses (default: data/raw)
  --credentials CREDENTIALS
                       Path to credentials.yaml file (default: credentials.yaml in project root)
  --source {igdb,rawg,steam,steamspy,hltb,all}
                       Which API source to process (default: all)
  --merge              Merge all processed files into a final CSV
  --merge-output MERGE_OUTPUT
                       Output file for merged results (default: data/processed/Games_Final.csv)
```

### Input/Output

- **Input**: Any CSV file with a "Name" column containing game names
- **Output**: Processed files are saved in the output directory (default: `data/processed/`):
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
├── run.py                     # Entry point
├── pyproject.toml            # Project metadata
├── requirements.txt          # Python dependencies
├── credentials.yaml          # API credentials (not in git)
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
