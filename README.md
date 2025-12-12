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

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

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

### Individual Scripts

Run individual enrichment scripts:

```bash
# Enrich with IGDB data
python run_igdb.py

# Enrich with RAWG data
python run_rawg.py

# Enrich with Steam data
python run_steam.py

# Enrich with SteamSpy data
python run_steamspy.py

# Enrich with HowLongToBeat data
python run_hltb.py
```

### Run All

Run all enrichment scripts sequentially:

```bash
python run_all.py
```

### Input/Output

- **Input**: Place your game catalog CSV file in `data/input/Games_Personal.csv`
- **Output**: Processed files are saved in `data/processed/` with names like:
  - `Games_IGDB.csv`
  - `Games_RAWG.csv`
  - `Games_Steam.csv`
  - `Games_SteamSpy.csv`
  - `Games_HLTB.csv`

The scripts will:
- Create output directories if they don't exist
- Resume processing from where it left off (skips already processed rows)
- Save progress incrementally every 10 processed games

## Project Structure

```
game-catalog-builder/
├── modules/
│   ├── igdb_client.py      # IGDB API client
│   ├── rawg_client.py       # RAWG API client
│   ├── steam_client.py      # Steam API client
│   ├── steamspy_client.py   # SteamSpy API client
│   ├── hltb_client.py       # HowLongToBeat client
│   ├── merger.py           # Merge utilities
│   └── utilities.py         # Common utilities
├── run_igdb.py             # IGDB enrichment script
├── run_rawg.py             # RAWG enrichment script
├── run_steam.py            # Steam enrichment script
├── run_steamspy.py         # SteamSpy enrichment script
├── run_hltb.py             # HowLongToBeat enrichment script
├── run_all.py              # Run all scripts
├── credentials.yaml        # API credentials (not in git)
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

## Data Fields

The scripts add various columns to your CSV:

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

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
