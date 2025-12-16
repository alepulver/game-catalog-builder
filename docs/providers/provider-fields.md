# Provider fields (what you can extract)

This document lists the main fields obtainable from each provider endpoint used by this project, and how they relate to the CSV columns we output.

It is intentionally “practical” rather than an exhaustive copy of each provider’s full API schema. For complete schemas, refer to provider documentation.

For a generated “catalog + observed examples” reference, see `docs/providers/provider-field-reference.md` (generated from `docs/providers/provider-field-catalog.yaml`).

## Conventions

- **ID**: stable identifier used for caching and joining.
- **Search**: the name-based query used to find the ID.
- **Details**: the richer document used to extract metadata (if applicable).
- **CSV column**: the column name in `Games_<Provider>.csv` / `Games_Final.csv`.
- **Path**: a representative JSON path (or object attribute for HLTB).

## RAWG

- API docs: https://rawg.io/apidocs

- **ID**: `RAWG_ID`
- **Search**: `GET /api/games?search=<name>&page_size=10&lang=en`
- **Details (optional)**: `GET /api/games/{id}` (not required by current pipeline)

Current output columns:

| CSV column | Path | Description |
|---|---|---|
| `RAWG_ID` | `id` | RAWG game id |
| `RAWG_Name` | `name` | Provider title |
| `RAWG_Released` | `released` | Release date (`YYYY-MM-DD`) |
| `RAWG_Year` | `released[:4]` | Release year |
| `RAWG_Genre` | `genres[0].name` | Primary genre |
| `RAWG_Genre2` | `genres[1].name` | Secondary genre |
| `RAWG_Platforms` | `platforms[].platform.name` | Platform list |
| `RAWG_Tags` | `tags[].name` | Tag list (project filters Cyrillic duplicates) |
| `RAWG_Rating` | `rating` | Average rating |
| `RAWG_RatingsCount` | `ratings_count` | Rating count |
| `RAWG_Metacritic` | `metacritic` | Metacritic score |

Other useful fields available (typically richer in `/games/{id}`):

- `description_raw`, `website`
- `developers[].name`, `publishers[].name`
- `stores[]` (store links/ids)
- `esrb_rating.name`
- `platforms[].requirements_{en,ru}.minimum/recommended`

## IGDB (Twitch/IGDB)

- API docs: https://api-docs.igdb.com/
- OAuth docs (Twitch): https://dev.twitch.tv/docs/authentication/getting-tokens-oauth/

- **ID**: `IGDB_ID`
- **OAuth**: `POST https://id.twitch.tv/oauth2/token`
- **Query**: `POST /v4/games` with `search "<name>"; fields ...;`
- **Details**: same `/v4/games` call (field expansion) — one request per game (excluding OAuth).

Current output columns (from the `/v4/games` object returned by the query):

| CSV column | Path | Description |
|---|---|---|
| `IGDB_ID` | `id` | IGDB game id |
| `IGDB_Name` | `name` | Provider title |
| `IGDB_Year` | `first_release_date` | First release year (Unix timestamp → year) |
| `IGDB_Platforms` | `platforms[].name` | Platform list |
| `IGDB_Genres` | `genres[].name` | Genres |
| `IGDB_Themes` | `themes[].name` | Themes |
| `IGDB_GameModes` | `game_modes[].name` | Game modes |
| `IGDB_Perspectives` | `player_perspectives[].name` | Player perspectives |
| `IGDB_Franchise` | `franchises[].name` | Franchise(s) |
| `IGDB_Engine` | `game_engines[].name` | Engine(s) |
| `IGDB_SteamAppID` | `external_games[]` | Cross-check Steam uid when `external_game_source == 1` |

Other useful fields you can request via `fields` (still in the same call):

- `summary`, `storyline`
- `rating`, `rating_count`
- `involved_companies.company.name` (developers/publishers), `involved_companies.developer`, `involved_companies.publisher`
- `collections.name`, `dlcs.name`, `expansions.name`
- `age_ratings.*`, `category`, `status`
- `parent_game.name`, `version_parent.name` (editions/ports)
- `websites.url`

## Steam (Store API)

- Store API (unofficial): https://steamcommunity.com/dev

- **ID**: `Steam_AppID`
- **Search**: `GET /api/storesearch?term=<name>&l=english&cc=US`
- **Details**: `GET /api/appdetails?appids=<appid>&l=english`

Current output columns (from the `data` object inside appdetails):

| CSV column | Path | Description |
|---|---|---|
| `Steam_AppID` | `<appid>` | Steam appid |
| `Steam_Name` | `name` | Store title |
| `Steam_ReleaseYear` | `release_date.date` | Parsed year from the release date string |
| `Steam_Platforms` | `platforms.{windows,mac,linux}` | Platform flags → list |
| `Steam_Tags` | `genres[].description` | Genre list (Steam “genres”) |
| `Steam_ReviewCount` | `recommendations.total` | Review count |
| `Steam_Price` | `is_free` / `price_overview.final_formatted` | Price string |
| `Steam_Categories` | `categories[].description` | Category list |

Other useful fields available in appdetails:

- `short_description`, `detailed_description`, `about_the_game`, `supported_languages`
- `developers[]`, `publishers[]`
- `metacritic.score`
- `content_descriptors`, `required_age`
- `achievements.total`, `screenshots[]`, `movies[]`

## SteamSpy

- API docs: https://steamspy.com/api.php

- **ID**: `Steam_AppID` (same as Steam)
- **Details**: `GET https://steamspy.com/api.php?request=appdetails&appid=<appid>`

Current output columns:

| CSV column | Path | Description |
|---|---|---|
| `SteamSpy_Owners` | `owners` | Owner range string |
| `SteamSpy_Players` | `players_forever` | Lifetime players |
| `SteamSpy_CCU` | `ccu` | Concurrent users |
| `SteamSpy_PlaytimeAvg` | `average_forever` | Average playtime |

Other useful fields available:

- `positive`, `negative` (ratings)
- `average_2weeks`, `median_2weeks`
- `languages` (free-form), `genre` (free-form)

## HowLongToBeat (HLTB)

- Library: https://pypi.org/project/howlongtobeatpy/

- **ID**: library-provided `game_id` when present; otherwise normalized-name fallback.
- **Search**: `howlongtobeatpy.HowLongToBeat().search(<name>)`
- **Details**: same search result object (library wraps HTTP calls).

Current output columns (from the best match object):

| CSV column | Attribute | Description |
|---|---|---|
| `HLTB_Name` | `game_name` | Matched title |
| `HLTB_Main` | `main_story` | Main story time (hours) |
| `HLTB_Extra` | `main_extra` | Main + extras (hours) |
| `HLTB_Completionist` | `completionist` | Completionist time (hours) |

Other useful attributes commonly present:

- `profile_dev`, `profile_platform`, `release_world`, `rating` (varies by library version/data)

