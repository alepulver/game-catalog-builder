# Provider fields (what you can extract)

This document lists the main fields obtainable from each provider endpoint used by this project, and how they relate to the CSV columns we output.

It is intentionally “practical” rather than an exhaustive copy of each provider’s full API schema. For complete schemas, refer to provider documentation.

For a generated “catalog + observed examples” reference, see `docs/providers/provider-field-reference.md` (generated from `docs/providers/provider-field-catalog.yaml`).

## Conventions

- **ID**: stable identifier used for caching and joining.
- **Search**: the name-based query used to find the ID.
- **Details**: the richer document used to extract metadata (if applicable).
- **CSV column**: the column name in `Provider_<Provider>.csv` / `Games_Enriched.csv`.
- **Path**: a representative JSON path (or object attribute for HLTB).
- **Caching**: caches store a mapping of request query → candidates (`by_query`), and provider id → full raw payload (`by_id`).

## RAWG

- API docs: https://rawg.io/apidocs

- **ID**: `RAWG_ID`
- **Search**: `GET /api/games?search=<name>&page_size=10&lang=en`
- **Details**: `GET /api/games/{id}` (fetched/cached so we retain the full payload for future fields)

Caching notes:
- Search responses (`/api/games?search=...`) are cached under `by_query`.
- The cache `by_id` stores the full `/api/games/{id}` detail payload (not the search stub), so fields like `description_raw` and `alternative_names` remain available later.

Current output columns:

| CSV column | Path | Description |
|---|---|---|
| `RAWG_ID` | `id` | RAWG game id |
| `RAWG_Name` | `name` | Provider title |
| `RAWG_Released` | `released` | Release date (`YYYY-MM-DD`) |
| `RAWG_Year` | `released[:4]` | Release year |
| `RAWG_Website` | `website` | Official website (if present) |
| `RAWG_DescriptionRaw` | `description_raw` | Raw description (truncated for CSV ergonomics) |
| `RAWG_Genre` | `genres[0].name` | Primary genre |
| `RAWG_Genre2` | `genres[1].name` | Secondary genre |
| `RAWG_Genres` | `genres[].name` | Full genre list (comma-separated) |
| `RAWG_Platforms` | `platforms[].platform.name` | Platform list |
| `RAWG_Tags` | `tags[].name` | Tag list (project filters Cyrillic duplicates) |
| `RAWG_ESRB` | `esrb_rating.name` | ESRB rating name (when present) |
| `RAWG_Developers` | `developers[].name` | JSON array of developers |
| `RAWG_Publishers` | `publishers[].name` | JSON array of publishers |
| `RAWG_Rating` | `rating` | Average rating |
| `Score_RAWG_100` | `rating` | Rating normalized to 0–100 |
| `RAWG_RatingsCount` | `ratings_count` | Rating count |
| `RAWG_Metacritic` | `metacritic` | Metacritic score |
| `RAWG_Added` | `added` | RAWG “added” count |
| `RAWG_AddedByStatusOwned` | `added_by_status.owned` | “Owned” count |
| `RAWG_AddedByStatusPlaying` | `added_by_status.playing` | “Playing” count |
| `RAWG_AddedByStatusBeaten` | `added_by_status.beaten` | “Beaten” count |
| `RAWG_AddedByStatusToplay` | `added_by_status.toplay` | “To play” count |
| `RAWG_AddedByStatusDropped` | `added_by_status.dropped` | “Dropped” count |

Other useful fields available (typically richer in `/games/{id}`):

- `description_raw`, `description`, `website`
- `alternative_names` (aliases)
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

Caching notes:
- The IGDB cache stores the raw `/v4/games` objects keyed by `language:id` in `by_id`.
- Name search queries are cached under `by_query` as lightweight candidates; `by_id` is the authoritative raw payload.

Current output columns (from the `/v4/games` object returned by the query):

| CSV column | Path | Description |
|---|---|---|
| `IGDB_ID` | `id` | IGDB game id |
| `IGDB_Name` | `name` | Provider title |
| `IGDB_Year` | `first_release_date` | First release year (Unix timestamp → year) |
| `IGDB_Summary` | `summary` | Summary (truncated for CSV ergonomics) |
| `IGDB_Websites` | `websites[].url` | Website URLs (comma-separated, capped) |
| `IGDB_Platforms` | `platforms[].name` | Platform list |
| `IGDB_Genres` | `genres[].name` | Genres |
| `IGDB_Themes` | `themes[].name` | Themes |
| `IGDB_GameModes` | `game_modes[].name` | Game modes |
| `IGDB_Perspectives` | `player_perspectives[].name` | Player perspectives |
| `IGDB_Franchise` | `franchises[].name` | Franchise(s) |
| `IGDB_Engine` | `game_engines[].name` | Engine(s) |
| `IGDB_ParentGame` | `parent_game.name` | Base/parent game (editions/ports context) |
| `IGDB_VersionParent` | `version_parent.name` | Edition parent (when the matched item is a version) |
| `IGDB_DLCs` | `dlcs[].name` | DLCs (comma-separated) |
| `IGDB_Expansions` | `expansions[].name` | Expansions (comma-separated) |
| `IGDB_Ports` | `ports[].name` | Ports (comma-separated) |
| `IGDB_SteamAppID` | `external_games[]` | Cross-check Steam uid when `external_game_source == 1` |
| `IGDB_Developers` | `involved_companies[]` | JSON array of developer companies |
| `IGDB_Publishers` | `involved_companies[]` | JSON array of publisher companies |
| `IGDB_Rating` | `rating` | IGDB user rating (0–100, float) |
| `IGDB_RatingCount` | `rating_count` | Rating count |
| `Score_IGDB_100` | `rating` | Rating normalized to 0–100 (rounded) |
| `IGDB_AggregatedRating` | `aggregated_rating` | Critic aggregated rating (0–100, float) |
| `IGDB_AggregatedRatingCount` | `aggregated_rating_count` | Critic rating count |
| `Score_IGDBCritic_100` | `aggregated_rating` | Critic rating normalized to 0–100 (rounded) |

Other useful fields you can request via `fields` (still in the same call):

- `summary`, `storyline`
- `alternative_names.name` (aliases)
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
- **Details**: `GET /api/appdetails?appids=<appid>&l=english&cc=us`

Caching notes:
- Store search results are cached under `by_query`.
- Full appdetails payloads are cached under `by_id` keyed by appid.

Current output columns (from the `data` object inside appdetails):

| CSV column | Path | Description |
|---|---|---|
| `Steam_AppID` | `<appid>` | Steam appid |
| `Steam_Name` | `name` | Store title |
| `Steam_URL` | `<appid>` | Store page URL |
| `Steam_Website` | `website` | Official website URL (if present) |
| `Steam_ShortDescription` | `short_description` | Short store description |
| `Steam_StoreType` | `type` | Store type (expected `game`; used for filtering/diagnostics) |
| `Steam_ReleaseYear` | `release_date.date` | Parsed year from the release date string |
| `Steam_Platforms` | `platforms.{windows,mac,linux}` | Platform flags → list |
| `Steam_Tags` | `genres[].description` | Genre list (Steam “genres”) |
| `Steam_ReviewCount` | `recommendations.total` | Review count |
| `Steam_Price` | `is_free` / `price_overview.final_formatted` | Price string |
| `Steam_Categories` | `categories[].description` | Category list |
| `Steam_Metacritic` | `metacritic.score` | Metacritic score (Steam) |
| `Steam_Developers` | `developers[]` | JSON array of developers |
| `Steam_Publishers` | `publishers[]` | JSON array of publishers |

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

Caching notes:
- SteamSpy is keyed by `Steam_AppID` and caches raw responses by id.

Current output columns:

| CSV column | Path | Description |
|---|---|---|
| `SteamSpy_Owners` | `owners` | Owner range string |
| `SteamSpy_Players` | `players_forever` | Lifetime players |
| `SteamSpy_Players2Weeks` | `players_2weeks` | Players in last ~2 weeks |
| `SteamSpy_CCU` | `ccu` | Concurrent users |
| `SteamSpy_PlaytimeAvg` | `average_forever` | Average playtime |
| `SteamSpy_PlaytimeAvg2Weeks` | `average_2weeks` | Average playtime (2 weeks) |
| `SteamSpy_PlaytimeMedian2Weeks` | `median_2weeks` | Median playtime (2 weeks) |
| `SteamSpy_Positive` | `positive` | Positive reviews |
| `SteamSpy_Negative` | `negative` | Negative reviews |
| `Score_SteamSpy_100` | `positive/negative` | Positive ratio normalized to 0–100 |
| `SteamSpy_Tags` | `tags{tag:count}` | Top tags (comma-separated; by weight) |
| `SteamSpy_TagsTop` | `tags{tag:count}` | Top tags + weights (JSON list of `[tag,count]` pairs) |

Other useful fields available:

- `positive`, `negative` (ratings)
- `average_2weeks`, `median_2weeks`
- `languages` (free-form), `genre` (free-form)

## HowLongToBeat (HLTB)

- Library: https://pypi.org/project/howlongtobeatpy/

- **ID**: `HLTB_ID` (numeric game id; visible in HLTB URLs like `https://howlongtobeat.com/game/8940`).
- **Search**: `howlongtobeatpy.HowLongToBeat().search(<name>)`
- **Details**: same search result object (library wraps HTTP calls).

Caching notes:
- HLTB search result lists are cached under `by_query` keyed by the *actual query sent* (the `term` value).
- The cache `by_id` stores the full dumped result object payload keyed by numeric `HLTB_ID`, including alias fields like `game_alias` and the embedded `json_content` when present.

Current output columns (from the best match object):

| CSV column | Attribute | Description |
|---|---|---|
| `HLTB_Name` | `game_name` | Matched title |
| `HLTB_Main` | `main_story` | Main story time (hours) |
| `HLTB_Extra` | `main_extra` | Main + extras (hours) |
| `HLTB_Completionist` | `completionist` | Completionist time (hours) |
| `HLTB_ReleaseYear` | `release_world` | Release year (when present) |
| `HLTB_Platforms` | `profile_platforms` | Platform list (when present) |
| `Score_HLTB_100` | `review_score` | HLTB score normalized to 0–100 (when present) |

Other useful attributes commonly present:

- `profile_dev`, `profile_platform`, `release_world`, `rating` (varies by library version/data)
- `game_alias` (aliases), `game_web_link` (URL), `profile_platforms` (platform list)

Note: the project caches the full HLTB result object payload (JSON-serialized) under `data/cache/`
even though only a small subset is written to CSV. This lets you add new derived fields later
without re-fetching.

## Wikidata

- API docs: https://www.wikidata.org/w/api.php (MediaWiki API)

- **ID**: `Wikidata_QID` (e.g. `Q123`)
- **Search**: `wbsearchentities`
- **Details**: `wbgetentities`

Caching notes:
- Search responses are cached under `by_query`.
- When provider-backed IDs exist (e.g. `Steam_AppID`), an additional resolver uses Wikidata SPARQL and caches the mapping under `by_hint` (external-id lookup → QID).
- Full entities are cached under `by_id` keyed by `Wikidata_QID`.
- Linked-entity labels (developer/publisher/platform/etc) are cached under `labels`.

Current output columns:

| CSV column | Path | Description |
|---|---|---|
| `Wikidata_QID` | `id` | Wikidata entity id |
| `Wikidata_Label` | `labels.en.value` | English label |
| `Wikidata_Description` | `descriptions.en.value` | English description |
| `Wikidata_ReleaseYear` | `claims.P577[*].time` | Publication date year (best effort) |
| `Wikidata_ReleaseDate` | `claims.P577[*].time` | Publication date (best effort, YYYY-MM-DD) |
| `Wikidata_Developers` | `claims.P178` | JSON array of developer labels |
| `Wikidata_Publishers` | `claims.P123` | JSON array of publisher labels |
| `Wikidata_Platforms` | `claims.P400` | Platforms (labels resolved) |
| `Wikidata_Series` | `claims.P179` | Series/franchise (labels resolved) |
| `Wikidata_Genres` | `claims.P136` | Genres (labels resolved) |
| `Wikidata_InstanceOf` | `claims.P31` | Instance-of classes (labels resolved) |
| `Wikidata_EnwikiTitle` | `sitelinks.enwiki.title` | English Wikipedia article title |
| `Wikidata_Wikipedia` | `sitelinks.enwiki.title` | English Wikipedia URL |
| `Wikidata_WikipediaPage` | Wikipedia summary API | Canonical enwiki page URL |
| `Wikidata_WikipediaSummary` | Wikipedia summary API | Short summary extract (truncated for CSV readability) |
| `Wikidata_WikipediaThumbnail` | Wikipedia summary API | Thumbnail URL (when present) |

Additional derived-from-Wikidata columns (official Wikimedia APIs, cached):

- **Wikipedia Pageviews API**: https://wikitech.wikimedia.org/wiki/Analytics/AQS/Pageviews
  - This project fetches a single 365-day daily series per `Wikidata_EnwikiTitle` and derives the
    30/90-day sums locally to reduce requests.
  - When adding new output columns, regenerate provider outputs (default `enrich --clean-output`)
    so derived columns are computed from cached provider payloads; the project does not try to
    “backfill” older `Provider_*.csv` files in place.
- **Wikipedia Summary API**: https://en.wikipedia.org/api/rest_v1/
  - Used to fetch short extracts and canonical page URLs for quick manual verification.

| CSV column | Source | Description |
|---|---|---|
| `Wikidata_Pageviews30d` | Pageviews API | Sum of pageviews over last 30 days (enwiki) |
| `Wikidata_Pageviews90d` | Pageviews API | Sum of pageviews over last 90 days (enwiki) |
| `Wikidata_Pageviews365d` | Pageviews API | Sum of pageviews over last 365 days (enwiki) |
| `Wikidata_PageviewsFirst30d` | Pageviews API | Sum of pageviews over the first 30 days since release (enwiki; only computed when Wikidata has a full `YYYY-MM-DD` release date and it’s within Pageviews coverage, i.e. release date >= 2015-07-01) |
| `Wikidata_PageviewsFirst90d` | Pageviews API | Sum of pageviews over the first 90 days since release (enwiki; only computed when Wikidata has a full `YYYY-MM-DD` release date and it’s within Pageviews coverage, i.e. release date >= 2015-07-01) |
