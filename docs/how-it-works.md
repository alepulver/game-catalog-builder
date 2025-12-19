# How the enrichment works

This project enriches a “personal catalog” CSV (from MyVideoGameList) with metadata from multiple providers, then merges everything into a single output CSV.

## Overview

1. Read the input CSV (must contain a `Name` column).
2. Run providers (RAWG, IGDB, Steam→SteamSpy, HLTB).
3. Each provider:
   - searches by name (fuzzy match),
   - optionally fetches details by ID (provider-specific),
   - extracts a stable subset of fields into `data/output/Provider_<Provider>.csv`,
   - caches results to avoid re-fetching on re-runs.
4. Merge all provider CSVs into `data/output/Games_Enriched.csv`.
5. Review import diagnostics in `data/input/Games_Catalog.csv` (`ReviewTags`, `MatchConfidence`) and pin IDs as needed.

Providers run in parallel at the CLI level, but each provider client itself is synchronous (no async in clients).

## Matching (search-by-name)

When a provider does not already have a cached ID for a given `Name`, it performs a search using the raw `Name` text and chooses the best match using fuzzy scoring.

- Minimum acceptance threshold is configured in code (currently 65%).
- If you provide a `YearHint` column (e.g. `1999`), it is used as a soft disambiguation signal during matching.
- Prefer keeping `Name` year-free and putting disambiguation years in `YearHint` instead of suffixing titles with `"(YYYY)"`.
- If the best match is not a perfect 100%, a `WARNING` is logged so you can review borderline matches.
- If no acceptable match is found, the provider logs a `WARNING` and stores a “negative cache” entry so the same miss does not re-query repeatedly.
- Request failures (no response) are not negative-cached, to avoid poisoning the cache when offline.

Because MyVideoGameList titles can be non-standard, the search step is treated as “best effort”; import diagnostics are the main tool for spotting when providers returned different games for the same row.

## Provider-specific search notes

In practice, providers react differently to years embedded in the search string:

- Steam: the store search endpoint is sensitive to punctuation/extra tokens; `"(YYYY)"` often hurts recall, and it does not expose a release year to use for filtering. Prefer a clean `Name` + `YearHint`.
- IGDB: searches tend to work better without a trailing `"(YYYY)"`. When `YearHint` is present, the client first tries a narrow release-date window (±1 year), then falls back to an unfiltered search.
- RAWG: generally tolerant, but year tokens can still skew fuzzy scoring; `YearHint` helps break ties when multiple candidates share a similar name.
- HLTB: uses `HLTB_ID` when pinned; otherwise it searches by query and strips a trailing `"(YYYY)"` when needed.

### Base game vs editions

For search/matching, a “base game” title and an edition/remaster are often acceptable equivalents for pinning:

- Prefer: base game or “complete/definitive/GOTY/remastered” editions (still the same game identity).
- Avoid: sequels, support packs, soundtracks, demos, season passes, DLC-only entries.

Limitations:
- Some “base games” have provider-specific canonical titles/subtitles (e.g. IGDB may return a long subtitle
  while Steam/RAWG use a shorter name). In those cases, do not rename your original `Name` just to satisfy
  one provider; instead, fix it at the pinning stage by editing the provider ID columns in
  `data/input/Games_Catalog.csv`.

Implementation notes:
- Steam selection uses appdetails (`type` + release date) to avoid DLC/music and to break ties; when the query has no sequel number, it strongly prefers a base/edition match over a sequel match when possible.
- Fuzzy matching treats common edition tokens (e.g. “GOTY”, “Enhanced”, “Complete”) as ignorable when one title is a strict superset of the other.
- If `Steam_AppID` is empty but `IGDB_ID` (or `RAWG_ID`) is pinned, the importer may infer a Steam AppID from:
  - IGDB `external_games` (Steam uid), or
  - RAWG store URLs containing `/app/<appid>`.
  Inferred Steam AppIDs are validated via Steam appdetails: if the inferred appid is not `type=game`,
  it is ignored and the importer falls back to name-based Steam search.

## Provider IDs, details, and caching

Each provider caches only **stable or raw** data (in JSON under `data/cache/`):

- `by_query`: query key → lightweight candidate list (IDs + display names + minimal date info).
- `by_id`: provider ID → raw provider payload (details response or expanded search hit).

The project intentionally does **not** cache “name → chosen ID” as a single pinned selection.
Selection heuristics can change over time, so on re-runs the program re-selects from cached
`by_query` candidates (no network) unless you explicitly pin an ID in `data/input/Games_Catalog.csv`.

## Configuration (static for now)

Most “magic numbers” (match thresholds, rate limits, batch sizes, retry settings) live in
`game_catalog_builder/config.py`. They are currently static constants but are grouped so we can
later make them configurable without chasing scattered literals.

Providers that don’t expose stable IDs (or where the library doesn’t provide one) fall back to
caching raw data under a synthetic key (e.g. `name:<normalized>`), but the search results are still
cached by query.

## Persistence (CSV + cache writes)

The tool is designed to be resumable, so it persists both caches and intermediate CSVs frequently:

- Provider caches (`data/cache/*.json`)
  - Written immediately after a successful “search by query” response is received (including empty
    results for negative caching).
  - Written immediately after a successful “fetch by id/details” response is received.
  - Request failures (no response / exception) are not negative-cached.

- Provider output CSVs (`data/output/Provider_<Provider>.csv`)
  - Written incrementally every 10 processed rows for that provider.
  - Final file always includes only `RowId`, `Name`, plus that provider’s prefixed columns
    (e.g. `IGDB_*`).

- Import output (`data/input/Games_Catalog.csv`)
  - Written once at the end of the import command.
  - During import, HLTB matching progress is also checkpointed every 25 processed rows because HLTB
    can be slow.
- When diagnostics are enabled, the import also writes:
    - `ReviewTags`: compact tags (missing providers, low fuzzy scores, year/platform drift, and a few high-signal Steam-specific checks like `steam_series_mismatch` and `steam_appid_disagree:*`).
    - `MatchConfidence`: `HIGH` / `MEDIUM` / `LOW` (missing providers are typically `MEDIUM`; strong drift signals like year/platform disagreements are `LOW`).

- Merge output (`data/output/Games_Enriched.csv`)
  - Written after all selected providers finish.
- After merging, diagnostic/eval columns are dropped so the enriched CSV stays focused on
  user-editable fields + provider enrichment fields.
- Enriched outputs include a small set of provider score fields normalized to 0–100 where possible:
  - `Score_RAWG_100`, `Score_IGDB_100`, `Score_SteamSpy_100`, `Score_HLTB_100`

## Logs (how to read them)

- Each run writes a separate log file under `data/logs/` (or `data/experiments/logs/` for experiment inputs).
- Providers emit `Cache stats` summary lines at completion (hits vs fetches, including negative-cache counts).
- When requests fail after retries (e.g. no internet), the logs include distinct error markers:
  - `[NETWORK] ...` (connection/timeout/SSL)
  - `[HTTP] ...` (non-2xx responses)
  - These are intentionally separate from provider “Not found ...” warnings.

## Per-provider flow

### RAWG

- Search: `GET /api/games?search=...`
- (Optional) details are available via `GET /api/games/{id}`, but the current pipeline extracts from the best-match object.
- ID: `RAWG_ID`

### IGDB

- OAuth token: `POST https://id.twitch.tv/oauth2/token`
- Game query: `POST /v4/games`
- The client uses field expansion so each game requires a single `/v4/games` request (excluding OAuth).
- When `YearHint` is present, the search first tries a narrow release-date window (±1 year) to avoid sequels/remakes and upcoming placeholders; it falls back to an unfiltered search if nothing matches.
- ID: `IGDB_ID`
- Steam cross-check: `external_games.external_game_source == 1` → `external_games.uid` is stored as `IGDB_SteamAppID` when present.

### Steam and SteamSpy

- Steam search: `GET https://store.steampowered.com/api/storesearch?term=...&l=english&cc=US`
- Steam details: `GET https://store.steampowered.com/api/appdetails?appids=<appid>&l=english&cc=us`
- SteamSpy details: `GET https://steamspy.com/api.php?request=appdetails&appid=<appid>`
- SteamSpy starts as soon as Steam discovers an appid (streaming queue), it does not wait for Steam to finish the full file.

### HowLongToBeat (HLTB)

- Uses `howlongtobeatpy`:
  - When `HLTB_ID` is present, it uses the library’s `search_from_id(id)` to avoid ambiguity.
  - Otherwise it searches by name (optionally using `HLTB_Query`) and extracts playtime fields.
- The project caches the full HLTB result object payload (JSON-serialized) so additional derived
  fields can be added later without re-fetching.

## Merge behavior (duplicate names)

If the input contains duplicate `Name` values (e.g., the same game on two platforms), the merge step avoids cartesian growth by merging using `(Name, occurrence_index)` instead of `Name` alone.

The occurrence index is the per-name row number within the file and is preserved across provider outputs because they originate from the same input ordering.

## Validation vs import diagnostics

The primary review surface is **import diagnostics** in `data/input/Games_Catalog.csv`:

- `ReviewTags`: compact tags describing why the row may need review.
- `MatchConfidence`: `HIGH` / `MEDIUM` / `LOW`.

In addition, you can generate a read-only **validation report** after enrichment:

- `enrich --validate` writes `data/output/Validation_Report.csv`
- `validate` can generate the same report from an existing enriched CSV

Validation focuses on cross-provider consistency checks and is not treated as a source of truth.
