# How the enrichment works

This project enriches a “personal catalog” CSV (from MyVideoGameList) with metadata from multiple providers, then merges everything into a single output CSV.

## Recommended workflow (spreadsheet round-trip)

Keep one source-of-truth catalog, and treat enriched output as an editable view:

1. `import` → create/update `data/input/Games_Catalog.csv` (adds stable `RowId`, proposes provider IDs, writes diagnostics).
2. Optional `resolve` → mutate pins only when explicitly requested (repin-or-unpin) based on diagnostics.
3. `enrich` → generate provider outputs + `data/output/Games_Enriched.csv` from pinned IDs.
4. Optional user edits → edit `data/output/Games_Enriched.csv` (user fields and/or provider ID pins).
5. `sync` → copy user-editable fields and provider ID pins back into `data/input/Games_Catalog.csv`.
6. `enrich` again → regenerate public/provider columns after pin fixes or user edits.

`Games_Catalog.csv` is the only file that should be treated as durable program input across runs.

## Overview

1. Read the input CSV (must contain a `Name` column).
2. Import (pin provider IDs + diagnostics) into `data/input/Games_Catalog.csv`.
3. Optionally resolve (third pass): conservative repin-or-unpin + one-shot repin attempts.
4. Run providers (RAWG, IGDB, Steam→SteamSpy, HLTB, Wikidata).
3. Each provider:
   - searches by name (fuzzy match),
   - optionally fetches details by ID (provider-specific),
   - extracts a stable subset of fields into `data/output/Provider_<Provider>.csv`,
   - caches results to avoid re-fetching on re-runs.
4. Merge all provider CSVs into `data/output/Games_Enriched.csv`.
5. Review import diagnostics in `data/input/Games_Catalog.csv` (`ReviewTags`, `MatchConfidence`) and pin IDs as needed.

Providers run in parallel at the CLI level, but each provider client itself is synchronous (no async in clients).

Logging defaults to `INFO` with periodic progress lines (e.g. `25/958`); per-row provider “Processing …” lines are `DEBUG` (enable with `--debug`).

`Games_Enriched.csv` includes derived identity/consistency helpers such as:

- `ContentType`: best-effort classification (`base_game|dlc|expansion|port|collection|demo|soundtrack`) from provider signals (e.g. Steam store type + IGDB relationships).
- `ContentType_ConsensusProviders`, `ContentType_SourceSignals`, `ContentType_Conflict`: supporting provenance/consensus fields to help spot edition/DLC mismatches without adding many extra boolean columns.
- `HasDLCs`, `HasExpansions`, `HasPorts`: best-effort “this title has related DLC/expansions/ports” flags derived from IGDB relationship lists (filters out soundtrack-like DLC entries).
- `Replayability_100`: best-effort replayability heuristic (0–100) using game-mode/style signals (e.g. MP/coop/roguelike/systemic tags + HLTB time ratios).
- `ModdingSignal_100` and `HasWorkshop`: best-effort modding/UGC proxy using Steam store categories (Workshop/level editor/mod tools when present).

## Matching (search-by-name)

When a provider does not already have a cached ID for a given `Name`, it performs a search using the raw `Name` text and chooses the best match using fuzzy scoring.

- Minimum acceptance threshold is configured in code (currently 65%).
- If you provide a `YearHint` column (e.g. `1999`), it is used as a soft disambiguation signal during matching.
- Prefer keeping `Name` year-free and putting disambiguation years in `YearHint` instead of suffixing titles with `"(YYYY)"`.
- If the best match is not a perfect 100%, a `WARNING` is logged so you can review borderline matches.
- If no acceptable match is found, the provider logs a `WARNING` and stores a “negative cache” entry so the same miss does not re-query repeatedly.
- Request failures (no response) are not negative-cached, to avoid poisoning the cache when offline.

Because MyVideoGameList titles can be non-standard, the search step is treated as “best effort”; import diagnostics are the main tool for spotting when providers returned different games for the same row.

## Import warnings (review, no auto-unpin)

The importer is conservative about warnings:

- Any non-100% match emits a `WARNING`.
- When providers disagree on identity (title/year/platform/genre), the importer adds compact tags
  in `ReviewTags` and a `MatchConfidence` level so you can review and pin IDs manually.

## Optional resolve pass (auto-resolve, explicit)

The `resolve` command is an explicit third pass (off by default) that can:

- re-run diagnostics and, for pins that are tagged as likely wrong (strict-majority consensus + outlier),
  attempt a single conservative repin; if no safe repin candidate exists, it unpins to avoid silently
  enriching the wrong game,
- attempt a single conservative repin per affected provider using the majority provider title and
  cached aliases (Wikidata aliases + IGDB alternative names),

This keeps the default `import` predictable and avoids extra provider requests unless you opt in.

Resolve is **dry-run by default**; pass `--apply` to write changes back to the catalog CSV.

## What’s implemented (quick index)

- Third pass auto-resolve: `python run.py resolve` (conservative repin-or-unpin)
- Wikidata provider-backed resolver: uses SPARQL external IDs (`data/cache/wikidata_cache.json:by_hint`)
- Review-aid fields:
  - Steam: `Steam_URL`, `Steam_Website`, `Steam_ShortDescription`, `Steam_StoreType`
  - RAWG: `RAWG_Website`, `RAWG_DescriptionRaw`, `RAWG_Genres`, `RAWG_ESRB`
  - IGDB: `IGDB_Summary`, `IGDB_Websites`, `IGDB_ParentGame`, `IGDB_VersionParent`, `IGDB_DLCs`, `IGDB_Expansions`, `IGDB_Ports`
- Validation signals:
  - `missing_ok:*` for expected Steam/SteamSpy misses (non-PC platform union)
  - `store_type_not_game:<type>`
  - `genre_outlier:<provider>` / `genre_no_consensus` + `Genres` / `GenreIntersection` columns

## Provider-specific search notes

In practice, providers react differently to years embedded in the search string:

- Steam: the store search endpoint is sensitive to punctuation/extra tokens; `"(YYYY)"` often hurts recall, and it does not expose a release year to use for filtering. Prefer a clean `Name` + `YearHint`.
- IGDB: searches tend to work better without a trailing `"(YYYY)"`. When `YearHint` is present, the client first tries a narrow release-date window (±1 year), then falls back to an unfiltered search.
- RAWG: generally tolerant, but year tokens can still skew fuzzy scoring; `YearHint` helps break ties when multiple candidates share a similar name.
- HLTB: uses `HLTB_ID` when pinned; otherwise it searches by query and strips a trailing `"(YYYY)"` when needed.
  - HLTB only uses a small set of query variants and stops early on a high-confidence match.
  - As a last resort (only if all other variants return no candidates), it tries lower/upper-case variants for stylized titles.

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
  - IGDB `external_games` (Steam uid), e.g.:
    - `external_games: [{ external_game_source: 1, uid: "620" }]`, or
    - `external_games: [{ category: "steam", uid: "620" }]`
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

All current providers in this project expose a stable ID that is used for `by_id` caching:
`RAWG_ID`, `IGDB_ID`, `Steam_AppID`, `HLTB_ID` (and SteamSpy uses `Steam_AppID`).

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
- Focused review list (`data/output/Review_TopRisk.csv`)
  - Generated on-demand via `python run.py review` (uses `Games_Catalog.csv` and optionally `Games_Enriched.csv` for extra context like Wikipedia summaries/links).
- When diagnostics are enabled, the import also writes:
    - `ReviewTags`: compact tags (missing providers, low fuzzy scores, cross-provider outliers, plus a few high-signal Steam-specific checks like `steam_series_mismatch` and `steam_appid_disagree:*`).
      - Consensus/outliers: `provider_consensus:*`, `provider_outlier:*`, `provider_no_consensus`
      - Metadata outliers: `year_outlier:*`, `platform_outlier:*` (and `*_no_consensus`)
      - Actionable rollups: `likely_wrong:*`, `ambiguous_title_year`
      - Dev/pub checks: `developer_disagree`, `publisher_disagree` (optionally with `<kind>_outlier:<provider>` when a strict-majority source disagrees)
    - `MatchConfidence`: `HIGH` / `MEDIUM` / `LOW` (missing providers are typically `MEDIUM`; strong disagreement signals are `LOW`).

- Merge output (`data/output/Games_Enriched.csv`)
  - Written after all selected providers finish.
- After merging, diagnostic/eval columns are dropped so the enriched CSV stays focused on
  user-editable fields + provider enrichment fields.
- Enriched outputs include a small set of provider score fields normalized to 0–100 where possible:
  - `Score_RAWG_100`, `Score_IGDB_100`, `Score_SteamSpy_100`, `Score_HLTB_100`
- Enriched outputs also include a small set of computed “signals” (Phase 1):
  - Reach: `Reach_SteamSpyOwners_*` (parsed from SteamSpy owners ranges)
  - Reach (cross-platform-ish): RAWG `added` / `added_by_status.*` is incorporated into `Reach_Composite` when present
  - Reach (critics): `Reach_IGDBAggregatedRatingCount` (when IGDB aggregated ratings exist)
  - Ratings: `CommunityRating_Composite_100`, `CriticRating_Composite_100` (uses Steam/RAWG Metacritic + IGDB aggregated rating when present)
  - Production: `Production_Tier` (optional; driven by `data/production_tiers.yaml` when present)
  - Now (current interest): `Now_SteamSpyPlayers2Weeks`, `Now_SteamSpyPlaytimeAvg2Weeks`, `Now_SteamSpyPlaytimeMedian2Weeks`
  - Notes/limitations:
    - These signals are best-effort and intentionally platform-biased toward Steam/PC and Wikipedia availability.
    - We avoid scraping; “now” coverage for non-Steam games is limited.
    - Wikidata numeric “facts” (sales/budget/awards) are too sparse/inconsistent to treat as first-class columns.

## Production tier mapping (AAA/AA/Indie)

`Production_Tier` is driven by a simple, project-specific YAML mapping file (local, git-ignored):

- `data/production_tiers.yaml` maps publisher/developer names to a coarse tier (`AAA` / `AA` / `Indie`).
- Start by copying the checked-in baseline:
  - `cp data/production_tiers.example.yaml data/production_tiers.yaml`
- To generate a focused TODO list from an existing enriched CSV (offline; no network):
  - `./.venv/bin/python run.py collect-production-tiers data/output/Games_Enriched.csv --only-missing --out data/production_tiers.todo.yaml --base data/production_tiers.yaml`

Notes:
- The collector scans all available `*_Publishers` / `*_Developers` columns (Steam/IGDB/RAWG/Wikidata) in the
  enriched CSV.
- `data/production_tiers.yaml` is intentionally simple and stable (tiers only, no catalog-dependent counts).
- The TODO file is catalog-dependent and includes `count`/`examples` to help prioritize.

Recommended workflow (keep enrich deterministic and read-only):

1. Run `enrich` to populate `*_Publishers` / `*_Developers`.
2. Run `collect-production-tiers` to generate/update the TODO list (only entities missing tiers):
   - `./.venv/bin/python run.py collect-production-tiers data/output/Games_Enriched.csv --only-missing --out data/production_tiers.todo.yaml --base data/production_tiers.yaml`
3. Curate tiers in `data/production_tiers.yaml` (`AAA` / `AA` / `Indie`), then re-run `enrich` to recompute `Production_Tier`.

## Logs (how to read them)

- Each run writes a separate log file under `<run-dir>/logs/` (default `data/logs/`).
- Providers emit `Cache stats` summary lines at completion (hits vs fetches, including negative-cache counts).
- When requests fail after retries (e.g. no internet), the logs include distinct error markers:
  - `[NETWORK] ...` (connection/timeout/SSL)
  - `[HTTP] ...` (non-2xx responses)
  - These are intentionally separate from provider “Not found ...” warnings.
- When a provider needs network (cache miss) and the network is unavailable, the run fails fast with a clear error instead of silently turning the failure into “not found”.
- When providers see `429 Too Many Requests`, retries honor `Retry-After` when available; cache stats append `429=... retries=... backoff_ms=...` only when it occurred.
- Provider JSON cache writes are throttled to avoid repeatedly rewriting large files; caches are flushed at process exit. Large caches (Wikidata + Wikipedia pageviews) use a longer interval than smaller caches.
- Provider HTTP clients reuse a persistent `requests.Session()` per provider instance (connection pooling / keep-alive). This is intended to improve performance; request retries handle stale connections.

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

### Wikidata

- Search: `GET https://www.wikidata.org/w/api.php?action=wbsearchentities&search=...`
- Details: `GET https://www.wikidata.org/w/api.php?action=wbgetentities&ids=...`
- ID: `Wikidata_QID`
- Provides cross-platform identity context: canonical label/description, release year, developer/publisher, platforms, series, genres, and an English Wikipedia link.
- Wikipedia signals (summary + pageviews) are fetched as soon as the Wikidata entity provides an `enwiki` title (pipelined; does not wait for all rows/QIDs).
- Linked entity labels (developers/publishers/platforms/genres/etc) are fetched via `wbgetentities&props=labels` in batches and cached.
  - During `import`, existing `Wikidata_QID` values are prefetched in bulk so warm-cache imports don’t do per-row Wikidata requests.

## Merge behavior

All merges are performed using `RowId`. Duplicate `Name` values are fine as long as `RowId` values are unique.

## Validation vs import diagnostics

The primary review surface is **import diagnostics** in `data/input/Games_Catalog.csv`:

- `ReviewTags`: compact tags describing why the row may need review.

Validation (`python run.py enrich --validate` or `python run.py validate`) is read-only and adds
additional cross-provider checks once provider metadata is available, including:

- platform and year consensus/outliers
- genre consensus/outliers (RAWG/IGDB/Steam)
- missing-provider severity (`missing_ok:*` for expected misses)
- `MatchConfidence`: `HIGH` / `MEDIUM` / `LOW`.

In addition, you can generate a read-only **validation report** after enrichment:

- `enrich --validate` writes `data/output/Validation_Report.csv`
- `validate` can generate the same report from an existing enriched CSV

Validation focuses on cross-provider consistency checks and is not treated as a source of truth.
The report uses a `ValidationTags` column that largely mirrors the same tagging vocabulary as
`ReviewTags` (consensus/outliers and actionable rollups), plus some enrichment-only tags (e.g.
`steam_dlc_like`).
