# How the enrichment works

This project enriches a “personal catalog” CSV (from MyVideoGameList) with metadata from multiple providers, then merges everything into a single output CSV and generates a validation report.

## Overview

1. Read the input CSV (must contain a `Name` column).
2. Run providers (RAWG, IGDB, Steam→SteamSpy, HLTB).
3. Each provider:
   - searches by name (fuzzy match),
   - optionally fetches details by ID (provider-specific),
   - extracts a stable subset of fields into `data/output/Games_<Provider>.csv`,
   - caches results to avoid re-fetching on re-runs.
4. Merge all provider CSVs into `data/output/Games_Final.csv`.
5. Generate `data/output/Validation_Report.csv` to help spot mismatches and suggest canonical titles.

Providers run in parallel at the CLI level, but each provider client itself is synchronous (no async in clients).

## Matching (search-by-name)

When a provider does not already have a cached ID for a given `Name`, it performs a search using the raw `Name` text and chooses the best match using fuzzy scoring.

- Minimum acceptance threshold: ~65%.
- If the best match is not a perfect 100%, a `WARNING` is logged so you can review borderline matches.
- If no acceptable match is found, the provider logs a `WARNING` and stores a “negative cache” entry so the same miss does not re-query repeatedly.

Because MyVideoGameList titles can be non-standard, the search step is treated as “best effort”; the validation report is the main tool for spotting when providers returned different games for the same row.

## Provider IDs, details, and caching

Each provider keeps two cache layers (in JSON under `data/cache/`):

- `by_name`: maps normalized names to a provider ID (or `null` if not found).
- `by_id`: maps provider IDs to extracted/enriched row fields.

On re-runs:
- if `by_id` already has the game’s ID, the provider skips searching by name entirely;
- otherwise it uses `by_name` (if present) to avoid repeating the search step.

Providers that don’t expose stable IDs (or where the library doesn’t provide one) fall back to caching by normalized name.

## Per-provider flow

### RAWG

- Search: `GET /api/games?search=...`
- (Optional) details are available via `GET /api/games/{id}`, but the current pipeline extracts from the best-match object.
- ID: `RAWG_ID`

### IGDB

- OAuth token: `POST https://id.twitch.tv/oauth2/token`
- Game query: `POST /v4/games`
- The client uses field expansion so each game requires a single `/v4/games` request (excluding OAuth).
- ID: `IGDB_ID`
- Steam cross-check: `external_games.external_game_source == 1` → `external_games.uid` is stored as `IGDB_SteamAppID` when present.

### Steam and SteamSpy

- Steam search: `GET https://store.steampowered.com/api/storesearch?term=...&l=english&cc=US`
- Steam details: `GET https://store.steampowered.com/api/appdetails?appids=<appid>&l=english`
- SteamSpy details: `GET https://steamspy.com/api.php?request=appdetails&appid=<appid>`
- SteamSpy starts as soon as Steam discovers an appid (streaming queue), it does not wait for Steam to finish the full file.

### HowLongToBeat (HLTB)

- Uses `howlongtobeatpy` library search and extracts playtime fields.

## Merge behavior (duplicate names)

If the input contains duplicate `Name` values (e.g., the same game on two platforms), the merge step avoids cartesian growth by merging using `(Name, occurrence_index)` instead of `Name` alone.

The occurrence index is the per-name row number within the file and is preserved across provider outputs because they originate from the same input ordering.

## Validation report

The validation report (`Validation_Report.csv`) is meant to help answer:

- “Did providers fetch the same game for this row?”
- “Which provider is most likely wrong?”
- “What canonical title should I rename this entry to?”

Key concepts:

- `SuggestedCanonicalTitle` / `SuggestedCanonicalSource`: the best “canonical” name based on provider consensus and source preference.
- `SuggestedRenamePersonalName`: strict/high-confidence rename suggestion (small list).
- `ReviewTitle`: broader “review this title” list (larger list), with `ReviewTitleReason`.
- `YearDisagree_RAWG_IGDB` is considered high-signal; Steam year drift is tracked separately because Steam often represents ports/remasters.

