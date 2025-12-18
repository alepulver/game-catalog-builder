# Provider Enhancements Proposal

This proposal summarizes high-value enhancements based on:

- `docs/providers/provider-fields.md`
- `docs/providers/provider-field-reference.md`
- Current cross-provider signals observed during `import --diagnostics` in `data/input/Games_Catalog.csv` (`ReviewTags`, `MatchConfidence`).

The goal is to improve: (1) match review speed, (2) mismatch detection quality, and (3) usefulness of the enriched CSV—while keeping transforms minimal and avoiding column clutter.

## Prioritized (implement first)

### 1) Use cross-provider IDs (highest ROI, low complexity)

Steam is currently the biggest source of ambiguity because store search doesn’t provide enough metadata to reliably disambiguate editions/demos/DLC/soundtracks without fetching details.

Proposal (already partially implemented):
- When `Steam_AppID` is empty, infer it from other providers when possible:
  - IGDB: `external_games` can include Steam as either:
    - `external_games: [{ external_game_source: 1, uid: "620" }]`, or
    - `external_games: [{ category: "steam", uid: "620" }]`
  - RAWG: `stores[].url` often contains `https://store.steampowered.com/app/<appid>/...`
- Use inferred IDs as:
  - a way to pin `Steam_AppID` without another fuzzy search, and/or
  - a cross-check that a chosen Steam app really corresponds to the same item.

Observed signal:
- `steam_appid_disagree:igdb` shows up frequently and is a strong “needs review” signal, but it also catches legitimate edition/port/VR cases. Treat it as `LOW` confidence by default, and consider downgrading it to `MEDIUM` when the titles are clearly the same game identity (edition tokens only).

### 2) Steam “wrong app type” guard (demo/DLC/soundtrack)

Problem:
- Steam `storesearch` can return entries that are not the base game (DLC, soundtrack, demo), and they can have a deceptively close name.
- We sometimes end up picking:
  - DLC packs instead of the game,
  - soundtracks (`... Soundtrack`),
  - demos (`... Demo`),
  - VR variants (e.g. `Bulletstorm VR`) when you wanted the original.

Proposal:
- When the chosen Steam candidate is suspicious (low match score, series mismatch, or appid disagreement with IGDB):
  - Fetch `appdetails` for a small set of top candidates and **filter out non-`type=="game"`** items.
  - Prefer candidates whose `appdetails.name` is a clean match (edition tokens allowed), and penalize obvious DLC-like tokens (`demo`, `soundtrack`, `season pass`, etc.).
- This does not require new Steam endpoints (still `appdetails`), just smarter selection.

### 3) Series-number detection: refine only if false positives show up

Series/roman numerals are already a high-signal mismatch detector.

If false positives appear, add guards in series-number extraction to ignore “numeric brand” patterns:
- Leading-zero tokens like `007`
- `2K`/`2k` patterns (including `2k23`-style)
- Similar non-sequel numeric brands

### 4) Developer/publisher extraction + cross-checks

No new endpoints needed: Steam provides developer/publisher lists in `appdetails`.

Add extraction:
- `Steam_Developers` from `developers[]` (comma-separated)
- `Steam_Publishers` from `publishers[]` (comma-separated)

Add validation:
- `developer_disagree`, `publisher_disagree` tags based on normalized set overlap.

Outcome: dev/pub agreement becomes a strong identity signal when titles/years are ambiguous.

### 5) Keep diagnostics compact

Principle:
- Prefer a compact tag list (`ReviewTags`) + a confidence field (`MatchConfidence`) over dozens of boolean columns.

Notes:
- “Missing provider” should not automatically mean “bad match” (e.g. `missing_steam` is common and often expected).
- Cross-provider disagreements (`year_disagree`, `steam_series_mismatch`, `steam_appid_disagree:*`) are higher signal and should drive `LOW` confidence.

## High-value next-wave enhancements (minimal transforms)

### A) “Review aid” fields (best ROI for match verification)

Add fields that let a user quickly confirm “is this the right game?” without leaving the spreadsheet:

- Short description / summary:
  - Steam: `short_description` or `about_the_game`
  - IGDB: `summary`
  - RAWG: `description_raw`
- Provider URLs:
  - Steam: `website`
  - RAWG: `website`
  - IGDB: `websites.url` (via expanded fields)
- Developer/publisher (identity anchors):
  - Steam: `developers[]`, `publishers[]`
  - IGDB: `involved_companies.company.name` + `involved_companies.developer/publisher`
  - RAWG: `developers[].name`, `publishers[].name`

### B) Better edition / DLC / bundle handling

- Steam classification:
  - `type` plus categories can classify store items as `game|dlc|soundtrack|demo` and warn if not a full game.
- IGDB relationships (field expansion in the same call):
  - `parent_game.name`, `version_parent.name`, `dlcs.name`, `expansions.name`, `ports.name`
  - Helps detect base-vs-edition mismatches across providers.

### C) Richer genre/tag metadata for filtering and identity checks

- IGDB (expanded fields in one call):
  - `keywords.name`, `collections.name`, `category`, `status`
  - `age_ratings.*`
  - `rating`, `aggregated_rating`, `total_rating` (+ counts)
- RAWG:
  - `esrb_rating.name`
  - full `genres[]` (not just the first two)
  - optionally `tags[]` (already extracted)

### D) Popularity/interest signals (prioritization)

- Steam:
  - already: `recommendations.total`
  - add: `metacritic.score`, `achievements.total`
- SteamSpy:
  - `positive`, `negative`
  - tag distribution `tags{tag:count}`
  - `average_2weeks`, `median_2weeks`
  - `price`, `initialprice`, `discount`
- RAWG:
  - `added`, `added_by_status.*`, `playtime`, `ratings[]` breakdown

### E) HLTB quality improvements

Keep the current times, but consider extracting for cross-checking:
- `release_world` (year)
- `profile_platform` (platform string)
- optionally “all styles” / coop time (observed in examples)

## Validation enhancements enabled by these fields

Add tags (to `ReviewTags`) without creating many new columns:
- `developer_disagree`
- `publisher_disagree`
- `age_rating_disagree`
- `store_type_not_game`
- `edition_parent_mismatch`
- `summary_mismatch` (heuristic)

Also improve culprit inference:
- When title/year disagree, dev/pub agreement can better identify which provider match is likely wrong than fuzzy score alone.

## Optional improvement: missing-provider severity

Use platform info to downgrade expected misses:
- If a row does not include `pc` in its platform intersection/union, treat `missing:Steam` and `missing:SteamSpy` as informational rather than review-worthy.
