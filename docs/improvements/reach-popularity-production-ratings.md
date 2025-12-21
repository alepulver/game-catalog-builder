# Reach, Popularity, Production Value, and Ratings (Proposal)

This proposal defines four independent signals for each catalog row:

1) **Lifetime commercial reach** (“how many people owned/engaged with it”)
2) **Current popularity** (“how many people play it now / recently”)
3) **Production value** (“AAA vs indie scale”)
4) **Ratings** (community vs critics, separately)

Key design goals:
- Keep raw inputs and computed outputs separate (auditability).
- Prefer ID-based joins. Use alternate titles only as search helpers.
- Never overwrite user data; enrichment columns are derived.
- Keep platform bias explicit (Steam ≠ console reach).

## 1) Lifetime Commercial Reach

### What it means
An estimate of how widely the game was acquired and/or engaged with over its lifetime.

### Recommended primary sources (Steam-heavy)
- **SteamSpy** owners range keyed by `Steam_AppID`
  - Best single “lifetime reach” proxy when available.
- **Steam store reviews** keyed by `Steam_AppID`
  - `Steam_ReviewCount` is a strong engagement proxy but not ownership.
- **RAWG** and **IGDB** vote counts as secondary proxies
  - Useful for non-Steam games, but coverage is variable.
- **Wikipedia pageviews** (official Wikimedia Pageviews API) keyed by Wikidata `enwiki` sitelink
  - Cross-platform “interest” proxy (not sales); helps for console/mobile exclusives.
Wikidata structured “facts” (like sales/budget/awards) exist for some items but are too sparse and
inconsistent to include as first-class CSV columns right now; keep them as “future optional” only.

### Proposed raw columns
- `Reach_SteamSpyOwners_Low`, `Reach_SteamSpyOwners_High`, `Reach_SteamSpyOwners_Mid`
- `Reach_SteamReviews` (reuse `Steam_ReviewCount`)
- `Reach_RAWGRatingsCount` (reuse `RAWG_RatingsCount`)
- `Reach_IGDBRatingCount` (reuse `IGDB_RatingCount`)
- `Wikidata_Pageviews365d` (Wikipedia pageviews, 365d sum)

### Proposed computed columns
- `Reach_Composite` (optional)
  - Implemented: derived 0–100 score using a weighted blend of log-scaled count proxies:
    - SteamSpy owners mid (highest weight when present)
    - Steam review count
    - RAWG ratings count
    - IGDB rating count
    - IGDB aggregated rating count (lowest weight)
    - Wikipedia pageviews in last 365 days (low weight; cross-platform interest proxy)
  - Intended for ranking within the catalog (not an estimate of unit sales).
- `Reach_SourceCoverage` (e.g. `steamspy+steam` / `rawg` / `igdb`)

### Additional data needed
- **None** if SteamSpy response already includes `owners` and we extract it.
- Otherwise: expand SteamSpy extraction (still same endpoint).

## 2) Current Popularity (“Now”)

### What it means
How actively the game is being played now or in the last ~2 weeks.

### Recommended sources
- **Steam current players** keyed by `Steam_AppID` (best for “now”, Steam-only)
  - If implemented: `Now_SteamPlayers_Current`
- **SteamSpy recent players** keyed by `Steam_AppID` (Steam-only)
  - If present in SteamSpy response: `players_2weeks`, `average_2weeks`, `median_2weeks`
- **Wikipedia pageviews (30/90d)** keyed by Wikidata `enwiki` sitelink (cross-platform interest proxy)
  - This is “interest”, not “players now”.

### Proposed raw columns
- `Now_SteamPlayers_Current` (optional)
- `Now_SteamSpyPlayers2Weeks` (optional)
- `Now_SteamSpyPlaytimeAvg2Weeks` / `Now_SteamSpyPlaytimeMedian2Weeks` (optional)
- `Wikidata_Pageviews30d` / `Wikidata_Pageviews90d` (Wikipedia pageviews, 30/90d sums)

### Release-window popularity proxy (optional; newer games only)
- If Wikidata has a usable publication date and pageviews history exists (pageviews start ~mid-2015),
  compute:
  - `Wikidata_PageviewsFirst30d`
  - `Wikidata_PageviewsFirst90d`
  - Derived convenience: `Launch_Interest_100` (log-scaled 0–100)
  - These are helpful for “launch attention”, but will often be blank for older titles.

### Proposed computed columns
- `Now_Composite` (optional) using whichever of the above exists.
  - Implemented: derived 0–100 score using a weighted blend of log-scaled recent activity proxies:
    - SteamSpy players_2weeks (preferred)
    - SteamSpy CCU (fallback / complement)

### Additional data needed
- Steam Web API call for current players (requires a key) OR a third-party dataset (SteamDB/SteamCharts-like).
  - If you want to avoid scraping/ToS risk, prefer the official Steam Web API.
  - Steam-only: cross-platform “now” is much harder without adding more providers.

## 3) Production Value (AAA / AA / Indie)

### What it means
An explicit classification of production scale. It is **not** popularity or quality.

### Recommended approach
Use a hybrid of:
- Extracted developer/publisher names (from existing provider payloads when available)
- A small curated mapping (manual, user-editable) of known entities to tiers

### Proposed raw columns
- `Steam_Developers`, `Steam_Publishers` (from Steam `appdetails`, no new endpoint)
- `RAWG_Developers`, `RAWG_Publishers` (from RAWG details, no new endpoint)
- `IGDB_Companies` split into developer/publisher where available (already requested in IGDB payload; may need extraction to CSV)

### Proposed computed columns
- `Production_Tier` (`AAA` / `AA` / `Indie` / `Unknown`)
- `Production_TierReason` (e.g. `publisher_map:EA`, `developer_map:id Software`, `unknown`)

### Additional data needed
- A local mapping file, e.g. `data/production_tiers.yaml`:
  - lists known publishers/developers and their tier.
  - allows project-specific overrides.
- Optionally, a cross-platform identity hub (see below) improves dev/pub normalization.

## 4) Ratings (Community vs Critics)

### What it means
Two separate views:
- **Community**: user reviews/ratings from platforms.
- **Critics**: curated critic scores (when available).

### Recommended sources (community)
- Steam: `Steam_ReviewPercent` + `Steam_ReviewCount`
- RAWG: `RAWG_Rating` + `RAWG_RatingsCount`
- IGDB: `IGDB_Rating` + `IGDB_RatingCount`
- HLTB: `Score_HLTB_100` (community-ish; niche audience)

### Recommended sources (critics)
- Steam `metacritic.score` (already extracted as `Steam_Metacritic` when present)
- RAWG `metacritic` (already extracted)
- IGDB `aggregated_rating` / `aggregated_rating_count` (critics; requested in the same IGDB call)

### Proposed computed columns
- `CommunityRating_Composite_100`
  - Weighted by vote counts (log-weighting to prevent a single source dominating).
- `CriticRating_Composite_100`
  - Combine Steam/RAWG Metacritic with IGDB aggregated rating when present.
- `Rating_SourceCoverage` (e.g. `steam+rawg+igdb` / `metacritic_only`)

### Additional data needed
- None for basic community composites (already in current outputs).
- None beyond extracting IGDB aggregated_rating fields (no new endpoints).

## Cross-platform identity (optional but high leverage)

If you want “Steam + cross-platform” to be coherent, add an identity hub:
- **Wikidata** `QID` for each game row (or optional pin column)
  - Provides canonical labels + aliases + release dates + platform lists + dev/pub.
  - Helps normalize titles and tie together provider IDs across ecosystems.

Proposed columns:
- `Wikidata_QID` (pinnable)
- `Wikidata_Label`, `Wikidata_Aliases`, `Wikidata_ReleaseYear`, `Wikidata_Platforms`
- `Wikidata_Developers`, `Wikidata_Publishers`

## Alternate-title assisted matching (guarded)

Alternate titles can be used to recover missing provider IDs, but must be conservative:
- Candidate alternate sources:
  - IGDB: `alternative_names[]`
  - HLTB: `game_alias` (comma-separated aliases)
  - RAWG: `alternative_names[]`
- Guardrails before auto-pinning:
  - Reject non-game types (Steam `appdetails.type != game`, etc.).
  - Require high score vs the personal name (e.g. ≥90).
  - Require metadata agreement with the majority (year/platform consensus) to avoid “same title, different game” traps.
  - Prefer suggesting candidate queries/IDs to the user rather than auto-pinning when uncertainty is high.

## Implementation plan (phased)

Phase 1 (no new external sources)
- Expand SteamSpy extraction for owners/recent players if available; parse owners range.
- Add reach/community composite computations using existing provider columns.
- Add dev/pub extraction from Steam appdetails (and optionally RAWG details) + a small `production_tiers.yaml`.

Phase 2 (cross-platform critic + identity)
- Use IGDB aggregated ratings for critic score and add Wikidata for identity context.
- Add optional Wikidata integration for identity and normalization (QID pin column).

Phase 3 (“now” popularity)
- Add optional Steam current players (Steam Web API) and/or a third-party dataset if acceptable.

Status (implemented so far):
- Phase 1: `Now_*` fields are populated from SteamSpy 2-week fields when available.
- Phase 2: Wikidata provider is implemented and can be enabled via `--source`.
  - `CriticRating_Composite_100` includes IGDB aggregated ratings when present, weighted by `IGDB_AggregatedRatingCount`.
  - `Reach_IGDBAggregatedRatingCount` is exposed as a convenience reach/coverage column.
