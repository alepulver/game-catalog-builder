# Provider Enhancements Proposal (Future Work)

This proposal summarizes high-value enhancements based on:

- `docs/providers/provider-fields.md`
- `docs/providers/provider-field-reference.md`
- Current cross-provider signals observed during `import --diagnostics` in `data/input/Games_Catalog.csv` (`ReviewTags`, `MatchConfidence`).

The goal is to improve: (1) match review speed, (2) mismatch detection quality, and (3) usefulness of the enriched CSV—while keeping transforms minimal and avoiding column clutter.

This doc is intentionally “action-first”: it prioritizes signals that help you decide whether a provider match is correct, and how to fix it (pin an ID vs rename vs add a YearHint).

This file is only for **not-yet-implemented** improvements. Current behavior and the current tag
vocabulary are documented in:

- `docs/how-it-works.md`
- `docs/matching.md`

## Prioritized (implement next)

### 1) Series-number detection: refine only if false positives show up

Series/roman numerals are already a high-signal mismatch detector.

If false positives appear, add guards in series-number extraction to ignore “numeric brand” patterns:
- Leading-zero tokens like `007`
- `2K`/`2k` patterns (including `2k23`-style)
- Similar non-sequel numeric brands

### 2) Developer/publisher extraction + cross-checks

No new endpoints needed: Steam provides developer/publisher lists in `appdetails`.

Add extraction:
- `Steam_Developers` from `developers[]` (comma-separated)
- `Steam_Publishers` from `publishers[]` (comma-separated)

Add validation:
- `developer_disagree`, `publisher_disagree` tags based on normalized set overlap.

Outcome: dev/pub agreement becomes a strong identity signal when titles/years are ambiguous.

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

Implementation note:
- The IGDB client already requests most of these (e.g. `summary`, `storyline`, `alternative_names.name`,
  `websites.url`, `parent_game.name`, `version_parent.name`, `involved_companies.*`, `age_ratings.*`) via
  field expansion in the single `games` request. Adding them to CSV outputs should not require extra IGDB API
  calls—just extracting and selecting additional `IGDB_*` columns.

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

## What’s missing right now (from this proposal)

The following items are not currently extracted into `Provider_*.csv` / `Games_Enriched.csv`, and therefore can’t yet drive review/validation tags:

- **Steam dev/pub fields**: `Steam_Developers`, `Steam_Publishers` (available from `appdetails`, no new endpoint)
- **Steam/RAWG/IGDB “review aid” text**: short description / summary and provider URLs
- **IGDB relationship context** for editions/ports: `parent_game`, `version_parent`, `expansions`, `ports` (still single-call via field expansion)
- **RAWG richer metadata**: full `genres[]` (not just first two), plus `esrb_rating.name`
- **Stronger cross-provider validation tags** based on dev/pub or store type:
  - `developer_disagree`, `publisher_disagree`, `store_type_not_game`

## Optional improvement: missing-provider severity

Use platform info to downgrade expected misses:
- If a row does not include `pc` in its platform intersection/union, treat `missing:Steam` and `missing:SteamSpy` as informational rather than review-worthy.
