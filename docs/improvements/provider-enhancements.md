# Provider Enhancements Proposal

This proposal summarizes high-value enhancements based on:

- `docs/providers/provider-fields.md`
- `docs/providers/provider-field-reference.md`
- Current cross-provider signals observed in `data/output/Validation_Report.csv`

The goal is to improve: (1) match review speed, (2) mismatch detection quality, and (3) usefulness of the enriched CSV—while keeping transforms minimal and avoiding column clutter.

## Prioritized (implement first)

### 1) Add `SteamEditionYearDrift` (informational)

Problem: Steam years often represent ports/remasters/editions, and we intentionally avoid calling that a mismatch when `SteamEditionOrPort=YES`. However, this hides large year drift that is still valuable to see.

Proposal:
- Add `SteamEditionYearDrift` to the validation report.
- Populate only when:
  - `SteamEditionOrPort=YES`, and
  - `abs(SteamYearDiff_vs_IGDB)` or `abs(SteamYearDiff_vs_RAWG)` is “large” (e.g. `>=2` years).
- Format: a compact string like `IGDB:+11` or `RAWG:-4` (use whichever reference year exists).

Outcome: large Steam year drift stays visible without being treated as a wrong-match signal.

### 2) Series-number detection: refine only if false positives show up

Current catalog signal: only a small number of rows are flagged as `SeriesDisagree=YES`, and those appear to be genuine mismatches (e.g., a base title matched to a sequel).

If false positives appear, add guards in series-number extraction to ignore “numeric brand” patterns:
- Leading-zero tokens like `007`
- `2K`/`2k` patterns (including `2k23`-style)
- Similar non-sequel numeric brands

### 3) Developer/publisher cross-checks (once Steam dev/pub are extracted)

No new endpoints needed: Steam provides developer/publisher lists in `appdetails`.

Add extraction:
- `Steam_Developers` from `developers[]` (comma-separated)
- `Steam_Publishers` from `publishers[]` (comma-separated)

Add validation:
- `developer_disagree`, `publisher_disagree` tags/flags using normalized set overlap:
  - casefold
  - strip punctuation
  - optionally normalize suffixes (`Ltd`, `Inc`, etc.)

Outcome: developer/publisher agreement becomes a strong identity signal when titles/years are ambiguous.

### 4) Reduce validation column clutter

Principle:
- If a column is boolean and primarily diagnostic, prefer summarizing it into `ValidationTags`.
- Keep string/int fields that provide context for manual review.

Notes from current catalog:
- `TitleNonEnglish` / `TitleNonEnglishProviders` were observed as fully empty and can be removed from the report; the raw provider title columns already expose the issue when it occurs.
- Per-provider fuzzy scores (`Score_*_vs_Personal`) belong in import diagnostics (identity stage) rather than the validation report.

Keep (suggested):
- `Name`
- `ValidationTags`
- Provider titles: `RAWG_Name`, `IGDB_Name`, `Steam_Name`, `HLTB_Name`
- `Years`, `Platforms`, `PlatformIntersection`
- ID cross-checks: `SteamAppID`, `IGDB_SteamAppID`
- Review signals: `ReviewTitle`, `ReviewTitleReason`
- New: `SteamEditionYearDrift`

Move to tags only (suggested):
- `TitleMismatch`
- `YearDisagree*` and `SteamYearDisagree`
- `PlatformDisagree`
- `SteamAppIDMismatch`
- `EditionDisagree`
- `SeriesDisagree`
- `TitleNonEnglish`
- `SteamLooksLikeDLC` (keep tag `steam_dlc_like`; optionally keep the column if you consider it “critical”)

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
  - `type` plus `dlc[]` and categories (already extracted) can classify store items as
    `game|dlc|soundtrack|demo` and warn if not a full game.
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

Add tags (to `ValidationTags`) without creating many new columns:
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
