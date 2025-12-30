# Provider unused fields (high-ROI candidates)

This document captures **useful provider response fields that we already fetch and cache** but do
not currently surface in `Provider_<Provider>.csv`, `Games_Enriched.csv`, or derived metrics.

Goal: make it easy to decide what to expose next (either as direct CSV columns or as derived
signals/tags) without re-fetching provider data.

Notes:
- “Available” here means “present in cached payloads” (or in example payloads under `docs/examples/`).
- Adding columns should remain curated: avoid dumping entire JSON objects into CSV.
- Prefer **derived signals** when the value comes from multiple sources or needs normalization.

## RAWG (detail payload `/api/games/{id}` is cached)

### High-ROI fields not currently used
- Popularity / engagement:
  - `suggestions_count`
  - `youtube_count`, `twitch_count`
- Ratings distribution:
  - `ratings[]` (breakdown), `reviews_count`, `reviews_text_count`
- Identity / review aid:
  - `metacritic_url`, `reddit_url`
- Media:
  - `background_image_additional`, `short_screenshots[]`, `movies_count`
- Platform requirements:
  - `platforms[].requirements_en.{minimum,recommended}` (HTML/plaintext-ish)

### What these enable
- Faster human verification via official URLs and richer descriptions/media.

## IGDB (single-call `/v4/games` payload is cached)

### High-ROI fields not currently used
- Discovery:
  - `similar_games[]` (and `similar_games.name` if expanded)
- Ratings:
  - `total_rating`, `total_rating_count` (when present)
- External mappings:
  - `external_games[]` beyond Steam (useful for future cross-checks)
- Websites:
  - `websites[].category` (if expanded alongside `websites.url`)

### What these enable
- Stronger “replayability / systemic gameplay” heuristics via keyword sets.
- Better cross-provider identity hints (aliases + external ids).

## Steam Store API (full `appdetails` payload cached per AppID)

### High-ROI fields not currently used
- Longform review aid:
  - `about_the_game`, `detailed_description` (HTML)
- DLC / packaging:
  - `dlc[]`, `package_groups[]`, `packages[]`
- Requirements:
  - `pc_requirements.{minimum,recommended}` (HTML), mac/linux equivalents
- Content rating:
  - `required_age`, `content_descriptors`, `ratings.esrb.*` (when present)
- Media:
  - `movies[]`, `screenshots[]`, `header_image`
- Price structure:
  - `price_overview.{initial,final,discount_percent,currency}` (numeric)

### What these enable
- “Edition / DLC” context for mismatches (base vs DLC vs soundtrack).

## SteamSpy (full payload cached per AppID)

### High-ROI fields not currently used
- Pricing:
  - `currency` (SteamSpy provides only cents + discount; currency inference is non-trivial)
- Time stats:
- Identity:
  - `languages` (free-form, noisy)

### What these enable
- Better “now vs reach” decompositions without adding new providers.

## HowLongToBeat (HLTB) (full result object cached by id)

### High-ROI fields not currently used
- Platform/dev hints (availability varies by game/library version):
  - `profile_dev`
  - `all_styles`, `coop_time`, `mp_time` (if consistently present in cache)

### What these enable
- Additional cross-checks when HLTB appears as an outlier (platform/year mismatches).

## Wikidata (entity + labels cached)

### High-ROI fields not currently used
Wikidata has many possible properties, but most are sparse/inconsistent for games. The safest
future use is:
- Additional external IDs (as cross-check-only hints)
- Better alias sets (already partially used)

## Recommended next extractions (curated)

If we want maximum ROI with minimal CSV bloat, the following are good “next wave” candidates:

- **Faster manual verification**
  - Steam `about_the_game` / `detailed_description` (HTML; consider truncation/cleanup)
  - Steam `header_image` (stable media URL)
  - RAWG `metacritic_url` / `reddit_url` + `background_image` (stable media URL)
  - IGDB `keywords` (strong “what kind of game is it?” identity signal)

## Implementation approach (keep caches stable)

1. Prefer **extracting small, stable columns** from already-cached payloads:
   - Add new columns in the provider client `extract_fields(...)` for the provider CSV.
2. Prefer **derived signals** (in `game_catalog_builder/utils/signals.py`) when:
   - multiple providers contribute to the signal, or
   - data needs normalization (counts/scales).
3. Keep provider caches raw:
   - do not store derived/CSV-format artifacts in cache; regenerate quickly from raw payloads.
