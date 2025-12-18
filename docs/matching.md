# Matching (pinning stage)

This document describes how the **import/pinning** stage matches your `Name` rows to provider IDs.

## Goals

- Prefer the “same game” across providers (base game vs sequel/DLC/demo is a mismatch).
- Avoid overfitting heuristics: when unclear, emit warnings and let you pin IDs in `Games_Catalog.csv`.
- Keep matching repeatable via cached raw search responses.

## Inputs used for matching

- `Name` (required): the title to search.
- `YearHint` (optional): a *soft* disambiguation signal (not strict validation).
- `Platform` (optional): used to decide whether Steam is “expected” during review tagging.

## Normalization

All fuzzy matching uses a normalized form:

- lowercase
- strip punctuation (`: - ’ ® ™` etc.)
- collapse whitespace
- common roman numerals → arabic (I/II/III → 1/2/3)

Example: `Assassin’s Creed®` → `assassins creed`

## Candidate scoring (shared)

When a provider returns multiple candidates, the selection uses:

- Base similarity: `rapidfuzz.token_sort_ratio`
- Partial matching is only allowed when the only difference is:
  - a year token (e.g. `Doom` vs `Doom 2016`), or
  - common edition tokens (e.g. `GOTY`, `Enhanced`, `Definitive`)

Additional adjustments:

- **Sequel penalty**: if the query has *no* sequel number but the candidate does, penalize it.
- **Different series-number penalty**: if both sides have numbers but they differ (e.g. `Postal 4` vs `Postal 2`), heavily penalize it.
- **DLC-like penalty**: titles containing tokens like `demo`, `soundtrack`, `season pass`, `pack` are penalized.
- **YearHint boost/penalty (when provider exposes a year)**:
  - closer year → higher score, far drift → lower score

### Exact match preference

If the candidate is an *exact token match* (same tokens, not just a substring match), it is preferred over other candidates even if `YearHint` would boost a different title.

Example: `Mafia` should prefer `Mafia` over `Mafia: The Game`.

### “Numbered prefix + subtitle” preference

If the query includes a non-year number and the candidate starts with the query, it is treated as a strong match.

Example: `Postal 4` → `POSTAL 4: No Regerts`.

## Provider-specific query behavior

### RAWG

- Tries a strict search first (`search_exact=1`, `search_precise=1`).
- Falls back to a looser search (no strict flags) when:
  - strict returns **0** results, or
  - strict returns results but the best match score is below the acceptance threshold.
- For titles with subtitles, may try a fallback search using only the portion before `:` (e.g.
  `Doom II: Hell on Earth` → `Doom II`).
- For short numbered titles (e.g. `WRC 6`), applies a conservative numeric filter so unrelated
  “... 6” titles don’t beat the real match.

### IGDB

- Uses a stripped query (removes trailing `"(YYYY)"` if present).
- If `YearHint` exists:
  - first query uses a ±1 year window on `first_release_date`
  - falls back to an unfiltered search if needed

### Steam

- Searches Steam store (`storesearch`) using English.
- If `YearHint` exists, it may fetch app details (`appdetails`) for a small set of candidates to:
  - filter out non-`game` types
  - use Steam release date as an additional tie-breaker (note: Steam “release year” can be a port/re-release year).

## Caching (important)

Caches are intentionally **not** “name → chosen ID”.

- `by_query`: caches lightweight candidate lists keyed by the exact query string/params.
- `by_id`: caches raw provider payloads keyed by provider ID.

This avoids stale/wrong selections when heuristics change: on re-runs, the program re-selects from cached results unless you pin IDs manually.

### “Not found” caching

Because caching is query-based, “not found” is also cached **per query**:

- If a provider returns an empty result set, that empty response is stored under `by_query` (all providers).
- If you later change how the query is formed (e.g. different punctuation stripping / year hint usage),
  the cache key changes and the provider will try again (which is the behavior you want).
- Request failures (timeouts/no response) are intentionally not negative-cached.

### HLTB note (build once)

HLTB searches are slow relative to other providers, so the cache is designed to be reusable:

- Once `data/cache/hltb_cache.json` contains the search results (`by_query`) and chosen entries (`by_id`),
  re-runs should not re-query HLTB for the same inputs.
- The client avoids rewriting the cache when an entry is already present in `by_id`.

## Warnings and manual pinning

- Any non-100% match emits a `WARNING` with alternatives.
- When a match is wrong or ambiguous, pin the provider ID in `data/input/Games_Catalog.csv` rather than changing your original `Name` unless the rename is truly canonical for you.

## Known issues / limitations

- Providers have provider-specific canonical titles; sometimes the “best” name differs per provider.
- Steam search can surface DLC/soundtrack entries even when the base game exists; these should be pinned when encountered.
- `YearHint` is best-effort: Steam’s year may reflect store release/re-release, not the original release year.
