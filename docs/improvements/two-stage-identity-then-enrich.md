# Two-stage pipeline: identity resolution → enrichment

This proposal separates the pipeline into two explicit stages:

1. **Identity resolution**: map each user-row title to a canonical identity and provider IDs (and/or canonical name).
2. **Enrichment**: use those IDs to fetch public metadata and derived fields, then join into final outputs.

The goal is to make the “which game is this?” problem explicit, reviewable, and stable across re-runs, and to reduce rework when changing which provider fields are extracted.

## Why split the pipeline?

Today the pipeline interleaves:
- “search by name, pick best match” and
- “extract metadata and write output columns”.

This makes it harder to:
- review and correct ambiguous matches (especially with MyVideoGameList non-standard names),
- reuse resolved IDs across different output schemas,
- keep user data (ratings, notes, etc.) cleanly attached to the intended canonical entry.

## Stage 1: Identity resolution

**Input**
- User catalog CSV (e.g. MyVideoGameList export).

**Output**
- An “identity mapping” file (CSV/JSON), one row per input row, containing:
  - `RowId` (stable key; see below)
  - `OriginalName` (as in user data)
  - Per-provider IDs (where resolved):
    - `RAWG_ID`, `IGDB_ID`, `Steam_AppID`
    - `HLTB_Query` (optional override string; HLTB has no stable public ID)
  - Match evidence:
    - `*_MatchedName` and `*_MatchScore` columns (per provider)
  - Review workflow:
    - `ReviewTags` (compact comma-separated tags like `missing_steam`, `year_mismatch`, `steam_appid_mismatch`, `steam_score:88`)
    - `NeedsReview`, `AddedAt`, `NewRow`

**Stable row identity (important)**
- Do not rely on `Name` alone because names can repeat (editions/platforms).
- Use either:
  - `InputRowIndex` (line/row number) + hash of the original row, or
  - a generated UUID stored in a new column in the user dataset, or
  - `(OriginalName, Platform, AddedDate, OccurrenceIndex)` as a composite key.

**Manual overrides**
- In practice, manual edits are made directly in `data/output/Games_Identity.csv` (pinning IDs / `HLTB_Query`), and the pipeline should preserve those edits across regenerations.

**Benefits**
- You can rerun enrichment without redoing fuzzy searches.
- You can safely change output schemas and re-enrich using IDs.
- You can keep a tight, reviewable list of ambiguous titles.

## Stage 1 fetch strategy: minimal vs full payload caching

Validation is most useful during matching, but it requires fetching *some* provider data (at least matched title and year/platform signals).

We discussed 3 practical strategies:

### Strategy A: Minimal identity payloads (recommended)

**Idea**
- Stage 1 fetches only what’s needed for identity validation and cross-provider consistency:
  - provider ID
  - provider title
  - release year/date (when available)
  - platforms (coarsened)
  - cross-check IDs (e.g., IGDB Steam external mapping when present)
  - match score + top alternatives
- Stage 2 fetches richer metadata (descriptions, tags, etc.) only after IDs are pinned.

**Pros**
- Faster Stage 1 review loop (especially if skipping slow providers like HLTB).
- Less bandwidth and fewer calls where detail endpoints are heavy (notably RAWG detail).
- Keeps Stage 1 artifacts compact and focused on “did we match the right game?”

**Cons**
- Some “interesting” fields are only visible in Stage 2 unless explicitly added to Stage 1.
- Requires deciding which fields count as “identity validation fields”.

### Strategy B: Full payload caching up front (eager)

**Idea**
- Stage 1 fetches and caches “full” provider payloads immediately after matching.

**Pros**
- Stage 2 becomes mostly a reshape/extract step (very fast).
- Easy to experiment with adding fields later if raw payloads are already cached.
- Works well for providers where “full” is almost the same as “minimal” (e.g., Steam appdetails; IGDB with expanded fields in one call).

**Cons**
- Can significantly increase runtime and calls for providers with heavy detail endpoints (notably RAWG if you add `/games/{id}` for all rows).
- Stage 1 becomes longer, even though the core human loop is “review matches”.

### Strategy C: Lazy detail fetching (on-demand)

**Idea**
- Stage 1 resolves IDs and caches search candidates + chosen ID only.
- Additional provider payloads are fetched only when:
  - Stage 2 runs, or
  - validation explicitly needs them, or
  - a user requests more detail for a specific row.

**Pros**
- Minimal upfront cost; best for quick “get IDs pinned” workflows.
- Avoids downloading large payloads you may never use.

**Cons**
- Harder to compare candidates during Stage 1 unless you add extra UI/reporting.
- Requires more orchestration complexity (tracking what’s available vs missing).

### Practical recommendation

- Stage 1: eager-cache what’s cheap and identity-relevant (IGDB single-call expanded fields, Steam appdetails), avoid heavy optional details unless needed (RAWG detail), and consider skipping slow providers (HLTB) during initial matching.
- Stage 2: fetch the full enrichment set, driven by an output schema selection.

## Stage 2: Enrichment (public + derived)

**Input**
- Identity mapping (IDs + canonical name)

**Process**
- Fetch provider payloads by ID (no name searching).
- Extract configured fields (direct + derived).
- Join results back to user rows by `RowId`.

**Output**
- `Games_<Provider>.csv` keyed by `RowId` (and optionally include `Name` for readability).
- `Games_Enriched.csv` as a join of user data + selected provider fields.
- Validation report remains useful, but most “wrong game” issues should be caught in Stage 1.

## Tradeoffs

**Pros**
- Clear separation of concerns; easier debugging.
- Faster re-runs when experimenting with new output fields.
- Better support for fixing ambiguous titles and duplicates.

**Cons**
- Introduces another artifact to manage (`identity_map.csv/json`).
- Requires refactoring merge keys from `Name` to `RowId`.
- Needs decisions around how to generate and persist `RowId`.

## Suggested incremental path

1. Add `RowId` generation in the CLI when reading input.
2. Write `data/output/Games_Identity.csv` with per-provider matched IDs + scores.
3. Add a `--resolve-only` mode that only performs Stage 1.
4. Update provider outputs + merger to join on `RowId` (not `Name`).
5. Add `--enrich-only` mode that uses IDs and skips searching.
