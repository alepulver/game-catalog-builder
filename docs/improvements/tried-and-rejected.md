# Tried And Rejected

This page tracks ideas we experimented with and intentionally reverted, so we don’t reintroduce
them without new evidence.

## Matching / Pinning

### Broad Wikidata-alias retries for missing provider IDs
- Idea: when a provider ID is missing, use Wikidata label/aliases/enwiki title as alternate search
  queries to “rescue” the missing provider.
- Result: significantly increased extra provider requests (especially Steam) and run time, with
  minimal improvement in pin coverage on this dataset.
- Decision: do not run “missing provider rescue” retries by default.

Note: we do keep a narrow, safer retry: after an **auto-unpin** (strict majority consensus + outlier),
the importer may attempt a single retry for that provider using the majority title and known aliases.

### Wikipedia search as an automatic alias source
- Idea: use Wikipedia search/redirects to source alternate titles, then retry provider searches.
- Result: expensive if applied broadly; even with a strict row budget and Wikidata “video game” guards,
  it did not materially improve repin success on this catalog compared to provider/Wikidata aliases.
- Decision: remove the code for now; it’s easy to reintroduce later if we find a strong need.

### Consensus-based retries to fill missing IDs
- Idea: if several providers agree on identity, retry missing providers using the consensus title.
- Result: no meaningful fills on the catalog during measurement; added code paths and complexity.
- Decision: removed; rely on direct provider matching + manual pinning and keep the importer
  predictable.

### RAWG “strict search” flags (exact/precise)
- Idea: use stricter RAWG search parameters to force exact matches.
- Result: worsened matching for ambiguous/short titles; fallback + scoring performed better.
- Decision: use standard RAWG search and select via scoring.

## Provider Coupling

### Treat IGDB Steam ID as a dependency
- Idea: require IGDB’s Steam external mapping to drive Steam/SteamSpy.
- Result: introduced ordering/coupling problems and made “run providers independently” harder.
- Decision: keep IGDB Steam ID as a cross-check only (log/diagnostics), not as a hard dependency.
