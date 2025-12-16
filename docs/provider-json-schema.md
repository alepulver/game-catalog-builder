# Provider JSON schemas (examples)

This project can capture "raw" provider responses for a specific game name (for example, `Doom`) and save them under `docs/examples/<game-slug>/`.

These files are intended for:
- Auditing what each provider returns
- Deciding what additional fields are worth extracting into the final CSV
- Keeping stable example fixtures for discussion and iteration

## Conventions

- Files are named `<provider>.<endpoint>.json` when a provider has multiple calls.
- Some providers return multiple candidate matches for a search; in that case, the search response is saved in full and we also save the chosen "best match" record.
- IGDB is not a single "complete JSON" document: you must choose which `fields` to request in the query. The example uses a broad but finite field set.
- When a title is ambiguous (e.g., `Doom`), capture examples using a disambiguated query (e.g., `Doom (2016)`) and verify by release year so all providers refer to the same game.

## RAWG

Files:
- `rawg.search.json`: Full response from `GET /api/games?search=...`
- `rawg.best.json`: The selected best match object from `results[]`
- `rawg.detail.json`: Full response from `GET /api/games/{id}`

Notes:
- `rawg.search.json` contains `results[]` objects which are typically smaller than the detail object.
- `rawg.detail.json` is the richest single RAWG document and is the best place to extract tags, platforms, stores, website, and description fields.

## IGDB (Twitch/IGDB)

Files:
- `igdb.games.search.json`: Raw list response from `POST /v4/games` using a broad `fields ...; search "...";` query
- `igdb.best.json`: Best-match game object from the search list
- `igdb.resolved.json`: Convenience object with common ID lists resolved to names (e.g., genre IDs → genre names)

Notes:
- IGDB returns numeric IDs for many relationships; resolving them requires additional `POST /v4/<endpoint>` calls.
- If requested via `external_games.external_game_source,external_games.uid`, IGDB can include a Steam appid mapping (where `external_game_source == 1`).
- The example `igdb.resolved.json` is not authoritative; it's a helper view to make analysis easier.

## Steam (Store API)

Files:
- `steam.storesearch.json`: Full response from `GET /api/storesearch?term=...`
- `steam.best.json`: The selected best match item from `items[]`
- `steam.appdetails.json`: Full response from `GET /api/appdetails?appids=<appid>&l=english`

Notes:
- `steam.appdetails.json` is keyed by appid as a string and wraps the actual data under `{ "<appid>": { "success": true, "data": ... } }`.

## SteamSpy

Files:
- `steamspy.appdetails.json`: Full response from `GET /api.php?request=appdetails&appid=<appid>`

Notes:
- SteamSpy may return `{ "error": "..." }` instead of a detail object (the example script records that verbatim).

## HowLongToBeat (HLTB)

Files:
- `hltb.search.json`: A JSON-serializable snapshot of the search results (derived from the library objects)
- `hltb.best.json`: The chosen best match (derived from the library object)

Notes:
- This project uses `howlongtobeatpy`, which returns Python objects rather than a raw HTTP JSON document. The example files are a faithful dump of the object attributes for analysis.
