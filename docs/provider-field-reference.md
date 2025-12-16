# Provider field reference

This reference is generated from `docs/provider-field-catalog.yaml` and enriched with observed examples from `docs/examples/` when available.

Legend:
- **Observed types/example**: derived from example JSON captures; may be empty if not present in current examples.
- **Doc links**: point to provider documentation; some providers (Steam Store API) are not formally specified.

## RAWG

Docs:
- RAWG API docs: https://rawg.io/apidocs

### GET /api/games (search)

| Path | Description | Observed types | Example | Example file |
|---|---|---|---|---|
| `results[].id` | Game id (in search results) | `number` | `2454` | `doom-2016/rawg.search.json` |
| `results[].name` | Game title | `string` | `"DOOM (2016)"` | `doom-2016/rawg.search.json` |
| `results[].released` | Release date (YYYY-MM-DD, may be empty) | `string` | `"2016-05-12"` | `doom-2016/rawg.search.json` |
| `results[].metacritic` | Metacritic score (may be null) | `null, number` | `85` | `doom-2016/rawg.search.json` |
| `results[].rating` | Average RAWG rating | `number` | `4.38` | `doom-2016/rawg.search.json` |
| `results[].ratings_count` | Ratings count | `number` | `3597` | `doom-2016/rawg.search.json` |
| `results[].genres[].name` | Genre names | `string` | `"Shooter"` | `doom-2016/rawg.search.json` |
| `results[].platforms[].platform.name` | Platform names | `string` | `"PC"` | `doom-2016/rawg.search.json` |
| `results[].tags[].name` | Tag names | `string` | `"Singleplayer"` | `doom-2016/rawg.search.json` |
| `results[].esrb_rating.name` | ESRB rating name (if present) | `string` | `"Mature"` | `doom-2016/rawg.search.json` |

#### Observed in examples (not yet described)

| Path | Observed types | Example | Example file |
|---|---|---|---|
| `$` | `object` | `{"count": 9709, "next": "https://api.rawg.io/api/games?key=7e2d873737374ee4a6044ced7272a146&page=2&page_size=10&search=Doom+%282016%29", "previous": null, "results": [{"slug": "...` | `doom-2016/rawg.search.json` |
| `achievements_count` | `number` | `334` | `doom-2016/rawg.detail.json` |
| `added` | `number` | `13956` | `doom-2016/rawg.detail.json` |
| `added_by_status` | `object` | `{"yet": 602, "owned": 8758, "beaten": 2989, "toplay": 527, "dropped": 774, "playing": 306}` | `doom-2016/rawg.detail.json` |
| `added_by_status.beaten` | `number` | `2989` | `doom-2016/rawg.detail.json` |
| `added_by_status.dropped` | `number` | `774` | `doom-2016/rawg.detail.json` |
| `added_by_status.owned` | `number` | `8758` | `doom-2016/rawg.detail.json` |
| `added_by_status.playing` | `number` | `306` | `doom-2016/rawg.detail.json` |
| `added_by_status.toplay` | `number` | `527` | `doom-2016/rawg.detail.json` |
| `added_by_status.yet` | `number` | `602` | `doom-2016/rawg.detail.json` |
| `additions_count` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `alternative_names` | `array` | `[]` | `doom-2016/rawg.detail.json` |
| `background_image` | `string` | `"https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"` | `doom-2016/rawg.detail.json` |
| `background_image_additional` | `string` | `"https://media.rawg.io/media/screenshots/d3b/d3b881ae214f9ad14724afc41b49dcea.jpg"` | `doom-2016/rawg.detail.json` |
| `best` | `object` | `{"slug": "doom", "name": "DOOM (2016)", "playtime": 10, "platforms": [{"platform": {"id": 4, "name": "PC", "slug": "pc"}}, {"platform": {"id": 1, "name": "Xbox One", "slug": "xb...` | `doom-2016/rawg.best.json` |
| `best.added` | `number` | `13956` | `doom-2016/rawg.best.json` |
| `best.added_by_status` | `object` | `{"yet": 602, "owned": 8758, "beaten": 2989, "toplay": 527, "dropped": 774, "playing": 306}` | `doom-2016/rawg.best.json` |
| `best.added_by_status.beaten` | `number` | `2989` | `doom-2016/rawg.best.json` |
| `best.added_by_status.dropped` | `number` | `774` | `doom-2016/rawg.best.json` |
| `best.added_by_status.owned` | `number` | `8758` | `doom-2016/rawg.best.json` |
| `best.added_by_status.playing` | `number` | `306` | `doom-2016/rawg.best.json` |
| `best.added_by_status.toplay` | `number` | `527` | `doom-2016/rawg.best.json` |
| `best.added_by_status.yet` | `number` | `602` | `doom-2016/rawg.best.json` |
| `best.background_image` | `string` | `"https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"` | `doom-2016/rawg.best.json` |
| `best.clip` | `null` | `null` | `doom-2016/rawg.best.json` |
| `best.dominant_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.best.json` |
| `best.esrb_rating` | `object` | `{"id": 4, "name": "Mature", "slug": "mature", "name_en": "Mature", "name_ru": "С 17 лет"}` | `doom-2016/rawg.best.json` |
| `best.esrb_rating.id` | `number` | `4` | `doom-2016/rawg.best.json` |
| `best.esrb_rating.name` | `string` | `"Mature"` | `doom-2016/rawg.best.json` |
| `best.esrb_rating.name_en` | `string` | `"Mature"` | `doom-2016/rawg.best.json` |
| `best.esrb_rating.name_ru` | `string` | `"С 17 лет"` | `doom-2016/rawg.best.json` |
| `best.esrb_rating.slug` | `string` | `"mature"` | `doom-2016/rawg.best.json` |
| `best.genres` | `array` | `[{"id": 2, "name": "Shooter", "slug": "shooter"}, {"id": 4, "name": "Action", "slug": "action"}]` | `doom-2016/rawg.best.json` |
| `best.genres[]` | `object` | `{"id": 2, "name": "Shooter", "slug": "shooter"}` | `doom-2016/rawg.best.json` |
| `best.genres[].id` | `number` | `2` | `doom-2016/rawg.best.json` |
| `best.genres[].name` | `string` | `"Shooter"` | `doom-2016/rawg.best.json` |
| `best.genres[].slug` | `string` | `"shooter"` | `doom-2016/rawg.best.json` |
| `best.id` | `number` | `2454` | `doom-2016/rawg.best.json` |
| `best.metacritic` | `number` | `85` | `doom-2016/rawg.best.json` |
| `best.name` | `string` | `"DOOM (2016)"` | `doom-2016/rawg.best.json` |
| `best.parent_platforms` | `array` | `[{"platform": {"id": 1, "name": "PC", "slug": "pc"}}, {"platform": {"id": 2, "name": "PlayStation", "slug": "playstation"}}, {"platform": {"id": 3, "name": "Xbox", "slug": "xbox...` | `doom-2016/rawg.best.json` |
| `best.parent_platforms[]` | `object` | `{"platform": {"id": 1, "name": "PC", "slug": "pc"}}` | `doom-2016/rawg.best.json` |
| `best.parent_platforms[].platform` | `object` | `{"id": 1, "name": "PC", "slug": "pc"}` | `doom-2016/rawg.best.json` |
| `best.parent_platforms[].platform.id` | `number` | `1` | `doom-2016/rawg.best.json` |
| `best.parent_platforms[].platform.name` | `string` | `"PC"` | `doom-2016/rawg.best.json` |
| `best.parent_platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.best.json` |
| `best.platforms` | `array` | `[{"platform": {"id": 4, "name": "PC", "slug": "pc"}}, {"platform": {"id": 1, "name": "Xbox One", "slug": "xbox-one"}}, {"platform": {"id": 18, "name": "PlayStation 4", "slug": "...` | `doom-2016/rawg.best.json` |
| `best.platforms[]` | `object` | `{"platform": {"id": 4, "name": "PC", "slug": "pc"}}` | `doom-2016/rawg.best.json` |
| `best.platforms[].platform` | `object` | `{"id": 4, "name": "PC", "slug": "pc"}` | `doom-2016/rawg.best.json` |
| `best.platforms[].platform.id` | `number` | `4` | `doom-2016/rawg.best.json` |
| `best.platforms[].platform.name` | `string` | `"PC"` | `doom-2016/rawg.best.json` |
| `best.platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.best.json` |
| `best.playtime` | `number` | `10` | `doom-2016/rawg.best.json` |
| `best.rating` | `number` | `4.38` | `doom-2016/rawg.best.json` |
| `best.rating_top` | `number` | `5` | `doom-2016/rawg.best.json` |
| `best.ratings` | `array` | `[{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}, {"id": 4, "title": "recommended", "count": 1400, "percent": 38.49}, {"id": 3, "title": "meh", "count": 261, ...` | `doom-2016/rawg.best.json` |
| `best.ratings[]` | `object` | `{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}` | `doom-2016/rawg.best.json` |
| `best.ratings[].count` | `number` | `1891` | `doom-2016/rawg.best.json` |
| `best.ratings[].id` | `number` | `5` | `doom-2016/rawg.best.json` |
| `best.ratings[].percent` | `number` | `51.99` | `doom-2016/rawg.best.json` |
| `best.ratings[].title` | `string` | `"exceptional"` | `doom-2016/rawg.best.json` |
| `best.ratings_count` | `number` | `3597` | `doom-2016/rawg.best.json` |
| `best.released` | `string` | `"2016-05-12"` | `doom-2016/rawg.best.json` |
| `best.reviews_count` | `number` | `3637` | `doom-2016/rawg.best.json` |
| `best.reviews_text_count` | `number` | `29` | `doom-2016/rawg.best.json` |
| `best.saturated_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.best.json` |
| `best.score` | `string` | `"77.28927"` | `doom-2016/rawg.best.json` |
| `best.short_screenshots` | `array` | `[{"id": -1, "image": "https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"}, {"id": 22393, "image": "https://media.rawg.io/media/screenshots/353/353c1e834...` | `doom-2016/rawg.best.json` |
| `best.short_screenshots[]` | `object` | `{"id": -1, "image": "https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"}` | `doom-2016/rawg.best.json` |
| `best.short_screenshots[].id` | `number` | `-1` | `doom-2016/rawg.best.json` |
| `best.short_screenshots[].image` | `string` | `"https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"` | `doom-2016/rawg.best.json` |
| `best.slug` | `string` | `"doom"` | `doom-2016/rawg.best.json` |
| `best.stores` | `array` | `[{"store": {"id": 1, "name": "Steam", "slug": "steam"}}, {"store": {"id": 3, "name": "PlayStation Store", "slug": "playstation-store"}}, {"store": {"id": 2, "name": "Xbox Store"...` | `doom-2016/rawg.best.json` |
| `best.stores[]` | `object` | `{"store": {"id": 1, "name": "Steam", "slug": "steam"}}` | `doom-2016/rawg.best.json` |
| `best.stores[].store` | `object` | `{"id": 1, "name": "Steam", "slug": "steam"}` | `doom-2016/rawg.best.json` |
| `best.stores[].store.id` | `number` | `1` | `doom-2016/rawg.best.json` |
| `best.stores[].store.name` | `string` | `"Steam"` | `doom-2016/rawg.best.json` |
| `best.stores[].store.slug` | `string` | `"steam"` | `doom-2016/rawg.best.json` |
| `best.suggestions_count` | `number` | `672` | `doom-2016/rawg.best.json` |
| `best.tags` | `array` | `[{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b...` | `doom-2016/rawg.best.json` |
| `best.tags[]` | `object` | `{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b1...` | `doom-2016/rawg.best.json` |
| `best.tags[].games_count` | `number` | `250602` | `doom-2016/rawg.best.json` |
| `best.tags[].id` | `number` | `31` | `doom-2016/rawg.best.json` |
| `best.tags[].image_background` | `string` | `"https://media.rawg.io/media/games/120/1201a40e4364557b124392ee50317b99.jpg"` | `doom-2016/rawg.best.json` |
| `best.tags[].language` | `string` | `"eng"` | `doom-2016/rawg.best.json` |
| `best.tags[].name` | `string` | `"Singleplayer"` | `doom-2016/rawg.best.json` |
| `best.tags[].slug` | `string` | `"singleplayer"` | `doom-2016/rawg.best.json` |
| `best.tba` | `boolean` | `false` | `doom-2016/rawg.best.json` |
| `best.updated` | `string` | `"2025-12-03T22:11:39"` | `doom-2016/rawg.best.json` |
| `best.user_game` | `null` | `null` | `doom-2016/rawg.best.json` |
| `clip` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `count` | `number` | `9709` | `doom-2016/rawg.search.json` |
| `creators_count` | `number` | `50` | `doom-2016/rawg.detail.json` |
| `description` | `string` | `"<p>Return of the classic FPS, Doom (2016) acts as a reboot of the series and brings back the Doomslayer, protagonist of the original Doom games. In order to solve the energy cr...` | `doom-2016/rawg.detail.json` |
| `description_raw` | `string` | `"Return of the classic FPS, Doom (2016) acts as a reboot of the series and brings back the Doomslayer, protagonist of the original Doom games. In order to solve the energy crisi...` | `doom-2016/rawg.detail.json` |
| `developers` | `array` | `[{"id": 4, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 63, "image_background": "https://media.rawg.io/media/games/596/596a48ef3b62b63b4cc59633e28b...` | `doom-2016/rawg.detail.json` |
| `developers[]` | `object` | `{"id": 4, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 63, "image_background": "https://media.rawg.io/media/games/596/596a48ef3b62b63b4cc59633e28be...` | `doom-2016/rawg.detail.json` |
| `developers[].games_count` | `number` | `63` | `doom-2016/rawg.detail.json` |
| `developers[].id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `developers[].image_background` | `string` | `"https://media.rawg.io/media/games/596/596a48ef3b62b63b4cc59633e28be903.jpg"` | `doom-2016/rawg.detail.json` |
| `developers[].name` | `string` | `"Bethesda Softworks"` | `doom-2016/rawg.detail.json` |
| `developers[].slug` | `string` | `"bethesda-softworks"` | `doom-2016/rawg.detail.json` |
| `dominant_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.detail.json` |
| `esrb_rating` | `object` | `{"id": 4, "name": "Mature", "slug": "mature"}` | `doom-2016/rawg.detail.json` |
| `esrb_rating.id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `esrb_rating.name` | `string` | `"Mature"` | `doom-2016/rawg.detail.json` |
| `esrb_rating.slug` | `string` | `"mature"` | `doom-2016/rawg.detail.json` |
| `game_series_count` | `number` | `9` | `doom-2016/rawg.detail.json` |
| `genres` | `array` | `[{"id": 4, "name": "Action", "slug": "action", "games_count": 191319, "image_background": "https://media.rawg.io/media/games/4be/4be6a6ad0364751a96229c56bf69be59.jpg"}, {"id": 2...` | `doom-2016/rawg.detail.json` |
| `genres[]` | `object` | `{"id": 4, "name": "Action", "slug": "action", "games_count": 191319, "image_background": "https://media.rawg.io/media/games/4be/4be6a6ad0364751a96229c56bf69be59.jpg"}` | `doom-2016/rawg.detail.json` |
| `genres[].games_count` | `number` | `191319` | `doom-2016/rawg.detail.json` |
| `genres[].id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `genres[].image_background` | `string` | `"https://media.rawg.io/media/games/4be/4be6a6ad0364751a96229c56bf69be59.jpg"` | `doom-2016/rawg.detail.json` |
| `genres[].name` | `string` | `"Action"` | `doom-2016/rawg.detail.json` |
| `genres[].slug` | `string` | `"action"` | `doom-2016/rawg.detail.json` |
| `id` | `number` | `2454` | `doom-2016/rawg.detail.json` |
| `metacritic` | `number` | `85` | `doom-2016/rawg.detail.json` |
| `metacritic_platforms` | `array` | `[]` | `doom-2016/rawg.detail.json` |
| `metacritic_url` | `string` | `""` | `doom-2016/rawg.detail.json` |
| `movies_count` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `name` | `string` | `"DOOM (2016)"` | `doom-2016/rawg.detail.json` |
| `name_original` | `string` | `"DOOM (2016)"` | `doom-2016/rawg.detail.json` |
| `next` | `string` | `"https://api.rawg.io/api/games?key=7e2d873737374ee4a6044ced7272a146&page=2&page_size=10&search=Doom+%282016%29"` | `doom-2016/rawg.search.json` |
| `parent_achievements_count` | `number` | `71` | `doom-2016/rawg.detail.json` |
| `parent_platforms` | `array` | `[{"platform": {"id": 1, "name": "PC", "slug": "pc"}}, {"platform": {"id": 2, "name": "PlayStation", "slug": "playstation"}}, {"platform": {"id": 3, "name": "Xbox", "slug": "xbox...` | `doom-2016/rawg.detail.json` |
| `parent_platforms[]` | `object` | `{"platform": {"id": 1, "name": "PC", "slug": "pc"}}` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform` | `object` | `{"id": 1, "name": "PC", "slug": "pc"}` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform.id` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform.name` | `string` | `"PC"` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.detail.json` |
| `parents_count` | `number` | `0` | `doom-2016/rawg.detail.json` |
| `platforms` | `array` | `[{"platform": {"id": 4, "name": "PC", "slug": "pc", "image": null, "year_end": null, "year_start": null, "games_count": 560082, "image_background": "https://media.rawg.io/media/...` | `doom-2016/rawg.detail.json` |
| `platforms[]` | `object` | `{"platform": {"id": 4, "name": "PC", "slug": "pc", "image": null, "year_end": null, "year_start": null, "games_count": 560082, "image_background": "https://media.rawg.io/media/g...` | `doom-2016/rawg.detail.json` |
| `platforms[].platform` | `object` | `{"id": 4, "name": "PC", "slug": "pc", "image": null, "year_end": null, "year_start": null, "games_count": 560082, "image_background": "https://media.rawg.io/media/games/d82/d829...` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.games_count` | `number` | `560082` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.image` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.image_background` | `string` | `"https://media.rawg.io/media/games/d82/d82990b9c67ba0d2d09d4e6fa88885a7.jpg"` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.name` | `string` | `"PC"` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.year_end` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.year_start` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `platforms[].released_at` | `string` | `"2016-05-12"` | `doom-2016/rawg.detail.json` |
| `platforms[].requirements` | `object` | `{"minimum": "Minimum:\nOS: Windows7,Windows8,Windows10\nProcessor: Intel cpu i3\nMemory: 4 GB RAM\nGraphics: GTX 650\nStorage: 2 GB available space\nSound Card: Realtek", "recom...` | `doom-2016/rawg.detail.json` |
| `platforms[].requirements.minimum` | `string` | `"Minimum:\nOS: Windows7,Windows8,Windows10\nProcessor: Intel cpu i3\nMemory: 4 GB RAM\nGraphics: GTX 650\nStorage: 2 GB available space\nSound Card: Realtek"` | `doom-2016/rawg.detail.json` |
| `platforms[].requirements.recommended` | `string` | `"Recommended:\nOS: Windows7,Windows8,Windows10\nProcessor: Intel cpu i5\nMemory: 8 GB RAM\nGraphics: GTX 770\nStorage: 4 GB available space\nSound Card: Realtek"` | `doom-2016/rawg.detail.json` |
| `playtime` | `number` | `10` | `doom-2016/rawg.detail.json` |
| `previous` | `null` | `null` | `doom-2016/rawg.search.json` |
| `publishers` | `array` | `[{"id": 339, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 189, "image_background": "https://media.rawg.io/media/games/5a4/5a4e70bb8a862829dbaa398aa...` | `doom-2016/rawg.detail.json` |
| `publishers[]` | `object` | `{"id": 339, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 189, "image_background": "https://media.rawg.io/media/games/5a4/5a4e70bb8a862829dbaa398aa5...` | `doom-2016/rawg.detail.json` |
| `publishers[].games_count` | `number` | `189` | `doom-2016/rawg.detail.json` |
| `publishers[].id` | `number` | `339` | `doom-2016/rawg.detail.json` |
| `publishers[].image_background` | `string` | `"https://media.rawg.io/media/games/5a4/5a4e70bb8a862829dbaa398aa5f66afc.jpg"` | `doom-2016/rawg.detail.json` |
| `publishers[].name` | `string` | `"Bethesda Softworks"` | `doom-2016/rawg.detail.json` |
| `publishers[].slug` | `string` | `"bethesda-softworks"` | `doom-2016/rawg.detail.json` |
| `rating` | `number` | `4.38` | `doom-2016/rawg.detail.json` |
| `rating_top` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `ratings` | `array` | `[{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}, {"id": 4, "title": "recommended", "count": 1400, "percent": 38.49}, {"id": 3, "title": "meh", "count": 261, ...` | `doom-2016/rawg.detail.json` |
| `ratings[]` | `object` | `{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}` | `doom-2016/rawg.detail.json` |
| `ratings[].count` | `number` | `1891` | `doom-2016/rawg.detail.json` |
| `ratings[].id` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `ratings[].percent` | `number` | `51.99` | `doom-2016/rawg.detail.json` |
| `ratings[].title` | `string` | `"exceptional"` | `doom-2016/rawg.detail.json` |
| `ratings_count` | `number` | `3596` | `doom-2016/rawg.detail.json` |
| `reactions` | `object` | `{"1": 10, "2": 3, "3": 7, "4": 3, "5": 1, "6": 5, "7": 1, "10": 2, "11": 6, "12": 10, "15": 1, "16": 1}` | `doom-2016/rawg.detail.json` |
| `reactions.1` | `number` | `10` | `doom-2016/rawg.detail.json` |
| `reactions.10` | `number` | `2` | `doom-2016/rawg.detail.json` |
| `reactions.11` | `number` | `6` | `doom-2016/rawg.detail.json` |
| `reactions.12` | `number` | `10` | `doom-2016/rawg.detail.json` |
| `reactions.15` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reactions.16` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reactions.2` | `number` | `3` | `doom-2016/rawg.detail.json` |
| `reactions.3` | `number` | `7` | `doom-2016/rawg.detail.json` |
| `reactions.4` | `number` | `3` | `doom-2016/rawg.detail.json` |
| `reactions.5` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reactions.6` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `reactions.7` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reddit_count` | `number` | `972` | `doom-2016/rawg.detail.json` |
| `reddit_description` | `string` | `""` | `doom-2016/rawg.detail.json` |
| `reddit_logo` | `string` | `""` | `doom-2016/rawg.detail.json` |
| `reddit_name` | `string` | `""` | `doom-2016/rawg.detail.json` |
| `reddit_url` | `string` | `"https://www.reddit.com/r/Doom/"` | `doom-2016/rawg.detail.json` |
| `released` | `string` | `"2016-05-12"` | `doom-2016/rawg.detail.json` |
| `results` | `array` | `[{"slug": "doom", "name": "DOOM (2016)", "playtime": 10, "platforms": [{"platform": {"id": 4, "name": "PC", "slug": "pc"}}, {"platform": {"id": 1, "name": "Xbox One", "slug": "x...` | `doom-2016/rawg.search.json` |
| `results[]` | `object` | `{"slug": "doom", "name": "DOOM (2016)", "playtime": 10, "platforms": [{"platform": {"id": 4, "name": "PC", "slug": "pc"}}, {"platform": {"id": 1, "name": "Xbox One", "slug": "xb...` | `doom-2016/rawg.search.json` |
| `results[].added` | `number` | `13956` | `doom-2016/rawg.search.json` |
| `results[].added_by_status` | `object` | `{"yet": 602, "owned": 8758, "beaten": 2989, "toplay": 527, "dropped": 774, "playing": 306}` | `doom-2016/rawg.search.json` |
| `results[].added_by_status.beaten` | `number` | `2989` | `doom-2016/rawg.search.json` |
| `results[].added_by_status.dropped` | `number` | `774` | `doom-2016/rawg.search.json` |
| `results[].added_by_status.owned` | `number` | `8758` | `doom-2016/rawg.search.json` |
| `results[].added_by_status.playing` | `number` | `306` | `doom-2016/rawg.search.json` |
| `results[].added_by_status.toplay` | `number` | `527` | `doom-2016/rawg.search.json` |
| `results[].added_by_status.yet` | `number` | `602` | `doom-2016/rawg.search.json` |
| `results[].background_image` | `string` | `"https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"` | `doom-2016/rawg.search.json` |
| `results[].clip` | `null` | `null` | `doom-2016/rawg.search.json` |
| `results[].dominant_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.search.json` |
| `results[].esrb_rating` | `null, object` | `{"id": 4, "name": "Mature", "slug": "mature", "name_en": "Mature", "name_ru": "С 17 лет"}` | `doom-2016/rawg.search.json` |
| `results[].esrb_rating.id` | `number` | `4` | `doom-2016/rawg.search.json` |
| `results[].esrb_rating.name_en` | `string` | `"Mature"` | `doom-2016/rawg.search.json` |
| `results[].esrb_rating.name_ru` | `string` | `"С 17 лет"` | `doom-2016/rawg.search.json` |
| `results[].esrb_rating.slug` | `string` | `"mature"` | `doom-2016/rawg.search.json` |
| `results[].genres` | `array` | `[{"id": 2, "name": "Shooter", "slug": "shooter"}, {"id": 4, "name": "Action", "slug": "action"}]` | `doom-2016/rawg.search.json` |
| `results[].genres[]` | `object` | `{"id": 2, "name": "Shooter", "slug": "shooter"}` | `doom-2016/rawg.search.json` |
| `results[].genres[].id` | `number` | `2` | `doom-2016/rawg.search.json` |
| `results[].genres[].slug` | `string` | `"shooter"` | `doom-2016/rawg.search.json` |
| `results[].parent_platforms` | `array` | `[{"platform": {"id": 1, "name": "PC", "slug": "pc"}}, {"platform": {"id": 2, "name": "PlayStation", "slug": "playstation"}}, {"platform": {"id": 3, "name": "Xbox", "slug": "xbox...` | `doom-2016/rawg.search.json` |
| `results[].parent_platforms[]` | `object` | `{"platform": {"id": 1, "name": "PC", "slug": "pc"}}` | `doom-2016/rawg.search.json` |
| `results[].parent_platforms[].platform` | `object` | `{"id": 1, "name": "PC", "slug": "pc"}` | `doom-2016/rawg.search.json` |
| `results[].parent_platforms[].platform.id` | `number` | `1` | `doom-2016/rawg.search.json` |
| `results[].parent_platforms[].platform.name` | `string` | `"PC"` | `doom-2016/rawg.search.json` |
| `results[].parent_platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.search.json` |
| `results[].platforms` | `array` | `[{"platform": {"id": 4, "name": "PC", "slug": "pc"}}, {"platform": {"id": 1, "name": "Xbox One", "slug": "xbox-one"}}, {"platform": {"id": 18, "name": "PlayStation 4", "slug": "...` | `doom-2016/rawg.search.json` |
| `results[].platforms[]` | `object` | `{"platform": {"id": 4, "name": "PC", "slug": "pc"}}` | `doom-2016/rawg.search.json` |
| `results[].platforms[].platform` | `object` | `{"id": 4, "name": "PC", "slug": "pc"}` | `doom-2016/rawg.search.json` |
| `results[].platforms[].platform.id` | `number` | `4` | `doom-2016/rawg.search.json` |
| `results[].platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.search.json` |
| `results[].playtime` | `number` | `10` | `doom-2016/rawg.search.json` |
| `results[].rating_top` | `number` | `5` | `doom-2016/rawg.search.json` |
| `results[].ratings` | `array` | `[{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}, {"id": 4, "title": "recommended", "count": 1400, "percent": 38.49}, {"id": 3, "title": "meh", "count": 261, ...` | `doom-2016/rawg.search.json` |
| `results[].ratings[]` | `object` | `{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}` | `doom-2016/rawg.search.json` |
| `results[].ratings[].count` | `number` | `1891` | `doom-2016/rawg.search.json` |
| `results[].ratings[].id` | `number` | `5` | `doom-2016/rawg.search.json` |
| `results[].ratings[].percent` | `number` | `51.99` | `doom-2016/rawg.search.json` |
| `results[].ratings[].title` | `string` | `"exceptional"` | `doom-2016/rawg.search.json` |
| `results[].reviews_count` | `number` | `3637` | `doom-2016/rawg.search.json` |
| `results[].reviews_text_count` | `number` | `29` | `doom-2016/rawg.search.json` |
| `results[].saturated_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.search.json` |
| `results[].score` | `string` | `"77.28927"` | `doom-2016/rawg.search.json` |
| `results[].short_screenshots` | `array` | `[{"id": -1, "image": "https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"}, {"id": 22393, "image": "https://media.rawg.io/media/screenshots/353/353c1e834...` | `doom-2016/rawg.search.json` |
| `results[].short_screenshots[]` | `object` | `{"id": -1, "image": "https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"}` | `doom-2016/rawg.search.json` |
| `results[].short_screenshots[].id` | `number` | `-1` | `doom-2016/rawg.search.json` |
| `results[].short_screenshots[].image` | `string` | `"https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"` | `doom-2016/rawg.search.json` |
| `results[].slug` | `string` | `"doom"` | `doom-2016/rawg.search.json` |
| `results[].stores` | `array` | `[{"store": {"id": 1, "name": "Steam", "slug": "steam"}}, {"store": {"id": 3, "name": "PlayStation Store", "slug": "playstation-store"}}, {"store": {"id": 2, "name": "Xbox Store"...` | `doom-2016/rawg.search.json` |
| `results[].stores[]` | `object` | `{"store": {"id": 1, "name": "Steam", "slug": "steam"}}` | `doom-2016/rawg.search.json` |
| `results[].stores[].store` | `object` | `{"id": 1, "name": "Steam", "slug": "steam"}` | `doom-2016/rawg.search.json` |
| `results[].stores[].store.id` | `number` | `1` | `doom-2016/rawg.search.json` |
| `results[].stores[].store.name` | `string` | `"Steam"` | `doom-2016/rawg.search.json` |
| `results[].stores[].store.slug` | `string` | `"steam"` | `doom-2016/rawg.search.json` |
| `results[].suggestions_count` | `number` | `672` | `doom-2016/rawg.search.json` |
| `results[].tags` | `array` | `[{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b...` | `doom-2016/rawg.search.json` |
| `results[].tags[]` | `object` | `{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b1...` | `doom-2016/rawg.search.json` |
| `results[].tags[].games_count` | `number` | `250602` | `doom-2016/rawg.search.json` |
| `results[].tags[].id` | `number` | `31` | `doom-2016/rawg.search.json` |
| `results[].tags[].image_background` | `string` | `"https://media.rawg.io/media/games/120/1201a40e4364557b124392ee50317b99.jpg"` | `doom-2016/rawg.search.json` |
| `results[].tags[].language` | `string` | `"eng"` | `doom-2016/rawg.search.json` |
| `results[].tags[].slug` | `string` | `"singleplayer"` | `doom-2016/rawg.search.json` |
| `results[].tba` | `boolean` | `false` | `doom-2016/rawg.search.json` |
| `results[].updated` | `string` | `"2025-12-03T22:11:39"` | `doom-2016/rawg.search.json` |
| `results[].user_game` | `null` | `null` | `doom-2016/rawg.search.json` |
| `reviews_count` | `number` | `3637` | `doom-2016/rawg.detail.json` |
| `reviews_text_count` | `number` | `40` | `doom-2016/rawg.detail.json` |
| `saturated_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.detail.json` |
| `score` | `number` | `100` | `doom-2016/rawg.best.json` |
| `screenshots_count` | `number` | `17` | `doom-2016/rawg.detail.json` |
| `slug` | `string` | `"doom"` | `doom-2016/rawg.detail.json` |
| `stores` | `array` | `[{"id": 1003369, "url": "", "store": {"id": 5, "name": "GOG", "slug": "gog", "domain": "gog.com", "games_count": 7098, "image_background": "https://media.rawg.io/media/games/c80...` | `doom-2016/rawg.detail.json` |
| `stores[]` | `object` | `{"id": 1003369, "url": "", "store": {"id": 5, "name": "GOG", "slug": "gog", "domain": "gog.com", "games_count": 7098, "image_background": "https://media.rawg.io/media/games/c80/...` | `doom-2016/rawg.detail.json` |
| `stores[].id` | `number` | `1003369` | `doom-2016/rawg.detail.json` |
| `stores[].store` | `object` | `{"id": 5, "name": "GOG", "slug": "gog", "domain": "gog.com", "games_count": 7098, "image_background": "https://media.rawg.io/media/games/c80/c80bcf321da44d69b18a06c04d942662.jpg"}` | `doom-2016/rawg.detail.json` |
| `stores[].store.domain` | `string` | `"gog.com"` | `doom-2016/rawg.detail.json` |
| `stores[].store.games_count` | `number` | `7098` | `doom-2016/rawg.detail.json` |
| `stores[].store.id` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `stores[].store.image_background` | `string` | `"https://media.rawg.io/media/games/c80/c80bcf321da44d69b18a06c04d942662.jpg"` | `doom-2016/rawg.detail.json` |
| `stores[].store.name` | `string` | `"GOG"` | `doom-2016/rawg.detail.json` |
| `stores[].store.slug` | `string` | `"gog"` | `doom-2016/rawg.detail.json` |
| `stores[].url` | `string` | `""` | `doom-2016/rawg.detail.json` |
| `suggestions_count` | `number` | `672` | `doom-2016/rawg.detail.json` |
| `tags` | `array` | `[{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b...` | `doom-2016/rawg.detail.json` |
| `tags[]` | `object` | `{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b1...` | `doom-2016/rawg.detail.json` |
| `tags[].games_count` | `number` | `250602` | `doom-2016/rawg.detail.json` |
| `tags[].id` | `number` | `31` | `doom-2016/rawg.detail.json` |
| `tags[].image_background` | `string` | `"https://media.rawg.io/media/games/120/1201a40e4364557b124392ee50317b99.jpg"` | `doom-2016/rawg.detail.json` |
| `tags[].language` | `string` | `"eng"` | `doom-2016/rawg.detail.json` |
| `tags[].name` | `string` | `"Singleplayer"` | `doom-2016/rawg.detail.json` |
| `tags[].slug` | `string` | `"singleplayer"` | `doom-2016/rawg.detail.json` |
| `tba` | `boolean` | `false` | `doom-2016/rawg.detail.json` |
| `twitch_count` | `number` | `0` | `doom-2016/rawg.detail.json` |
| `updated` | `string` | `"2025-12-03T22:11:39"` | `doom-2016/rawg.detail.json` |
| `user_game` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `user_platforms` | `boolean` | `false` | `doom-2016/rawg.search.json` |
| `website` | `string` | `"https://bethesda.net/game/doom"` | `doom-2016/rawg.detail.json` |
| `youtube_count` | `number` | `1000000` | `doom-2016/rawg.detail.json` |

### GET /api/games/{id} (detail)

| Path | Description | Observed types | Example | Example file |
|---|---|---|---|---|
| `id` | Game id | `number` | `2454` | `doom-2016/rawg.detail.json` |
| `slug` | RAWG slug | `string` | `"doom"` | `doom-2016/rawg.detail.json` |
| `name` | Game title | `string` | `"DOOM (2016)"` | `doom-2016/rawg.detail.json` |
| `released` | Release date | `string` | `"2016-05-12"` | `doom-2016/rawg.detail.json` |
| `tba` | “To be announced” flag | `boolean` | `false` | `doom-2016/rawg.detail.json` |
| `updated` | Last updated timestamp | `string` | `"2025-12-03T22:11:39"` | `doom-2016/rawg.detail.json` |
| `website` | Official website | `string` | `"https://bethesda.net/game/doom"` | `doom-2016/rawg.detail.json` |
| `description_raw` | Plain-text description | `string` | `"Return of the classic FPS, Doom (2016) acts as a reboot of the series and brings back the Doomslayer, protagonist of the original Doom games. In order to solve the energy crisi...` | `doom-2016/rawg.detail.json` |
| `description` | HTML description | `string` | `"<p>Return of the classic FPS, Doom (2016) acts as a reboot of the series and brings back the Doomslayer, protagonist of the original Doom games. In order to solve the energy cr...` | `doom-2016/rawg.detail.json` |
| `metacritic` | Metacritic score | `number` | `85` | `doom-2016/rawg.detail.json` |
| `metacritic_url` | Metacritic URL | `string` | `""` | `doom-2016/rawg.detail.json` |
| `rating` | RAWG rating | `number` | `4.38` | `doom-2016/rawg.detail.json` |
| `ratings_count` | RAWG ratings count | `number` | `3596` | `doom-2016/rawg.detail.json` |
| `reddit_url` | Reddit URL | `string` | `"https://www.reddit.com/r/Doom/"` | `doom-2016/rawg.detail.json` |
| `reddit_name` | Subreddit name | `string` | `""` | `doom-2016/rawg.detail.json` |
| `achievements_count` | Achievement count (if known) | `number` | `334` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform.name` | High-level platform groups | `string` | `"PC"` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.name` | Platforms | `string` | `"PC"` | `doom-2016/rawg.detail.json` |
| `genres[].name` | Genres | `string` | `"Action"` | `doom-2016/rawg.detail.json` |
| `tags[].name` | Tags | `string` | `"Singleplayer"` | `doom-2016/rawg.detail.json` |
| `developers[].name` | Developer names | `string` | `"Bethesda Softworks"` | `doom-2016/rawg.detail.json` |
| `publishers[].name` | Publisher names | `string` | `"Bethesda Softworks"` | `doom-2016/rawg.detail.json` |
| `stores[].store.name` | Store names | `string` | `"GOG"` | `doom-2016/rawg.detail.json` |
| `stores[].url` | Store URL | `string` | `""` | `doom-2016/rawg.detail.json` |

#### Observed in examples (not yet described)

| Path | Observed types | Example | Example file |
|---|---|---|---|
| `$` | `object` | `{"id": 2454, "slug": "doom", "name": "DOOM (2016)", "name_original": "DOOM (2016)", "description": "<p>Return of the classic FPS, Doom (2016) acts as a reboot of the series and ...` | `doom-2016/rawg.detail.json` |
| `added` | `number` | `13956` | `doom-2016/rawg.detail.json` |
| `added_by_status` | `object` | `{"yet": 602, "owned": 8758, "beaten": 2989, "toplay": 527, "dropped": 774, "playing": 306}` | `doom-2016/rawg.detail.json` |
| `added_by_status.beaten` | `number` | `2989` | `doom-2016/rawg.detail.json` |
| `added_by_status.dropped` | `number` | `774` | `doom-2016/rawg.detail.json` |
| `added_by_status.owned` | `number` | `8758` | `doom-2016/rawg.detail.json` |
| `added_by_status.playing` | `number` | `306` | `doom-2016/rawg.detail.json` |
| `added_by_status.toplay` | `number` | `527` | `doom-2016/rawg.detail.json` |
| `added_by_status.yet` | `number` | `602` | `doom-2016/rawg.detail.json` |
| `additions_count` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `alternative_names` | `array` | `[]` | `doom-2016/rawg.detail.json` |
| `background_image` | `string` | `"https://media.rawg.io/media/games/587/587588c64afbff80e6f444eb2e46f9da.jpg"` | `doom-2016/rawg.detail.json` |
| `background_image_additional` | `string` | `"https://media.rawg.io/media/screenshots/d3b/d3b881ae214f9ad14724afc41b49dcea.jpg"` | `doom-2016/rawg.detail.json` |
| `clip` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `creators_count` | `number` | `50` | `doom-2016/rawg.detail.json` |
| `developers` | `array` | `[{"id": 4, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 63, "image_background": "https://media.rawg.io/media/games/596/596a48ef3b62b63b4cc59633e28b...` | `doom-2016/rawg.detail.json` |
| `developers[]` | `object` | `{"id": 4, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 63, "image_background": "https://media.rawg.io/media/games/596/596a48ef3b62b63b4cc59633e28be...` | `doom-2016/rawg.detail.json` |
| `developers[].games_count` | `number` | `63` | `doom-2016/rawg.detail.json` |
| `developers[].id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `developers[].image_background` | `string` | `"https://media.rawg.io/media/games/596/596a48ef3b62b63b4cc59633e28be903.jpg"` | `doom-2016/rawg.detail.json` |
| `developers[].slug` | `string` | `"bethesda-softworks"` | `doom-2016/rawg.detail.json` |
| `dominant_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.detail.json` |
| `esrb_rating` | `object` | `{"id": 4, "name": "Mature", "slug": "mature"}` | `doom-2016/rawg.detail.json` |
| `esrb_rating.id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `esrb_rating.name` | `string` | `"Mature"` | `doom-2016/rawg.detail.json` |
| `esrb_rating.slug` | `string` | `"mature"` | `doom-2016/rawg.detail.json` |
| `game_series_count` | `number` | `9` | `doom-2016/rawg.detail.json` |
| `genres` | `array` | `[{"id": 4, "name": "Action", "slug": "action", "games_count": 191319, "image_background": "https://media.rawg.io/media/games/4be/4be6a6ad0364751a96229c56bf69be59.jpg"}, {"id": 2...` | `doom-2016/rawg.detail.json` |
| `genres[]` | `object` | `{"id": 4, "name": "Action", "slug": "action", "games_count": 191319, "image_background": "https://media.rawg.io/media/games/4be/4be6a6ad0364751a96229c56bf69be59.jpg"}` | `doom-2016/rawg.detail.json` |
| `genres[].games_count` | `number` | `191319` | `doom-2016/rawg.detail.json` |
| `genres[].id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `genres[].image_background` | `string` | `"https://media.rawg.io/media/games/4be/4be6a6ad0364751a96229c56bf69be59.jpg"` | `doom-2016/rawg.detail.json` |
| `genres[].slug` | `string` | `"action"` | `doom-2016/rawg.detail.json` |
| `metacritic_platforms` | `array` | `[]` | `doom-2016/rawg.detail.json` |
| `movies_count` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `name_original` | `string` | `"DOOM (2016)"` | `doom-2016/rawg.detail.json` |
| `parent_achievements_count` | `number` | `71` | `doom-2016/rawg.detail.json` |
| `parent_platforms` | `array` | `[{"platform": {"id": 1, "name": "PC", "slug": "pc"}}, {"platform": {"id": 2, "name": "PlayStation", "slug": "playstation"}}, {"platform": {"id": 3, "name": "Xbox", "slug": "xbox...` | `doom-2016/rawg.detail.json` |
| `parent_platforms[]` | `object` | `{"platform": {"id": 1, "name": "PC", "slug": "pc"}}` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform` | `object` | `{"id": 1, "name": "PC", "slug": "pc"}` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform.id` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `parent_platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.detail.json` |
| `parents_count` | `number` | `0` | `doom-2016/rawg.detail.json` |
| `platforms` | `array` | `[{"platform": {"id": 4, "name": "PC", "slug": "pc", "image": null, "year_end": null, "year_start": null, "games_count": 560082, "image_background": "https://media.rawg.io/media/...` | `doom-2016/rawg.detail.json` |
| `platforms[]` | `object` | `{"platform": {"id": 4, "name": "PC", "slug": "pc", "image": null, "year_end": null, "year_start": null, "games_count": 560082, "image_background": "https://media.rawg.io/media/g...` | `doom-2016/rawg.detail.json` |
| `platforms[].platform` | `object` | `{"id": 4, "name": "PC", "slug": "pc", "image": null, "year_end": null, "year_start": null, "games_count": 560082, "image_background": "https://media.rawg.io/media/games/d82/d829...` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.games_count` | `number` | `560082` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.id` | `number` | `4` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.image` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.image_background` | `string` | `"https://media.rawg.io/media/games/d82/d82990b9c67ba0d2d09d4e6fa88885a7.jpg"` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.slug` | `string` | `"pc"` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.year_end` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `platforms[].platform.year_start` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `platforms[].released_at` | `string` | `"2016-05-12"` | `doom-2016/rawg.detail.json` |
| `platforms[].requirements` | `object` | `{"minimum": "Minimum:\nOS: Windows7,Windows8,Windows10\nProcessor: Intel cpu i3\nMemory: 4 GB RAM\nGraphics: GTX 650\nStorage: 2 GB available space\nSound Card: Realtek", "recom...` | `doom-2016/rawg.detail.json` |
| `platforms[].requirements.minimum` | `string` | `"Minimum:\nOS: Windows7,Windows8,Windows10\nProcessor: Intel cpu i3\nMemory: 4 GB RAM\nGraphics: GTX 650\nStorage: 2 GB available space\nSound Card: Realtek"` | `doom-2016/rawg.detail.json` |
| `platforms[].requirements.recommended` | `string` | `"Recommended:\nOS: Windows7,Windows8,Windows10\nProcessor: Intel cpu i5\nMemory: 8 GB RAM\nGraphics: GTX 770\nStorage: 4 GB available space\nSound Card: Realtek"` | `doom-2016/rawg.detail.json` |
| `playtime` | `number` | `10` | `doom-2016/rawg.detail.json` |
| `publishers` | `array` | `[{"id": 339, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 189, "image_background": "https://media.rawg.io/media/games/5a4/5a4e70bb8a862829dbaa398aa...` | `doom-2016/rawg.detail.json` |
| `publishers[]` | `object` | `{"id": 339, "name": "Bethesda Softworks", "slug": "bethesda-softworks", "games_count": 189, "image_background": "https://media.rawg.io/media/games/5a4/5a4e70bb8a862829dbaa398aa5...` | `doom-2016/rawg.detail.json` |
| `publishers[].games_count` | `number` | `189` | `doom-2016/rawg.detail.json` |
| `publishers[].id` | `number` | `339` | `doom-2016/rawg.detail.json` |
| `publishers[].image_background` | `string` | `"https://media.rawg.io/media/games/5a4/5a4e70bb8a862829dbaa398aa5f66afc.jpg"` | `doom-2016/rawg.detail.json` |
| `publishers[].slug` | `string` | `"bethesda-softworks"` | `doom-2016/rawg.detail.json` |
| `rating_top` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `ratings` | `array` | `[{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}, {"id": 4, "title": "recommended", "count": 1400, "percent": 38.49}, {"id": 3, "title": "meh", "count": 261, ...` | `doom-2016/rawg.detail.json` |
| `ratings[]` | `object` | `{"id": 5, "title": "exceptional", "count": 1891, "percent": 51.99}` | `doom-2016/rawg.detail.json` |
| `ratings[].count` | `number` | `1891` | `doom-2016/rawg.detail.json` |
| `ratings[].id` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `ratings[].percent` | `number` | `51.99` | `doom-2016/rawg.detail.json` |
| `ratings[].title` | `string` | `"exceptional"` | `doom-2016/rawg.detail.json` |
| `reactions` | `object` | `{"1": 10, "2": 3, "3": 7, "4": 3, "5": 1, "6": 5, "7": 1, "10": 2, "11": 6, "12": 10, "15": 1, "16": 1}` | `doom-2016/rawg.detail.json` |
| `reactions.1` | `number` | `10` | `doom-2016/rawg.detail.json` |
| `reactions.10` | `number` | `2` | `doom-2016/rawg.detail.json` |
| `reactions.11` | `number` | `6` | `doom-2016/rawg.detail.json` |
| `reactions.12` | `number` | `10` | `doom-2016/rawg.detail.json` |
| `reactions.15` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reactions.16` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reactions.2` | `number` | `3` | `doom-2016/rawg.detail.json` |
| `reactions.3` | `number` | `7` | `doom-2016/rawg.detail.json` |
| `reactions.4` | `number` | `3` | `doom-2016/rawg.detail.json` |
| `reactions.5` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reactions.6` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `reactions.7` | `number` | `1` | `doom-2016/rawg.detail.json` |
| `reddit_count` | `number` | `972` | `doom-2016/rawg.detail.json` |
| `reddit_description` | `string` | `""` | `doom-2016/rawg.detail.json` |
| `reddit_logo` | `string` | `""` | `doom-2016/rawg.detail.json` |
| `reviews_count` | `number` | `3637` | `doom-2016/rawg.detail.json` |
| `reviews_text_count` | `number` | `40` | `doom-2016/rawg.detail.json` |
| `saturated_color` | `string` | `"0f0f0f"` | `doom-2016/rawg.detail.json` |
| `screenshots_count` | `number` | `17` | `doom-2016/rawg.detail.json` |
| `stores` | `array` | `[{"id": 1003369, "url": "", "store": {"id": 5, "name": "GOG", "slug": "gog", "domain": "gog.com", "games_count": 7098, "image_background": "https://media.rawg.io/media/games/c80...` | `doom-2016/rawg.detail.json` |
| `stores[]` | `object` | `{"id": 1003369, "url": "", "store": {"id": 5, "name": "GOG", "slug": "gog", "domain": "gog.com", "games_count": 7098, "image_background": "https://media.rawg.io/media/games/c80/...` | `doom-2016/rawg.detail.json` |
| `stores[].id` | `number` | `1003369` | `doom-2016/rawg.detail.json` |
| `stores[].store` | `object` | `{"id": 5, "name": "GOG", "slug": "gog", "domain": "gog.com", "games_count": 7098, "image_background": "https://media.rawg.io/media/games/c80/c80bcf321da44d69b18a06c04d942662.jpg"}` | `doom-2016/rawg.detail.json` |
| `stores[].store.domain` | `string` | `"gog.com"` | `doom-2016/rawg.detail.json` |
| `stores[].store.games_count` | `number` | `7098` | `doom-2016/rawg.detail.json` |
| `stores[].store.id` | `number` | `5` | `doom-2016/rawg.detail.json` |
| `stores[].store.image_background` | `string` | `"https://media.rawg.io/media/games/c80/c80bcf321da44d69b18a06c04d942662.jpg"` | `doom-2016/rawg.detail.json` |
| `stores[].store.slug` | `string` | `"gog"` | `doom-2016/rawg.detail.json` |
| `suggestions_count` | `number` | `672` | `doom-2016/rawg.detail.json` |
| `tags` | `array` | `[{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b...` | `doom-2016/rawg.detail.json` |
| `tags[]` | `object` | `{"id": 31, "name": "Singleplayer", "slug": "singleplayer", "language": "eng", "games_count": 250602, "image_background": "https://media.rawg.io/media/games/120/1201a40e4364557b1...` | `doom-2016/rawg.detail.json` |
| `tags[].games_count` | `number` | `250602` | `doom-2016/rawg.detail.json` |
| `tags[].id` | `number` | `31` | `doom-2016/rawg.detail.json` |
| `tags[].image_background` | `string` | `"https://media.rawg.io/media/games/120/1201a40e4364557b124392ee50317b99.jpg"` | `doom-2016/rawg.detail.json` |
| `tags[].language` | `string` | `"eng"` | `doom-2016/rawg.detail.json` |
| `tags[].slug` | `string` | `"singleplayer"` | `doom-2016/rawg.detail.json` |
| `twitch_count` | `number` | `0` | `doom-2016/rawg.detail.json` |
| `user_game` | `null` | `null` | `doom-2016/rawg.detail.json` |
| `youtube_count` | `number` | `1000000` | `doom-2016/rawg.detail.json` |

## IGDB

Docs:
- IGDB API docs: https://api-docs.igdb.com/
- Twitch OAuth (client credentials): https://dev.twitch.tv/docs/authentication/getting-tokens-oauth/

### POST /v4/games

| Path | Description | Observed types | Example | Example file |
|---|---|---|---|---|
| `$[]` | IGDB returns a list of game objects for a query | `object` | `{"id": 7351, "aggregated_rating": 86.96875, "aggregated_rating_count": 28, "dlcs": [25571, 26558, 22431], "external_games": [{"id": 1934827, "uid": "B01CTWZEDE"}, {"id": 138274,...` | `doom-2016/igdb.games.search.json` |
| `$[].id` | Game id | `number` | `7351` | `doom-2016/igdb.games.search.json` |
| `$[].name` | Game title | `string` | `"Doom"` | `doom-2016/igdb.games.search.json` |
| `$[].slug` | URL slug | `string` | `"doom--2"` | `doom-2016/igdb.games.search.json` |
| `$[].summary` | Summary text | `string` | `"Developed by id software, the studio that pioneered the first-person shooter genre and created multiplayer Deathmatch, Doom returns as a brutally fun and challenging modern-day...` | `doom-2016/igdb.games.search.json` |
| `$[].storyline` | Storyline text | `string` | `"You’ve come here for a reason. The Union Aerospace Corporation’s massive research facility on Mars is overwhelmed by fierce and powerful demons, and only one person stands betw...` | `doom-2016/igdb.games.search.json` |
| `$[].rating` | User rating (0-100) when present | `number` | `85.30925725648731` | `doom-2016/igdb.games.search.json` |
| `$[].rating_count` | Rating count when present | `number` | `1794` | `doom-2016/igdb.games.search.json` |
| `$[].aggregated_rating` | Aggregated critic rating (0-100) when present | `number` | `86.96875` | `doom-2016/igdb.games.search.json` |
| `$[].aggregated_rating_count` | Aggregated rating count when present | `number` | `28` | `doom-2016/igdb.games.search.json` |
| `$[].total_rating` | Combined rating (0-100) when present | `number` | `86.13900362824366` | `doom-2016/igdb.games.search.json` |
| `$[].total_rating_count` | Combined rating count when present | `number` | `1822` | `doom-2016/igdb.games.search.json` |
| `$[].first_release_date` | First release date as unix timestamp (seconds) | `number` | `1463011200` | `doom-2016/igdb.games.search.json` |
| `$[].genres[]` | Genre ids (use genres.name to expand in-query) | `number` | `5` | `doom-2016/igdb.games.search.json` |
| `$[].themes[]` | Theme ids (use themes.name to expand in-query) | `number` | `1` | `doom-2016/igdb.games.search.json` |
| `$[].game_modes[]` | Game mode ids (use game_modes.name to expand in-query) | `number` | `1` | `doom-2016/igdb.games.search.json` |
| `$[].player_perspectives[]` | Player perspective ids (use player_perspectives.name to expand in-query) | `number` | `1` | `doom-2016/igdb.games.search.json` |
| `$[].franchises[]` | Franchise ids (use franchises.name to expand in-query) | `number` | `798` | `doom-2016/igdb.games.search.json` |
| `$[].game_engines[]` | Game engine ids (use game_engines.name to expand in-query) | `number` | `172` | `doom-2016/igdb.games.search.json` |
| `$[].platforms[]` | Platform ids (use platforms.name to expand in-query) | `number` | `48` | `doom-2016/igdb.games.search.json` |
| `$[].keywords[]` | Keyword ids (use keywords.name to expand in-query) | `number` | `3` | `doom-2016/igdb.games.search.json` |
| `$[].dlcs[]` | DLC ids (use dlcs.name to expand in-query) | `number` | `25571` | `doom-2016/igdb.games.search.json` |
| `$[].ports[]` | Port ids (use ports.name to expand in-query) | `number` | `76217` | `doom-2016/igdb.games.search.json` |
| `$[].similar_games[]` | Similar game ids (use similar_games.name to expand in-query) | `number` | `1006` | `doom-2016/igdb.games.search.json` |
| `$[].websites[]` | Website ids (use websites.url/websites.category to expand in-query) | `number` | `61572` | `doom-2016/igdb.games.search.json` |
| `$[].involved_companies[]` | Involved company ids (use involved_companies.* to expand in-query) | `number` | `198290` | `doom-2016/igdb.games.search.json` |
| `$[].genres[].name` | Expanded genre names (when requested as genres.name) | `` | `` | `` |
| `$[].themes[].name` | Expanded theme names (when requested as themes.name) | `` | `` | `` |
| `$[].game_modes[].name` | Expanded game mode names (when requested as game_modes.name) | `` | `` | `` |
| `$[].player_perspectives[].name` | Expanded player perspective names (when requested as player_perspectives.name) | `` | `` | `` |
| `$[].franchises[].name` | Expanded franchise names (when requested as franchises.name) | `` | `` | `` |
| `$[].game_engines[].name` | Expanded game engine names (when requested as game_engines.name) | `` | `` | `` |
| `$[].platforms[].name` | Expanded platform names (when requested as platforms.name) | `` | `` | `` |
| `$[].keywords[].name` | Expanded keyword names (when requested as keywords.name) | `` | `` | `` |
| `$[].dlcs[].name` | Expanded DLC names (when requested as dlcs.name) | `` | `` | `` |
| `$[].ports[].name` | Expanded port names (when requested as ports.name) | `` | `` | `` |
| `$[].similar_games[].name` | Expanded similar game names (when requested as similar_games.name) | `` | `` | `` |
| `$[].websites[].url` | Expanded website URL (when requested as websites.url) | `` | `` | `` |
| `$[].websites[].category` | Expanded website category code (when requested as websites.category) | `` | `` | `` |
| `$[].involved_companies[].developer` | Expanded involved company developer flag (when requested) | `` | `` | `` |
| `$[].involved_companies[].publisher` | Expanded involved company publisher flag (when requested) | `` | `` | `` |
| `$[].involved_companies[].company.name` | Expanded company name (when requested as involved_companies.company.name) | `` | `` | `` |
| `$[].external_games[].external_game_source` | External mapping source id (1 == Steam) | `` | `` | `` |
| `$[].external_games[].id` | External mapping row id | `number` | `1934827` | `doom-2016/igdb.games.search.json` |
| `$[].external_games[].uid` | External mapping uid (Steam appid as string when source == 1) | `string` | `"B01CTWZEDE"` | `doom-2016/igdb.games.search.json` |

#### Observed in examples (not yet described)

| Path | Observed types | Example | Example file |
|---|---|---|---|
| `$` | `array, object` | `[{"id": 7351, "aggregated_rating": 86.96875, "aggregated_rating_count": 28, "dlcs": [25571, 26558, 22431], "external_games": [{"id": 1934827, "uid": "B01CTWZEDE"}, {"id": 138274...` | `doom-2016/igdb.games.search.json` |
| `$[].dlcs` | `array` | `[25571, 26558, 22431]` | `doom-2016/igdb.games.search.json` |
| `$[].external_games` | `array` | `[{"id": 1934827, "uid": "B01CTWZEDE"}, {"id": 138274, "uid": "20654"}, {"id": 1930257, "uid": "B00M3D8IYM"}, {"id": 1930098, "uid": "B00M3D8IPQ"}, {"id": 1932010, "uid": "B00M3D...` | `doom-2016/igdb.games.search.json` |
| `$[].external_games[]` | `object` | `{"id": 1934827, "uid": "B01CTWZEDE"}` | `doom-2016/igdb.games.search.json` |
| `$[].franchises` | `array` | `[798]` | `doom-2016/igdb.games.search.json` |
| `$[].game_engines` | `array` | `[172]` | `doom-2016/igdb.games.search.json` |
| `$[].game_modes` | `array` | `[1, 2, 3]` | `doom-2016/igdb.games.search.json` |
| `$[].genres` | `array` | `[5, 9]` | `doom-2016/igdb.games.search.json` |
| `$[].involved_companies` | `array` | `[198290, 198292, 198294, 198295, 198288, 198289, 198293, 198291]` | `doom-2016/igdb.games.search.json` |
| `$[].keywords` | `array` | `[3, 129, 175, 274, 558, 578, 905, 923, 977, 1069, 1158, 1293, 1299, 1308, 1333, 1898, 1986, 2153, 2199, 2498, 2543, 2746, 3203, 3486, 3831, 4004, 4134, 4150, 4213, 4245, 4282, 4...` | `doom-2016/igdb.games.search.json` |
| `$[].platforms` | `array` | `[48, 6, 49]` | `doom-2016/igdb.games.search.json` |
| `$[].player_perspectives` | `array` | `[1]` | `doom-2016/igdb.games.search.json` |
| `$[].ports` | `array` | `[76217]` | `doom-2016/igdb.games.search.json` |
| `$[].similar_games` | `array` | `[1006, 19531, 2031, 571, 533, 9498, 3188, 9727, 7342, 231]` | `doom-2016/igdb.games.search.json` |
| `$[].themes` | `array` | `[1, 18, 19]` | `doom-2016/igdb.games.search.json` |
| `$[].websites` | `array` | `[61572, 799146, 730848, 904973, 904974, 904975, 904976, 799147, 61571, 5601, 43279, 51206, 332473, 43278, 61570]` | `doom-2016/igdb.games.search.json` |
| `best` | `object` | `{"id": 7351, "aggregated_rating": 86.96875, "aggregated_rating_count": 28, "dlcs": [25571, 26558, 22431], "external_games": [{"id": 1934827, "uid": "B01CTWZEDE"}, {"id": 138274,...` | `doom-2016/igdb.best.json` |
| `best.aggregated_rating` | `number` | `86.96875` | `doom-2016/igdb.best.json` |
| `best.aggregated_rating_count` | `number` | `28` | `doom-2016/igdb.best.json` |
| `best.dlcs` | `array` | `[25571, 26558, 22431]` | `doom-2016/igdb.best.json` |
| `best.dlcs[]` | `number` | `25571` | `doom-2016/igdb.best.json` |
| `best.external_games` | `array` | `[{"id": 1934827, "uid": "B01CTWZEDE"}, {"id": 138274, "uid": "20654"}, {"id": 1930257, "uid": "B00M3D8IYM"}, {"id": 1930098, "uid": "B00M3D8IPQ"}, {"id": 1932010, "uid": "B00M3D...` | `doom-2016/igdb.best.json` |
| `best.external_games[]` | `object` | `{"id": 1934827, "uid": "B01CTWZEDE"}` | `doom-2016/igdb.best.json` |
| `best.external_games[].id` | `number` | `1934827` | `doom-2016/igdb.best.json` |
| `best.external_games[].uid` | `string` | `"B01CTWZEDE"` | `doom-2016/igdb.best.json` |
| `best.first_release_date` | `number` | `1463011200` | `doom-2016/igdb.best.json` |
| `best.franchises` | `array` | `[798]` | `doom-2016/igdb.best.json` |
| `best.franchises[]` | `number` | `798` | `doom-2016/igdb.best.json` |
| `best.game_engines` | `array` | `[172]` | `doom-2016/igdb.best.json` |
| `best.game_engines[]` | `number` | `172` | `doom-2016/igdb.best.json` |
| `best.game_modes` | `array` | `[1, 2, 3]` | `doom-2016/igdb.best.json` |
| `best.game_modes[]` | `number` | `1` | `doom-2016/igdb.best.json` |
| `best.genres` | `array` | `[5, 9]` | `doom-2016/igdb.best.json` |
| `best.genres[]` | `number` | `5` | `doom-2016/igdb.best.json` |
| `best.id` | `number` | `7351` | `doom-2016/igdb.best.json` |
| `best.involved_companies` | `array` | `[198290, 198292, 198294, 198295, 198288, 198289, 198293, 198291]` | `doom-2016/igdb.best.json` |
| `best.involved_companies[]` | `number` | `198290` | `doom-2016/igdb.best.json` |
| `best.keywords` | `array` | `[3, 129, 175, 274, 558, 578, 905, 923, 977, 1069, 1158, 1293, 1299, 1308, 1333, 1898, 1986, 2153, 2199, 2498, 2543, 2746, 3203, 3486, 3831, 4004, 4134, 4150, 4213, 4245, 4282, 4...` | `doom-2016/igdb.best.json` |
| `best.keywords[]` | `number` | `3` | `doom-2016/igdb.best.json` |
| `best.name` | `string` | `"Doom"` | `doom-2016/igdb.best.json` |
| `best.platforms` | `array` | `[48, 6, 49]` | `doom-2016/igdb.best.json` |
| `best.platforms[]` | `number` | `48` | `doom-2016/igdb.best.json` |
| `best.player_perspectives` | `array` | `[1]` | `doom-2016/igdb.best.json` |
| `best.player_perspectives[]` | `number` | `1` | `doom-2016/igdb.best.json` |
| `best.ports` | `array` | `[76217]` | `doom-2016/igdb.best.json` |
| `best.ports[]` | `number` | `76217` | `doom-2016/igdb.best.json` |
| `best.rating` | `number` | `85.30925725648731` | `doom-2016/igdb.best.json` |
| `best.rating_count` | `number` | `1794` | `doom-2016/igdb.best.json` |
| `best.similar_games` | `array` | `[1006, 19531, 2031, 571, 533, 9498, 3188, 9727, 7342, 231]` | `doom-2016/igdb.best.json` |
| `best.similar_games[]` | `number` | `1006` | `doom-2016/igdb.best.json` |
| `best.slug` | `string` | `"doom--2"` | `doom-2016/igdb.best.json` |
| `best.storyline` | `string` | `"You’ve come here for a reason. The Union Aerospace Corporation’s massive research facility on Mars is overwhelmed by fierce and powerful demons, and only one person stands betw...` | `doom-2016/igdb.best.json` |
| `best.summary` | `string` | `"Developed by id software, the studio that pioneered the first-person shooter genre and created multiplayer Deathmatch, Doom returns as a brutally fun and challenging modern-day...` | `doom-2016/igdb.best.json` |
| `best.themes` | `array` | `[1, 18, 19]` | `doom-2016/igdb.best.json` |
| `best.themes[]` | `number` | `1` | `doom-2016/igdb.best.json` |
| `best.total_rating` | `number` | `86.13900362824366` | `doom-2016/igdb.best.json` |
| `best.total_rating_count` | `number` | `1822` | `doom-2016/igdb.best.json` |
| `best.websites` | `array` | `[61572, 799146, 730848, 904973, 904974, 904975, 904976, 799147, 61571, 5601, 43279, 51206, 332473, 43278, 61570]` | `doom-2016/igdb.best.json` |
| `best.websites[]` | `number` | `61572` | `doom-2016/igdb.best.json` |
| `score` | `number` | `100` | `doom-2016/igdb.best.json` |

## Steam Store API

Docs:
- Steam Web/Store API reference (unofficial): https://steamcommunity.com/dev

### GET /api/storesearch

| Path | Description | Observed types | Example | Example file |
|---|---|---|---|---|
| `items[].id` | AppID | `number` | `3017860` | `doom-2016/steam.storesearch.json` |
| `items[].name` | Store title | `string` | `"DOOM: The Dark Ages"` | `doom-2016/steam.storesearch.json` |
| `items[].type` | Item type (app/sub/etc) | `string` | `"app"` | `doom-2016/steam.storesearch.json` |
| `total` | Total results (may be present) | `number` | `10` | `doom-2016/steam.storesearch.json` |

#### Observed in examples (not yet described)

| Path | Observed types | Example | Example file |
|---|---|---|---|
| `$` | `object` | `{"total": 10, "items": [{"type": "app", "name": "DOOM: The Dark Ages", "id": 3017860, "price": {"currency": "USD", "initial": 6999, "final": 6999}, "tiny_image": "https://shared...` | `doom-2016/steam.storesearch.json` |
| `items` | `array` | `[{"type": "app", "name": "DOOM: The Dark Ages", "id": 3017860, "price": {"currency": "USD", "initial": 6999, "final": 6999}, "tiny_image": "https://shared.akamai.steamstatic.com...` | `doom-2016/steam.storesearch.json` |
| `items[]` | `object` | `{"type": "app", "name": "DOOM: The Dark Ages", "id": 3017860, "price": {"currency": "USD", "initial": 6999, "final": 6999}, "tiny_image": "https://shared.akamai.steamstatic.com/...` | `doom-2016/steam.storesearch.json` |
| `items[].controller_support` | `string` | `"full"` | `doom-2016/steam.storesearch.json` |
| `items[].metascore` | `string` | `""` | `doom-2016/steam.storesearch.json` |
| `items[].platforms` | `object` | `{"windows": true, "mac": false, "linux": false}` | `doom-2016/steam.storesearch.json` |
| `items[].platforms.linux` | `boolean` | `false` | `doom-2016/steam.storesearch.json` |
| `items[].platforms.mac` | `boolean` | `false` | `doom-2016/steam.storesearch.json` |
| `items[].platforms.windows` | `boolean` | `true` | `doom-2016/steam.storesearch.json` |
| `items[].price` | `object` | `{"currency": "USD", "initial": 6999, "final": 6999}` | `doom-2016/steam.storesearch.json` |
| `items[].price.currency` | `string` | `"USD"` | `doom-2016/steam.storesearch.json` |
| `items[].price.final` | `number` | `6999` | `doom-2016/steam.storesearch.json` |
| `items[].price.initial` | `number` | `6999` | `doom-2016/steam.storesearch.json` |
| `items[].streamingvideo` | `boolean` | `false` | `doom-2016/steam.storesearch.json` |
| `items[].tiny_image` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/3017860/4b39d554fb3b3a48ff02e2f05bba7186a58052ce/capsule_231x87.jpg?t=1764606093"` | `doom-2016/steam.storesearch.json` |

### GET /api/appdetails

| Path | Description | Observed types | Example | Example file |
|---|---|---|---|---|
| `$.<appid>.success` | Whether the request for this appid succeeded (wrapper) | `boolean` | `true` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.name` | Store title | `string` | `"DOOM"` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.type` | Item type (game/dlc/etc) | `string` | `"game"` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.is_free` | Is free-to-play flag | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.release_date.date` | Release date string | `string` | `"12 May, 2016"` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.platforms.windows` | Windows support flag | `boolean` | `true` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.platforms.mac` | macOS support flag | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.platforms.linux` | Linux support flag | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.categories[].description` | Category descriptions | `string` | `"Single-player"` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.genres[].description` | Genre descriptions | `string` | `"Action"` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.metacritic.score` | Metacritic score (if present) | `number` | `85` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.developers[]` | Developer names | `string` | `"id Software"` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.publishers[]` | Publisher names | `string` | `"Bethesda Softworks"` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.recommendations.total` | Recommendation/review count | `number` | `148323` | `doom-2016/steam.appdetails.json` |
| `$.<appid>.data.price_overview.final_formatted` | Price string (if not free) | `string` | `"$11.99 USD"` | `doom-2016/steam.appdetails.json` |

#### Observed in examples (not yet described)

| Path | Observed types | Example | Example file |
|---|---|---|---|
| `$` | `object` | `{"379720": {"success": true, "data": {"type": "game", "name": "DOOM", "steam_appid": 379720, "required_age": "17", "is_free": false, "dlc": [1195480], "detailed_description": "<...` | `doom-2016/steam.appdetails.json` |
| `379720` | `object` | `{"success": true, "data": {"type": "game", "name": "DOOM", "steam_appid": 379720, "required_age": "17", "is_free": false, "dlc": [1195480], "detailed_description": "<h1>2016 Gam...` | `doom-2016/steam.appdetails.json` |
| `379720.data` | `object` | `{"type": "game", "name": "DOOM", "steam_appid": 379720, "required_age": "17", "is_free": false, "dlc": [1195480], "detailed_description": "<h1>2016 Game Awards Winner</h1><p><sp...` | `doom-2016/steam.appdetails.json` |
| `379720.data.about_the_game` | `string` | `"Developed by id software, the studio that pioneered the first-person shooter genre and created multiplayer Deathmatch, DOOM returns as a brutally fun and challenging modern-day...` | `doom-2016/steam.appdetails.json` |
| `379720.data.achievements` | `object` | `{"total": 54, "highlighted": [{"name": "Shoot it Until it Dies", "path": "https://cdn.akamai.steamstatic.com/steamcommunity/public/images/apps/379720/70b24a89d14d808b13ea75e7102...` | `doom-2016/steam.appdetails.json` |
| `379720.data.achievements.highlighted` | `array` | `[{"name": "Shoot it Until it Dies", "path": "https://cdn.akamai.steamstatic.com/steamcommunity/public/images/apps/379720/70b24a89d14d808b13ea75e7102515399003ce1a.jpg"}, {"name":...` | `doom-2016/steam.appdetails.json` |
| `379720.data.achievements.highlighted[]` | `object` | `{"name": "Shoot it Until it Dies", "path": "https://cdn.akamai.steamstatic.com/steamcommunity/public/images/apps/379720/70b24a89d14d808b13ea75e7102515399003ce1a.jpg"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.achievements.highlighted[].name` | `string` | `"Shoot it Until it Dies"` | `doom-2016/steam.appdetails.json` |
| `379720.data.achievements.highlighted[].path` | `string` | `"https://cdn.akamai.steamstatic.com/steamcommunity/public/images/apps/379720/70b24a89d14d808b13ea75e7102515399003ce1a.jpg"` | `doom-2016/steam.appdetails.json` |
| `379720.data.achievements.total` | `number` | `54` | `doom-2016/steam.appdetails.json` |
| `379720.data.background` | `string` | `"https://store.akamai.steamstatic.com/images/storepagebackground/app/379720?t=1750784856"` | `doom-2016/steam.appdetails.json` |
| `379720.data.background_raw` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/page_bg_raw.jpg?t=1750784856"` | `doom-2016/steam.appdetails.json` |
| `379720.data.capsule_image` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/capsule_231x87.jpg?t=1750784856"` | `doom-2016/steam.appdetails.json` |
| `379720.data.capsule_imagev5` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/capsule_184x69.jpg?t=1750784856"` | `doom-2016/steam.appdetails.json` |
| `379720.data.categories` | `array` | `[{"id": 2, "description": "Single-player"}, {"id": 1, "description": "Multi-player"}, {"id": 9, "description": "Co-op"}, {"id": 22, "description": "Steam Achievements"}, {"id": ...` | `doom-2016/steam.appdetails.json` |
| `379720.data.categories[]` | `object` | `{"id": 2, "description": "Single-player"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.categories[].description` | `string` | `"Single-player"` | `doom-2016/steam.appdetails.json` |
| `379720.data.categories[].id` | `number` | `2` | `doom-2016/steam.appdetails.json` |
| `379720.data.content_descriptors` | `object` | `{"ids": [2, 5], "notes": "Blood and Gore\r\nViolence\r\nLanguage\r\nHorror"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.content_descriptors.ids` | `array` | `[2, 5]` | `doom-2016/steam.appdetails.json` |
| `379720.data.content_descriptors.ids[]` | `number` | `2` | `doom-2016/steam.appdetails.json` |
| `379720.data.content_descriptors.notes` | `string` | `"Blood and Gore\r\nViolence\r\nLanguage\r\nHorror"` | `doom-2016/steam.appdetails.json` |
| `379720.data.demos` | `array` | `[{"appid": 479030, "description": ""}]` | `doom-2016/steam.appdetails.json` |
| `379720.data.demos[]` | `object` | `{"appid": 479030, "description": ""}` | `doom-2016/steam.appdetails.json` |
| `379720.data.demos[].appid` | `number` | `479030` | `doom-2016/steam.appdetails.json` |
| `379720.data.demos[].description` | `string` | `""` | `doom-2016/steam.appdetails.json` |
| `379720.data.detailed_description` | `string` | `"<h1>2016 Game Awards Winner</h1><p><span class=\"bb_img_ctn\"><img class=\"bb_img\" src=\"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/extras/91e8d...` | `doom-2016/steam.appdetails.json` |
| `379720.data.developers` | `array` | `["id Software"]` | `doom-2016/steam.appdetails.json` |
| `379720.data.developers[]` | `string` | `"id Software"` | `doom-2016/steam.appdetails.json` |
| `379720.data.dlc` | `array` | `[1195480]` | `doom-2016/steam.appdetails.json` |
| `379720.data.dlc[]` | `number` | `1195480` | `doom-2016/steam.appdetails.json` |
| `379720.data.genres` | `array` | `[{"id": "1", "description": "Action"}]` | `doom-2016/steam.appdetails.json` |
| `379720.data.genres[]` | `object` | `{"id": "1", "description": "Action"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.genres[].description` | `string` | `"Action"` | `doom-2016/steam.appdetails.json` |
| `379720.data.genres[].id` | `string` | `"1"` | `doom-2016/steam.appdetails.json` |
| `379720.data.header_image` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/header.jpg?t=1750784856"` | `doom-2016/steam.appdetails.json` |
| `379720.data.is_free` | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `379720.data.legal_notice` | `string` | `"© 2016 Bethesda Softworks LLC, a ZeniMax Media company.  Bethesda, Bethesda Softworks, ZeniMax and related logos are registered trademarks or trademarks of ZeniMax Media Inc. i...` | `doom-2016/steam.appdetails.json` |
| `379720.data.linux_requirements` | `array` | `[]` | `doom-2016/steam.appdetails.json` |
| `379720.data.mac_requirements` | `array` | `[]` | `doom-2016/steam.appdetails.json` |
| `379720.data.metacritic` | `object` | `{"score": 85, "url": "https://www.metacritic.com/game/pc/doom?ftag=MCD-06-10aaa1f"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.metacritic.score` | `number` | `85` | `doom-2016/steam.appdetails.json` |
| `379720.data.metacritic.url` | `string` | `"https://www.metacritic.com/game/pc/doom?ftag=MCD-06-10aaa1f"` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies` | `array` | `[{"id": 256664074, "name": "DOOM Launch Trailer", "thumbnail": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/256664074/movie.293x165.jpg?t=1462985736", "da...` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[]` | `object` | `{"id": 256664074, "name": "DOOM Launch Trailer", "thumbnail": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/256664074/movie.293x165.jpg?t=1462985736", "das...` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[].dash_av1` | `string` | `"https://video.akamai.steamstatic.com/store_trailers/379720/58691/954a3b4e4cfc0e0015a79e5316b66f49a82edd83/1750521227/dash_av1.mpd?t=1462985736"` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[].dash_h264` | `string` | `"https://video.akamai.steamstatic.com/store_trailers/379720/58691/954a3b4e4cfc0e0015a79e5316b66f49a82edd83/1750521227/dash_h264.mpd?t=1462985736"` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[].highlight` | `boolean` | `true` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[].hls_h264` | `string` | `"https://video.akamai.steamstatic.com/store_trailers/379720/58691/954a3b4e4cfc0e0015a79e5316b66f49a82edd83/1750521227/hls_264_master.m3u8?t=1462985736"` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[].id` | `number` | `256664074` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[].name` | `string` | `"DOOM Launch Trailer"` | `doom-2016/steam.appdetails.json` |
| `379720.data.movies[].thumbnail` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/256664074/movie.293x165.jpg?t=1462985736"` | `doom-2016/steam.appdetails.json` |
| `379720.data.name` | `string` | `"DOOM"` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups` | `array` | `[{"name": "default", "title": "Buy DOOM", "description": "", "selection_text": "Select a purchase option", "save_text": "", "display_type": 0, "is_recurring_subscription": "fals...` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[]` | `object` | `{"name": "default", "title": "Buy DOOM", "description": "", "selection_text": "Select a purchase option", "save_text": "", "display_type": 0, "is_recurring_subscription": "false...` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].description` | `string` | `""` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].display_type` | `number` | `0` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].is_recurring_subscription` | `string` | `"false"` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].name` | `string` | `"default"` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].save_text` | `string` | `""` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].selection_text` | `string` | `"Select a purchase option"` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs` | `array` | `[{"packageid": 103396, "percent_savings_text": " ", "percent_savings": 0, "option_text": "DOOM - $11.99 USD", "option_description": "", "can_get_free_license": "0", "is_free_lic...` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[]` | `object` | `{"packageid": 103396, "percent_savings_text": " ", "percent_savings": 0, "option_text": "DOOM - $11.99 USD", "option_description": "", "can_get_free_license": "0", "is_free_lice...` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].can_get_free_license` | `string` | `"0"` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].is_free_license` | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].option_description` | `string` | `""` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].option_text` | `string` | `"DOOM - $11.99 USD"` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].packageid` | `number` | `103396` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].percent_savings` | `number` | `0` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].percent_savings_text` | `string` | `" "` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].subs[].price_in_cents_with_discount` | `number` | `1199` | `doom-2016/steam.appdetails.json` |
| `379720.data.package_groups[].title` | `string` | `"Buy DOOM"` | `doom-2016/steam.appdetails.json` |
| `379720.data.packages` | `array` | `[103396]` | `doom-2016/steam.appdetails.json` |
| `379720.data.packages[]` | `number` | `103396` | `doom-2016/steam.appdetails.json` |
| `379720.data.pc_requirements` | `object` | `{"minimum": "<strong>Minimum:</strong><br><ul class=\"bb_ul\"><li><strong>OS *:</strong> Windows 7/8.1/10 (64-bit versions)<br></li><li><strong>Processor:</strong> Intel Core i5...` | `doom-2016/steam.appdetails.json` |
| `379720.data.pc_requirements.minimum` | `string` | `"<strong>Minimum:</strong><br><ul class=\"bb_ul\"><li><strong>OS *:</strong> Windows 7/8.1/10 (64-bit versions)<br></li><li><strong>Processor:</strong> Intel Core i5-2400/AMD FX...` | `doom-2016/steam.appdetails.json` |
| `379720.data.pc_requirements.recommended` | `string` | `"<strong>Recommended:</strong><br><ul class=\"bb_ul\"><li><strong>OS *:</strong> Windows 7/8.1/10 (64-bit versions)<br></li><li><strong>Processor:</strong> Intel Core i7-3770/AM...` | `doom-2016/steam.appdetails.json` |
| `379720.data.platforms` | `object` | `{"windows": true, "mac": false, "linux": false}` | `doom-2016/steam.appdetails.json` |
| `379720.data.platforms.linux` | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `379720.data.platforms.mac` | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `379720.data.platforms.windows` | `boolean` | `true` | `doom-2016/steam.appdetails.json` |
| `379720.data.price_overview` | `object` | `{"currency": "USD", "initial": 1199, "final": 1199, "discount_percent": 0, "initial_formatted": "", "final_formatted": "$11.99 USD"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.price_overview.currency` | `string` | `"USD"` | `doom-2016/steam.appdetails.json` |
| `379720.data.price_overview.discount_percent` | `number` | `0` | `doom-2016/steam.appdetails.json` |
| `379720.data.price_overview.final` | `number` | `1199` | `doom-2016/steam.appdetails.json` |
| `379720.data.price_overview.final_formatted` | `string` | `"$11.99 USD"` | `doom-2016/steam.appdetails.json` |
| `379720.data.price_overview.initial` | `number` | `1199` | `doom-2016/steam.appdetails.json` |
| `379720.data.price_overview.initial_formatted` | `string` | `""` | `doom-2016/steam.appdetails.json` |
| `379720.data.publishers` | `array` | `["Bethesda Softworks"]` | `doom-2016/steam.appdetails.json` |
| `379720.data.publishers[]` | `string` | `"Bethesda Softworks"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings` | `object` | `{"esrb": {"use_age_gate": "true", "required_age": "17", "rating": "m", "descriptors": "Blood and Gore, Intense Violence, Strong Language", "display_online_notice": "true"}, "peg...` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.crl` | `object` | `{"use_age_gate": "true", "required_age": "18", "rating": "18"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.crl.rating` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.crl.required_age` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.crl.use_age_gate` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.dejus` | `object` | `{"rating": "18", "descriptors": "Extreme Violence", "use_age_gate": "true", "required_age": "18"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.dejus.descriptors` | `string` | `"Extreme Violence"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.dejus.rating` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.dejus.required_age` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.dejus.use_age_gate` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.esrb` | `object` | `{"use_age_gate": "true", "required_age": "17", "rating": "m", "descriptors": "Blood and Gore, Intense Violence, Strong Language", "display_online_notice": "true"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.esrb.descriptors` | `string` | `"Blood and Gore, Intense Violence, Strong Language"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.esrb.display_online_notice` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.esrb.rating` | `string` | `"m"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.esrb.required_age` | `string` | `"17"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.esrb.use_age_gate` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.fpb` | `object` | `{"rating": "18", "use_age_gate": "true", "required_age": "18"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.fpb.rating` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.fpb.required_age` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.fpb.use_age_gate` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.nzoflc` | `object` | `{"use_age_gate": "true", "required_age": "16", "rating": "r16", "descriptors": "graphic violence, horror"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.nzoflc.descriptors` | `string` | `"graphic violence, horror"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.nzoflc.rating` | `string` | `"r16"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.nzoflc.required_age` | `string` | `"16"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.nzoflc.use_age_gate` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.oflc` | `object` | `{"rating": "r18", "descriptors": "High impact violence, blood and gore, online interactivity", "use_age_gate": "true", "required_age": "18"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.oflc.descriptors` | `string` | `"High impact violence, blood and gore, online interactivity"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.oflc.rating` | `string` | `"r18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.oflc.required_age` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.oflc.use_age_gate` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.pegi` | `object` | `{"use_age_gate": "true", "required_age": "18", "rating": "18", "descriptors": "Bad Language, Violence"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.pegi.descriptors` | `string` | `"Bad Language, Violence"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.pegi.rating` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.pegi.required_age` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.pegi.use_age_gate` | `string` | `"true"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.steam_germany` | `object` | `{"rating_generated": "1", "rating": "18", "required_age": "18", "banned": "0", "use_age_gate": "0", "descriptors": "Drastische Gewalt\nDerbe Sprache"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.steam_germany.banned` | `string` | `"0"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.steam_germany.descriptors` | `string` | `"Drastische Gewalt\nDerbe Sprache"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.steam_germany.rating` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.steam_germany.rating_generated` | `string` | `"1"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.steam_germany.required_age` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.steam_germany.use_age_gate` | `string` | `"0"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.usk` | `object` | `{"required_age": "18", "rating": "18"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.usk.rating` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.ratings.usk.required_age` | `string` | `"18"` | `doom-2016/steam.appdetails.json` |
| `379720.data.recommendations` | `object` | `{"total": 148323}` | `doom-2016/steam.appdetails.json` |
| `379720.data.recommendations.total` | `number` | `148323` | `doom-2016/steam.appdetails.json` |
| `379720.data.release_date` | `object` | `{"coming_soon": false, "date": "12 May, 2016"}` | `doom-2016/steam.appdetails.json` |
| `379720.data.release_date.coming_soon` | `boolean` | `false` | `doom-2016/steam.appdetails.json` |
| `379720.data.release_date.date` | `string` | `"12 May, 2016"` | `doom-2016/steam.appdetails.json` |
| `379720.data.required_age` | `string` | `"17"` | `doom-2016/steam.appdetails.json` |
| `379720.data.reviews` | `string` | `"“BEST SHOOTER OF 2016”<br>ROCK, PAPER, SHOTGUN<br><br>“BADASS”<br>PC GAMER<br><br>“NEAR PERFECT”<br>GAMECRATE<br>"` | `doom-2016/steam.appdetails.json` |
| `379720.data.screenshots` | `array` | `[{"id": 0, "path_thumbnail": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/ss_f989e793786bf1d6459da1139a484203efef1447.600x338.jpg?t=1750784856", "p...` | `doom-2016/steam.appdetails.json` |
| `379720.data.screenshots[]` | `object` | `{"id": 0, "path_thumbnail": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/ss_f989e793786bf1d6459da1139a484203efef1447.600x338.jpg?t=1750784856", "pa...` | `doom-2016/steam.appdetails.json` |
| `379720.data.screenshots[].id` | `number` | `0` | `doom-2016/steam.appdetails.json` |
| `379720.data.screenshots[].path_full` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/ss_f989e793786bf1d6459da1139a484203efef1447.1920x1080.jpg?t=1750784856"` | `doom-2016/steam.appdetails.json` |
| `379720.data.screenshots[].path_thumbnail` | `string` | `"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/379720/ss_f989e793786bf1d6459da1139a484203efef1447.600x338.jpg?t=1750784856"` | `doom-2016/steam.appdetails.json` |
| `379720.data.short_description` | `string` | `"Now includes all three premium DLC packs (Unto the Evil, Hell Followed, and Bloodfall), maps, modes, and weapons, as well as all feature updates including Arcade Mode, Photo Mo...` | `doom-2016/steam.appdetails.json` |
| `379720.data.steam_appid` | `number` | `379720` | `doom-2016/steam.appdetails.json` |
| `379720.data.support_info` | `object` | `{"url": "http://help.bethesda.net/", "email": ""}` | `doom-2016/steam.appdetails.json` |
| `379720.data.support_info.email` | `string` | `""` | `doom-2016/steam.appdetails.json` |
| `379720.data.support_info.url` | `string` | `"http://help.bethesda.net/"` | `doom-2016/steam.appdetails.json` |
| `379720.data.supported_languages` | `string` | `"English<strong>*</strong>, French<strong>*</strong>, Italian<strong>*</strong>, German<strong>*</strong>, Spanish - Spain<strong>*</strong>, Japanese<strong>*</strong>, Polish<...` | `doom-2016/steam.appdetails.json` |
| `379720.data.type` | `string` | `"game"` | `doom-2016/steam.appdetails.json` |
| `379720.data.website` | `string` | `"http://www.doom.com"` | `doom-2016/steam.appdetails.json` |
| `379720.success` | `boolean` | `true` | `doom-2016/steam.appdetails.json` |

## SteamSpy

Docs:
- SteamSpy API: https://steamspy.com/api.php

### GET /api.php?request=appdetails

| Path | Description | Observed types | Example | Example file |
|---|---|---|---|---|
| `owners` | Owner range string | `string` | `"5,000,000 .. 10,000,000"` | `doom-2016/steamspy.appdetails.json` |
| `players_forever` | Lifetime players | `` | `` | `` |
| `ccu` | Current concurrent users | `number` | `1590` | `doom-2016/steamspy.appdetails.json` |
| `average_forever` | Average playtime (minutes) | `number` | `1074` | `doom-2016/steamspy.appdetails.json` |
| `average_2weeks` | Average playtime last 2 weeks (minutes) | `number` | `80` | `doom-2016/steamspy.appdetails.json` |
| `median_2weeks` | Median playtime last 2 weeks (minutes) | `number` | `101` | `doom-2016/steamspy.appdetails.json` |
| `positive` | Positive ratings count | `number` | `198001` | `doom-2016/steamspy.appdetails.json` |
| `negative` | Negative ratings count | `number` | `9278` | `doom-2016/steamspy.appdetails.json` |
| `price` | Price in cents (when present) | `string` | `"1999"` | `doom-2016/steamspy.appdetails.json` |
| `initialprice` | Initial price in cents (when present) | `string` | `"1999"` | `doom-2016/steamspy.appdetails.json` |
| `languages` | Languages (free-form string) | `string` | `"English, French, Italian, German, Spanish - Spain, Japanese, Polish, Portuguese - Brazil, Russian, Traditional Chinese"` | `doom-2016/steamspy.appdetails.json` |
| `genre` | Genre (free-form string) | `string` | `"Action"` | `doom-2016/steamspy.appdetails.json` |

#### Observed in examples (not yet described)

| Path | Observed types | Example | Example file |
|---|---|---|---|
| `$` | `object` | `{"appid": 379720, "name": "DOOM", "developer": "id Software", "publisher": "Bethesda Softworks", "score_rank": "", "positive": 198001, "negative": 9278, "userscore": 0, "owners"...` | `doom-2016/steamspy.appdetails.json` |
| `appid` | `number` | `379720` | `doom-2016/steamspy.appdetails.json` |
| `developer` | `string` | `"id Software"` | `doom-2016/steamspy.appdetails.json` |
| `discount` | `string` | `"0"` | `doom-2016/steamspy.appdetails.json` |
| `median_forever` | `number` | `516` | `doom-2016/steamspy.appdetails.json` |
| `name` | `string` | `"DOOM"` | `doom-2016/steamspy.appdetails.json` |
| `publisher` | `string` | `"Bethesda Softworks"` | `doom-2016/steamspy.appdetails.json` |
| `score_rank` | `string` | `""` | `doom-2016/steamspy.appdetails.json` |
| `tags` | `object` | `{"FPS": 2977, "Gore": 2528, "Action": 2391, "Shooter": 2059, "Great Soundtrack": 1969, "Demons": 1953, "First-Person": 1799, "Multiplayer": 1577, "Fast-Paced": 1571, "Singleplay...` | `doom-2016/steamspy.appdetails.json` |
| `tags.Action` | `number` | `2391` | `doom-2016/steamspy.appdetails.json` |
| `tags.Atmospheric` | `number` | `1045` | `doom-2016/steamspy.appdetails.json` |
| `tags.Blood` | `number` | `881` | `doom-2016/steamspy.appdetails.json` |
| `tags.Classic` | `number` | `998` | `doom-2016/steamspy.appdetails.json` |
| `tags.Co-op` | `number` | `429` | `doom-2016/steamspy.appdetails.json` |
| `tags.Demons` | `number` | `1953` | `doom-2016/steamspy.appdetails.json` |
| `tags.Difficult` | `number` | `763` | `doom-2016/steamspy.appdetails.json` |
| `tags.FPS` | `number` | `2977` | `doom-2016/steamspy.appdetails.json` |
| `tags.Fast-Paced` | `number` | `1571` | `doom-2016/steamspy.appdetails.json` |
| `tags.First-Person` | `number` | `1799` | `doom-2016/steamspy.appdetails.json` |
| `tags.Gore` | `number` | `2528` | `doom-2016/steamspy.appdetails.json` |
| `tags.Great Soundtrack` | `number` | `1969` | `doom-2016/steamspy.appdetails.json` |
| `tags.Horror` | `number` | `1106` | `doom-2016/steamspy.appdetails.json` |
| `tags.Multiplayer` | `number` | `1577` | `doom-2016/steamspy.appdetails.json` |
| `tags.Remake` | `number` | `546` | `doom-2016/steamspy.appdetails.json` |
| `tags.Sci-fi` | `number` | `1284` | `doom-2016/steamspy.appdetails.json` |
| `tags.Shooter` | `number` | `2059` | `doom-2016/steamspy.appdetails.json` |
| `tags.Singleplayer` | `number` | `1564` | `doom-2016/steamspy.appdetails.json` |
| `tags.Violent` | `number` | `459` | `doom-2016/steamspy.appdetails.json` |
| `tags.Zombies` | `number` | `495` | `doom-2016/steamspy.appdetails.json` |
| `userscore` | `number` | `0` | `doom-2016/steamspy.appdetails.json` |

## HowLongToBeat

Docs:
- howlongtobeatpy (library): https://pypi.org/project/howlongtobeatpy/

### howlongtobeatpy object dump (derived)

| Path | Description | Observed types | Example | Example file |
|---|---|---|---|---|
| `game_id` | Game id (when provided by the library) | `number` | `2708` | `doom-2016/hltb.best.json` |
| `game_name` | Matched title | `string` | `"Doom"` | `doom-2016/hltb.best.json` |
| `main_story` | Main story time (hours) | `number` | `11.59` | `doom-2016/hltb.best.json` |
| `main_extra` | Main + extras time (hours) | `number` | `16.44` | `doom-2016/hltb.best.json` |
| `completionist` | Completionist time (hours) | `number` | `27.09` | `doom-2016/hltb.best.json` |
| `profile_platform` | Platform string (sometimes present) | `` | `` | `` |
| `release_world` | Release year (sometimes present) | `number` | `2016` | `doom-2016/hltb.best.json` |

#### Observed in examples (not yet described)

| Path | Observed types | Example | Example file |
|---|---|---|---|
| `$` | `object` | `{"game_id": 2708, "game_name": "Doom", "game_alias": "Doom, Doom 4, Doom 2016", "game_type": "game", "game_image_url": "https://howlongtobeat.com/games/doom_2016.jpg", "game_web...` | `doom-2016/hltb.best.json` |
| `all_styles` | `number` | `14.55` | `doom-2016/hltb.best.json` |
| `attempts` | `array` | `[{"term": "Doom (2016)", "results": []}, {"term": "Doom", "results": [{"game_id": 2708, "game_name": "Doom", "game_alias": "Doom, Doom 4, Doom 2016", "game_type": "game", "game_...` | `doom-2016/hltb.search.json` |
| `attempts[]` | `object` | `{"term": "Doom (2016)", "results": []}` | `doom-2016/hltb.search.json` |
| `attempts[].results` | `array` | `[]` | `doom-2016/hltb.search.json` |
| `attempts[].results[]` | `object` | `{"game_id": 2708, "game_name": "Doom", "game_alias": "Doom, Doom 4, Doom 2016", "game_type": "game", "game_image_url": "https://howlongtobeat.com/games/doom_2016.jpg", "game_web...` | `doom-2016/hltb.search.json` |
| `attempts[].results[].all_styles` | `number` | `14.55` | `doom-2016/hltb.search.json` |
| `attempts[].results[].completionist` | `number` | `27.09` | `doom-2016/hltb.search.json` |
| `attempts[].results[].complexity_lvl_co` | `boolean` | `false` | `doom-2016/hltb.search.json` |
| `attempts[].results[].complexity_lvl_combine` | `boolean` | `false` | `doom-2016/hltb.search.json` |
| `attempts[].results[].complexity_lvl_mp` | `boolean` | `true` | `doom-2016/hltb.search.json` |
| `attempts[].results[].complexity_lvl_sp` | `boolean` | `true` | `doom-2016/hltb.search.json` |
| `attempts[].results[].coop_time` | `number` | `4.59` | `doom-2016/hltb.search.json` |
| `attempts[].results[].game_alias` | `string` | `"Doom, Doom 4, Doom 2016"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].game_id` | `number` | `2708` | `doom-2016/hltb.search.json` |
| `attempts[].results[].game_image_url` | `string` | `"https://howlongtobeat.com/games/doom_2016.jpg"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].game_name` | `string` | `"Doom"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].game_type` | `string` | `"game"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].game_web_link` | `string` | `"https://howlongtobeat.com/game/2708"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content` | `object` | `{"game_id": 2708, "game_name": "Doom", "game_name_date": 1, "game_alias": "Doom, Doom 4, Doom 2016", "game_type": "game", "game_image": "doom_2016.jpg", "comp_lvl_combine": 0, "...` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_100` | `number` | `97528` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_100_count` | `number` | `670` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_all` | `number` | `52372` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_all_count` | `number` | `7001` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_lvl_co` | `number` | `0` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_lvl_combine` | `number` | `0` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_lvl_mp` | `number` | `1` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_lvl_sp` | `number` | `1` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_main` | `number` | `41730` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_main_count` | `number` | `3414` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_plus` | `number` | `59199` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.comp_plus_count` | `number` | `2917` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.count_backlog` | `number` | `17309` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.count_comp` | `number` | `20055` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.count_playing` | `number` | `260` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.count_retired` | `number` | `1139` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.count_review` | `number` | `5466` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.count_speedrun` | `number` | `6` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.game_alias` | `string` | `"Doom, Doom 4, Doom 2016"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.game_id` | `number` | `2708` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.game_image` | `string` | `"doom_2016.jpg"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.game_name` | `string` | `"Doom"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.game_name_date` | `number` | `1` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.game_type` | `string` | `"game"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.invested_co` | `number` | `16528` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.invested_co_count` | `number` | `9` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.invested_mp` | `number` | `29404` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.invested_mp_count` | `number` | `86` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.profile_platform` | `string` | `"Google Stadia, Nintendo Switch, PC, PlayStation 4, Xbox One"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.profile_popular` | `number` | `1307` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.release_world` | `number` | `2016` | `doom-2016/hltb.search.json` |
| `attempts[].results[].json_content.review_score` | `number` | `84` | `doom-2016/hltb.search.json` |
| `attempts[].results[].main_extra` | `number` | `16.44` | `doom-2016/hltb.search.json` |
| `attempts[].results[].main_story` | `number` | `11.59` | `doom-2016/hltb.search.json` |
| `attempts[].results[].mp_time` | `number` | `8.17` | `doom-2016/hltb.search.json` |
| `attempts[].results[].profile_dev` | `null` | `null` | `doom-2016/hltb.search.json` |
| `attempts[].results[].profile_platforms` | `array` | `["Google Stadia", "Nintendo Switch", "PC", "PlayStation 4", "Xbox One"]` | `doom-2016/hltb.search.json` |
| `attempts[].results[].profile_platforms[]` | `string` | `"Google Stadia"` | `doom-2016/hltb.search.json` |
| `attempts[].results[].release_world` | `number` | `2016` | `doom-2016/hltb.search.json` |
| `attempts[].results[].review_score` | `number` | `84` | `doom-2016/hltb.search.json` |
| `attempts[].results[].similarity` | `number` | `1.0` | `doom-2016/hltb.search.json` |
| `attempts[].term` | `string` | `"Doom (2016)"` | `doom-2016/hltb.search.json` |
| `complexity_lvl_co` | `boolean` | `false` | `doom-2016/hltb.best.json` |
| `complexity_lvl_combine` | `boolean` | `false` | `doom-2016/hltb.best.json` |
| `complexity_lvl_mp` | `boolean` | `true` | `doom-2016/hltb.best.json` |
| `complexity_lvl_sp` | `boolean` | `true` | `doom-2016/hltb.best.json` |
| `coop_time` | `number` | `4.59` | `doom-2016/hltb.best.json` |
| `game_alias` | `string` | `"Doom, Doom 4, Doom 2016"` | `doom-2016/hltb.best.json` |
| `game_image_url` | `string` | `"https://howlongtobeat.com/games/doom_2016.jpg"` | `doom-2016/hltb.best.json` |
| `game_type` | `string` | `"game"` | `doom-2016/hltb.best.json` |
| `game_web_link` | `string` | `"https://howlongtobeat.com/game/2708"` | `doom-2016/hltb.best.json` |
| `json_content` | `object` | `{"game_id": 2708, "game_name": "Doom", "game_name_date": 1, "game_alias": "Doom, Doom 4, Doom 2016", "game_type": "game", "game_image": "doom_2016.jpg", "comp_lvl_combine": 0, "...` | `doom-2016/hltb.best.json` |
| `json_content.comp_100` | `number` | `97528` | `doom-2016/hltb.best.json` |
| `json_content.comp_100_count` | `number` | `670` | `doom-2016/hltb.best.json` |
| `json_content.comp_all` | `number` | `52372` | `doom-2016/hltb.best.json` |
| `json_content.comp_all_count` | `number` | `7001` | `doom-2016/hltb.best.json` |
| `json_content.comp_lvl_co` | `number` | `0` | `doom-2016/hltb.best.json` |
| `json_content.comp_lvl_combine` | `number` | `0` | `doom-2016/hltb.best.json` |
| `json_content.comp_lvl_mp` | `number` | `1` | `doom-2016/hltb.best.json` |
| `json_content.comp_lvl_sp` | `number` | `1` | `doom-2016/hltb.best.json` |
| `json_content.comp_main` | `number` | `41730` | `doom-2016/hltb.best.json` |
| `json_content.comp_main_count` | `number` | `3414` | `doom-2016/hltb.best.json` |
| `json_content.comp_plus` | `number` | `59199` | `doom-2016/hltb.best.json` |
| `json_content.comp_plus_count` | `number` | `2917` | `doom-2016/hltb.best.json` |
| `json_content.count_backlog` | `number` | `17309` | `doom-2016/hltb.best.json` |
| `json_content.count_comp` | `number` | `20055` | `doom-2016/hltb.best.json` |
| `json_content.count_playing` | `number` | `260` | `doom-2016/hltb.best.json` |
| `json_content.count_retired` | `number` | `1139` | `doom-2016/hltb.best.json` |
| `json_content.count_review` | `number` | `5466` | `doom-2016/hltb.best.json` |
| `json_content.count_speedrun` | `number` | `6` | `doom-2016/hltb.best.json` |
| `json_content.game_alias` | `string` | `"Doom, Doom 4, Doom 2016"` | `doom-2016/hltb.best.json` |
| `json_content.game_id` | `number` | `2708` | `doom-2016/hltb.best.json` |
| `json_content.game_image` | `string` | `"doom_2016.jpg"` | `doom-2016/hltb.best.json` |
| `json_content.game_name` | `string` | `"Doom"` | `doom-2016/hltb.best.json` |
| `json_content.game_name_date` | `number` | `1` | `doom-2016/hltb.best.json` |
| `json_content.game_type` | `string` | `"game"` | `doom-2016/hltb.best.json` |
| `json_content.invested_co` | `number` | `16528` | `doom-2016/hltb.best.json` |
| `json_content.invested_co_count` | `number` | `9` | `doom-2016/hltb.best.json` |
| `json_content.invested_mp` | `number` | `29404` | `doom-2016/hltb.best.json` |
| `json_content.invested_mp_count` | `number` | `86` | `doom-2016/hltb.best.json` |
| `json_content.profile_platform` | `string` | `"Google Stadia, Nintendo Switch, PC, PlayStation 4, Xbox One"` | `doom-2016/hltb.best.json` |
| `json_content.profile_popular` | `number` | `1307` | `doom-2016/hltb.best.json` |
| `json_content.release_world` | `number` | `2016` | `doom-2016/hltb.best.json` |
| `json_content.review_score` | `number` | `84` | `doom-2016/hltb.best.json` |
| `mp_time` | `number` | `8.17` | `doom-2016/hltb.best.json` |
| `profile_dev` | `null` | `null` | `doom-2016/hltb.best.json` |
| `profile_platforms` | `array` | `["Google Stadia", "Nintendo Switch", "PC", "PlayStation 4", "Xbox One"]` | `doom-2016/hltb.best.json` |
| `profile_platforms[]` | `string` | `"Google Stadia"` | `doom-2016/hltb.best.json` |
| `review_score` | `number` | `84` | `doom-2016/hltb.best.json` |
| `similarity` | `number` | `1.0` | `doom-2016/hltb.best.json` |
| `term` | `string` | `"Doom"` | `doom-2016/hltb.search.json` |
