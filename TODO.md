
- Create a cross-check mechahism to verify all providers fetched the same title
- Separate the process of obtaining and confirming provider IDs for each game, from data fetching process (store the ID only next to user data, then join); show matching score to spot games not found
- Make user data (input) and output columns configurable; for the latter, keep a schema or flat list of available data per provider with description (so they can be mapped to the output)
- Keep working on derived metrics with ChatGPT (tags, estimated popularity, users, production level, replayability, mods, "must play classic", etc)
- Investigate localization (how to retrieve results in other languages if available); would impact caching
