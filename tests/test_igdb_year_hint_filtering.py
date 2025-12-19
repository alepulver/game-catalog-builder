from __future__ import annotations


def test_igdb_search_uses_year_window_then_falls_back(tmp_path, monkeypatch):
    from game_catalog_builder.clients.igdb_client import IGDBClient

    monkeypatch.setattr(
        "game_catalog_builder.clients.igdb_client.IGDBClient._ensure_token", lambda self: None
    )

    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=tmp_path / "igdb_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    client._token = "t"

    queries: list[str] = []

    def fake_post(endpoint: str, query: str):
        assert endpoint == "games"
        queries.append(query)
        if "where first_release_date" in query:
            return []  # force fallback
        return [
            {
                "id": 42,
                "name": "Example Game",
                "first_release_date": 946684800,  # 2000-01-01 UTC
                "genres": [{"name": "Shooter"}],
                "themes": [{"name": "Action"}],
                "game_modes": [{"name": "Single player"}],
                "player_perspectives": [{"name": "First person"}],
                "franchises": [],
                "game_engines": [],
                "external_games": [],
            }
        ]

    monkeypatch.setattr(client, "_post", fake_post)

    out = client.search("Example Game (2000)", year_hint=2000)
    assert out is not None
    assert out["IGDB_ID"] == "42"
    assert len(queries) == 2
    assert "where first_release_date" in queries[0]
    assert "where first_release_date" not in queries[1]
