from __future__ import annotations


def test_igdb_year_hint_uses_year_window_query_first(tmp_path, monkeypatch):
    from game_catalog_builder.clients.igdb_client import IGDBClient

    monkeypatch.setattr(
        "game_catalog_builder.clients.igdb_client.IGDBClient._ensure_token", lambda self: None
    )

    cache_path = tmp_path / "igdb_cache.json"
    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=cache_path,
        language="en",
        min_interval_s=0.0,
    )
    client._token = "t"

    calls = {"post": 0}
    queries: list[str] = []

    def fake_post(endpoint: str, query: str):
        calls["post"] += 1
        queries.append(query)
        return [
            {
                "id": 1,
                "name": "Fallout",
                "first_release_date": 875750400,  # 1997-10-01 UTC
                "genres": [],
                "themes": [],
                "game_modes": [],
                "player_perspectives": [],
                "franchises": [],
                "game_engines": [],
                "external_games": [],
            }
        ]

    monkeypatch.setattr(client, "_post", fake_post)

    out = client.search("Fallout", year_hint=1997)
    assert out is not None
    assert out["igdb.name"] == "Fallout"
    assert calls["post"] == 1
    assert any("first_release_date >=" in q for q in queries)
