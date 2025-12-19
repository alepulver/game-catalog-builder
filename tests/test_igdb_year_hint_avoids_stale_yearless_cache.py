from __future__ import annotations


def test_igdb_year_hint_does_not_use_yearless_cache_when_year_mismatches(tmp_path, monkeypatch):
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

    # Seed a stale yearless mapping "en:fallout" -> 76 (2018), but request year_hint=1997.
    client._by_name = {"en:fallout": "en:999"}
    client._by_id = {"en:999": {"IGDB_ID": "999", "IGDB_Name": "Fallout 76", "IGDB_Year": "2018"}}

    calls = {"post": 0}

    def fake_post(endpoint: str, query: str):
        calls["post"] += 1
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
    assert out["IGDB_Name"] == "Fallout"
    assert calls["post"] == 1
