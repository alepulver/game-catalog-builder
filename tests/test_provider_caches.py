from __future__ import annotations

import json
from pathlib import Path


def test_steam_cache_is_id_based(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                if "storesearch" in url:
                    return {"items": [{"id": 123, "name": "Example Game", "type": "app"}]}
                if "appdetails" in url:
                    return {"123": {"success": True, "data": {"name": "Example Game", "is_free": True}}}
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    cache_path = tmp_path / "steam_cache.json"
    client = SteamClient(cache_path=cache_path, min_interval_s=0.0)

    best = client.search_appid("Example Game")
    assert best["id"] == 123
    details = client.get_app_details(123)
    assert details["name"] == "Example Game"

    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert "by_name" in raw and "by_id" in raw and "by_details" in raw
    assert raw["by_name"]["example game"] == "123"
    assert raw["by_id"]["123"]["id"] == 123
    assert raw["by_details"]["123"]["name"] == "Example Game"


def test_steam_negative_caching_avoids_repeat_search(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    calls = {"storesearch": 0}

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                calls["storesearch"] += 1
                return {"items": []}

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    cache_path = tmp_path / "steam_cache.json"
    client = SteamClient(cache_path=cache_path, min_interval_s=0.0)

    assert client.search_appid("No Such Game") is None
    assert client.search_appid("No Such Game") is None
    assert calls["storesearch"] == 1


def test_rawg_cache_is_id_based(tmp_path, monkeypatch):
    from game_catalog_builder.clients.rawg_client import RAWGClient

    def fake_get(url, params=None, timeout=None):
        assert "rawg.io" in url

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"results": [{"id": 999, "name": "Example Game", "released": "2019-01-01"}]}

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    cache_path = tmp_path / "rawg_cache.json"
    client = RAWGClient(api_key="x", cache_path=cache_path, language="en", min_interval_s=0.0)

    best = client.search("Example Game")
    assert best["id"] == 999

    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert raw["by_name"]["en:example game"] == "en:999"
    assert raw["by_id"]["en:999"]["name"] == "Example Game"


def test_igdb_cache_is_id_based(tmp_path, monkeypatch):
    from game_catalog_builder.clients.igdb_client import IGDBClient

    def fake_post(endpoint, query):
        assert endpoint == "games"
        return [
            {
                "id": 42,
                "name": "Example Game",
                "genres": [{"name": "Shooter"}],
                "themes": [{"name": "Action"}],
                "game_modes": [{"name": "Single player"}],
                "player_perspectives": [{"name": "First person"}],
                "franchises": [],
                "game_engines": [],
                "external_games": [],
            }
        ]

    monkeypatch.setattr("game_catalog_builder.clients.igdb_client.IGDBClient._ensure_token", lambda self: None)
    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=tmp_path / "igdb_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    client._token = "t"
    monkeypatch.setattr(client, "_post", fake_post)

    enriched = client.search("Example Game")
    assert enriched["IGDB_ID"] == "42"
    assert enriched["IGDB_Genres"] == "Shooter"

    raw = json.loads((tmp_path / "igdb_cache.json").read_text(encoding="utf-8"))
    assert raw["by_name"]["en:example game"] == "en:42"
    assert raw["by_id"]["en:42"]["IGDB_ID"] == "42"
