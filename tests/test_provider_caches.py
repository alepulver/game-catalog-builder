from __future__ import annotations

import json


def test_steam_cache_is_id_based(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(url, params=None, timeout=None):
        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                if "storesearch" in url:
                    return {"items": [{"id": 123, "name": "Example Game", "type": "app"}]}
                if "appdetails" in url:
                    return {
                        "123": {"success": True, "data": {"name": "Example Game", "is_free": True}}
                    }
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
    assert "by_query" in raw and "by_id" in raw
    assert "by_details" not in raw
    assert raw["by_id"]["123"]["name"] == "Example Game"
    assert any(
        k.startswith("l:english|cc:US|term:Example Game") for k in raw.get("by_query", {}).keys()
    )


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
                # Detail endpoint (by id).
                if str(url).rstrip("/").endswith("/999"):
                    return {
                        "id": 999,
                        "name": "Example Game",
                        "released": "2019-01-01",
                        "description": "<p>Example</p>",
                        "description_raw": "Example",
                        "alternative_names": ["Example Alt"],
                    }
                # Simulate strict search returning no results, then fallback returning results.
                if params and params.get("search_exact") == 1:
                    return {"results": []}
                return {"results": [{"id": 999, "name": "Example Game", "released": "2019-01-01"}]}

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    cache_path = tmp_path / "rawg_cache.json"
    client = RAWGClient(api_key="x", cache_path=cache_path, language="en", min_interval_s=0.0)

    best = client.search("Example Game")
    assert best["id"] == 999

    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert raw["by_id"]["en:999"]["name"] == "Example Game"
    assert any(k.startswith("lang:en|search:Example Game|") for k in raw.get("by_query", {}).keys())


def test_rawg_falls_back_to_loose_when_strict_matches_are_irrelevant(tmp_path, monkeypatch):
    from game_catalog_builder.clients.rawg_client import RAWGClient

    calls = {"loose": 0}

    def fake_get(url, params=None, timeout=None):
        assert "rawg.io" in url

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                # Detail endpoint (by id).
                if str(url).rstrip("/").endswith("/2"):
                    return {
                        "id": 2,
                        "name": "Quake II",
                        "released": "1997-12-09",
                        "description": "<p>Example</p>",
                        "description_raw": "Example",
                        "alternative_names": [],
                    }
                calls["loose"] += 1
                return {"results": [{"id": 2, "name": "Quake II", "released": "1997-12-09"}]}

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    client = RAWGClient(
        api_key="x",
        cache_path=tmp_path / "rawg_cache.json",
        language="en",
        min_interval_s=0.0,
    )

    best = client.search("Quake II")
    assert best is not None
    assert best["id"] == 2
    assert calls["loose"] == 1


def test_pick_best_match_prefers_exact_over_year_adjustment():
    from game_catalog_builder.utils.utilities import pick_best_match

    candidates = [
        {"id": 1, "name": "Mafia", "released": "2020-09-25"},
        {"id": 2, "name": "Mafia: The Game", "released": "2002-08-29"},
    ]

    def year_getter(obj):
        released = str(obj.get("released", "") or "")
        return int(released[:4]) if len(released) >= 4 and released[:4].isdigit() else None

    best, score, _ = pick_best_match(
        "Mafia", candidates, name_key="name", year_hint=2002, year_getter=year_getter
    )
    assert best is not None
    assert best["name"] == "Mafia"
    assert score == 100


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
    monkeypatch.setattr(client, "_post", fake_post)

    enriched = client.search("Example Game")
    assert enriched["IGDB_ID"] == "42"
    assert enriched["IGDB_Genres"] == "Shooter"

    raw = json.loads((tmp_path / "igdb_cache.json").read_text(encoding="utf-8"))
    assert raw["by_id"]["en:42"]["id"] == 42
    assert len(raw.get("by_query", {})) >= 1


def test_hltb_requires_stable_id(tmp_path, monkeypatch):
    from game_catalog_builder.clients.hltb_client import HLTBClient

    class FakeResult:
        game_id = None
        game_name = "Example Game"
        main_story = "1"
        main_extra = ""
        completionist = ""

    client = HLTBClient(cache_path=tmp_path / "hltb_cache.json")
    monkeypatch.setattr(client.client, "search", lambda name: [FakeResult()])

    data = client.search("Example Game")
    assert data is None
