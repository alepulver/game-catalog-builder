from __future__ import annotations

import json


def test_steam_batch_fills_cache_and_single_uses_cache(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    calls = {"appdetails": 0}

    def fake_get(_self, url, params=None, timeout=None):
        assert "appdetails" in url
        calls["appdetails"] += 1
        appids = ""
        if "appids=" in url:
            appids = url.split("appids=", 1)[1].split("&", 1)[0]
        ids = [s.strip() for s in appids.split(",") if s.strip()]
        payload = {}
        for s in ids:
            payload[s] = {
                "success": True,
                "data": {
                    "steam_appid": int(s),
                    "name": f"Game {s}",
                    "type": "game",
                    "is_free": True,
                },
            }

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                return payload

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    cache_path = tmp_path / "steam_cache.json"
    c1 = SteamClient(cache_path=cache_path, min_interval_s=0.0)
    out = c1.get_app_details_many([1, 2, 3])
    assert out[1]["name"] == "Game 1"
    assert calls["appdetails"] == 1
    c1._cache_io.flush()

    # Cache file should have by_id entries for all.
    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert "by_id" in raw and "1" in raw["by_id"] and "2" in raw["by_id"] and "3" in raw["by_id"]

    # New client should read cache and avoid any network calls.
    def no_get(_self, *_args, **_kwargs):
        raise AssertionError("unexpected requests.get call; should use cache")

    monkeypatch.setattr("requests.sessions.Session.get", no_get)
    c2 = SteamClient(cache_path=cache_path, min_interval_s=0.0)
    d = c2.get_app_details(2)
    assert d is not None
    assert d["name"] == "Game 2"


def test_igdb_batch_fills_cache_and_single_uses_cache(tmp_path, monkeypatch):
    from game_catalog_builder.clients.igdb_client import IGDBClient

    calls = {"post": 0}

    def fake_post(_self, url, headers=None, data=None, timeout=None):
        calls["post"] += 1
        assert url.endswith("/v4/games")

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                return [
                    {"id": 1, "name": "One", "first_release_date": 0},
                    {"id": 2, "name": "Two", "first_release_date": 0},
                ]

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.post", fake_post)

    cache_path = tmp_path / "igdb_cache.json"
    c1 = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=cache_path,
        language="en",
        min_interval_s=0.0,
    )
    c1._token = "token"
    out = c1.get_by_ids([1, 2])
    assert out["1"]["igdb.name"] == "One"
    assert calls["post"] == 1
    c1._cache_io.flush()

    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert "by_id" in raw
    assert "en:1" in raw["by_id"]
    assert "en:2" in raw["by_id"]

    # New client should read by_id cache and avoid POST calls.
    def no_post(_self, *_args, **_kwargs):
        raise AssertionError("unexpected requests.post call; should use cache")

    monkeypatch.setattr("requests.sessions.Session.post", no_post)
    c2 = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=cache_path,
        language="en",
        min_interval_s=0.0,
    )
    c2._token = "token"
    single = c2.get_by_id(2)
    assert single is not None
    assert single["igdb.id"] == "2"
    assert single["igdb.name"] == "Two"
