from __future__ import annotations


def test_steam_appdetails_success_false_is_negative_cached(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    calls = {"appdetails": 0}

    def fake_get(_self, url, params=None, timeout=None):
        assert "appdetails" in url
        calls["appdetails"] += 1

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                # success=false is still a real response and should be negative-cached.
                return {"999": {"success": False}}

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    assert client.get_app_details(999) is None
    assert client.get_app_details(999) is None
    assert calls["appdetails"] == 1


def test_igdb_get_by_id_missing_is_negative_cached(tmp_path, monkeypatch):
    from game_catalog_builder.clients.igdb_client import IGDBClient

    calls = {"post": 0}

    def fake_post(_self, url, headers=None, data=None, timeout=None):
        assert "/games" in url
        calls["post"] += 1

        class Resp:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                # Empty response list => id not found.
                return []

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.post", fake_post)

    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=tmp_path / "igdb_cache.json",
        min_interval_s=0.0,
    )
    # Avoid OAuth in test.
    client._token = "t"

    assert client.get_by_id("123") is None
    assert client.get_by_id("123") is None
    assert calls["post"] == 1


def test_rawg_get_by_id_invalid_payload_is_negative_cached(tmp_path, monkeypatch):
    from game_catalog_builder.clients.rawg_client import RAWGClient

    calls = {"get": 0}

    def fake_get(_self, url, params=None, timeout=None):
        calls["get"] += 1

        class Resp:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                # Real payload, but not a valid game object.
                return {"detail": "Not found."}

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = RAWGClient(api_key="k", cache_path=tmp_path / "rawg_cache.json", min_interval_s=0.0)
    assert client.get_by_id("999") is None
    assert client.get_by_id("999") is None
    assert calls["get"] == 1
