from __future__ import annotations


def test_steam_search_prefers_release_year_via_appdetails(tmp_path, monkeypatch):
    import json

    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return json.loads(json.dumps(self._payload))

        if "storesearch" in url:
            return Resp(
                {
                    "total": 2,
                    "items": [
                        {"id": 200, "name": "Doom 2", "type": "app"},
                        {"id": 100, "name": "Doom", "type": "app"},
                    ],
                }
            )

        if "appdetails" in url:
            appids = str((params or {}).get("appids"))
            if appids == "100":
                return Resp(
                    {
                        "100": {
                            "success": True,
                            "data": {
                                "type": "game",
                                "name": "Doom",
                                "release_date": {"date": "10 Dec, 1993"},
                            },
                        }
                    }
                )
            if appids == "200":
                return Resp(
                    {
                        "200": {
                            "success": True,
                            "data": {
                                "type": "game",
                                "name": "Doom 2",
                                "release_date": {"date": "30 Sep, 1994"},
                            },
                        }
                    }
                )
            raise AssertionError(f"unexpected appid {appids}")

        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Doom", year_hint=1993)
    assert best is not None
    assert best["id"] == 100


def test_steam_search_skips_non_game_types(tmp_path, monkeypatch):
    import json

    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return json.loads(json.dumps(self._payload))

        if "storesearch" in url:
            return Resp(
                {
                    "total": 2,
                    "items": [
                        {"id": 111, "name": "Example Game Soundtrack", "type": "app"},
                        {"id": 222, "name": "Example Game", "type": "app"},
                    ],
                }
            )
        if "appdetails" in url:
            appids = str((params or {}).get("appids"))
            if appids == "111":
                return Resp(
                    {
                        "111": {
                            "success": True,
                            "data": {
                                "type": "music",
                                "name": "Example Game Soundtrack",
                                "release_date": {"date": "1 Jan, 2000"},
                            },
                        }
                    }
                )
            if appids == "222":
                return Resp(
                    {
                        "222": {
                            "success": True,
                            "data": {
                                "type": "game",
                                "name": "Example Game",
                                "release_date": {"date": "1 Jan, 2000"},
                            },
                        }
                    }
                )
            raise AssertionError(f"unexpected appid {appids}")

        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Example Game", year_hint=2000)
    assert best is not None
    assert best["id"] == 222


def test_steam_search_prefers_base_or_edition_over_sequel_when_query_has_no_number(
    tmp_path, monkeypatch
):
    import json

    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(url, params=None, timeout=None):
        class Resp:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return json.loads(json.dumps(self._payload))

        if "storesearch" in url:
            return Resp(
                {
                    "total": 4,
                    "items": [
                        {"id": 400, "name": "Borderlands 4", "type": "app"},
                        {"id": 300, "name": "Borderlands 3", "type": "app"},
                        {"id": 200, "name": "Borderlands 2", "type": "app"},
                        {"id": 100, "name": "Borderlands Game of the Year Enhanced", "type": "app"},
                    ],
                }
            )

        if "appdetails" in url:
            appids = str((params or {}).get("appids"))
            names = {
                "100": ("Borderlands Game of the Year Enhanced", "20 Sep, 2019"),
                "200": ("Borderlands 2", "18 Sep, 2012"),
                "300": ("Borderlands 3", "13 Mar, 2020"),
                "400": ("Borderlands 4", "Coming soon"),
            }
            if appids in names:
                nm, date = names[appids]
                return Resp(
                    {
                        appids: {
                            "success": True,
                            "data": {"type": "game", "name": nm, "release_date": {"date": date}},
                        }
                    }
                )
            raise AssertionError(f"unexpected appid {appids}")

        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Borderlands")
    assert best is not None
    assert best["id"] == 100
