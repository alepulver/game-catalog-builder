from __future__ import annotations

import json


def _appids_from_url(u: str) -> str:
    if "appids=" not in u:
        return ""
    return u.split("appids=", 1)[1].split("&", 1)[0]


class Resp:
    status_code = 200
    headers: dict[str, str] = {}

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        # Provide a deep copy so tests don't accidentally rely on shared dict mutation.
        return json.loads(json.dumps(self._payload))


def test_steam_search_prefers_release_year_via_appdetails(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(_self, url, params=None, timeout=None):
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
            appids = _appids_from_url(url)
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

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Doom", year_hint=1993)
    assert best is not None
    assert best["id"] == 100


def test_steam_search_skips_non_game_types(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(_self, url, params=None, timeout=None):
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
            appids = _appids_from_url(url)
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

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Example Game", year_hint=2000)
    assert best is not None
    assert best["id"] == 222


def test_steam_search_prefers_base_or_edition_over_sequel_when_query_has_no_number(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(_self, url, params=None, timeout=None):
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
            appids = _appids_from_url(url)
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

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Borderlands")
    assert best is not None
    assert best["id"] == 100


def test_steam_search_prefers_game_over_soundtrack_via_details(monkeypatch, tmp_path):
    from game_catalog_builder.clients.steam_client import SteamClient

    # storesearch returns a soundtrack-ish item and the real game.
    def fake_get(_self, url, params=None, timeout=None):
        assert "storesearch" in url or "appdetails" in url

        if "storesearch" in url:
            return Resp(
                {
                    "items": [
                        {"id": 1, "name": "Half-Life 2: Episode Two Soundtrack", "type": "app"},
                        {"id": 2, "name": "Half-Life 2: Episode Two", "type": "app"},
                    ]
                }
            )

        appid = _appids_from_url(url)
        if appid == "1":
            return Resp(
                {
                    "1": {
                        "success": True,
                        "data": {
                            "type": "music",
                            "name": "Half-Life 2: Episode Two Soundtrack",
                        },
                    }
                }
            )
        if appid == "2":
            return Resp(
                {
                    "2": {
                        "success": True,
                        "data": {"type": "game", "name": "Half-Life 2: Episode Two"},
                    }
                }
            )
        return Resp({appid: {"success": False}})

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Half-Life 2: Episode Two")
    assert best is not None
    assert best["id"] == 2


def test_steam_rejects_dlc_type_when_appdetails_type_is_not_game(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(_self, url, params=None, timeout=None):
        if "storesearch" in url:
            return Resp({"items": [{"id": 2112230, "name": "Car Mechanic Simulator 2021: Aston Martin"}]})

        if "appdetails" in url:
            appids = _appids_from_url(url)
            return Resp(
                {
                    appids: {
                        "success": True,
                        "data": {
                            "name": "Car Mechanic Simulator 2021 - Aston Martin DLC",
                            "type": "dlc",
                        },
                    }
                }
            )

        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    assert client.search_appid("Car Mechanic Simulator 2021") is None
