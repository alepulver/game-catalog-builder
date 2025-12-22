from __future__ import annotations


def test_steam_search_ignores_sub_results_and_pins_appid(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(_self, url, params=None, timeout=None):
        def _appids_from_url(u: str) -> list[str]:
            if "appids=" not in u:
                return []
            raw = u.split("appids=", 1)[1].split("&", 1)[0]
            return [s.strip() for s in raw.split(",") if s.strip()]

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                if "storesearch" in url:
                    return {
                        "items": [
                            {
                                "id": 199943,
                                "name": "Fallout 4: Game of the Year Edition",
                                "type": "sub",
                            },
                            {"id": 377160, "name": "Fallout 4", "type": "app"},
                        ]
                    }
                if "appdetails" in url:
                    payload = {}
                    for appid in _appids_from_url(url):
                        if appid == "377160":
                            payload[appid] = {
                                "success": True,
                                "data": {"type": "game", "name": "Fallout 4"},
                            }
                        else:
                            payload[appid] = {"success": False}
                    return payload
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Fallout 4: Game of the Year Edition")
    assert best is not None
    assert best["id"] == 377160


def test_steam_search_returns_none_when_only_sub_results_exist(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(_self, url, params=None, timeout=None):
        def _packageid_from_url(u: str) -> str:
            if "packageids=" not in u:
                return ""
            return u.split("packageids=", 1)[1].split("&", 1)[0]

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                if "storesearch" in url:
                    return {
                        "items": [
                            {"id": 77828, "name": "LISA: Complete Edition", "type": "sub"},
                        ]
                    }
                if "packagedetails" in url:
                    # Package exists but doesn't expose any apps; treat as non-resolvable.
                    pid = _packageid_from_url(url)
                    return {
                        pid: {
                            "success": True,
                            "data": {"name": "LISA: Complete Edition", "apps": []},
                        }
                    }
                if "appdetails" in url:
                    # Even if it gets called, this id can't be resolved via appdetails.
                    return {"77828": {"success": False}}
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    assert client.search_appid("LISA: Complete Edition") is None


def test_steam_search_resolves_sub_via_packagedetails(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(_self, url, params=None, timeout=None):
        def _packageid_from_url(u: str) -> str:
            if "packageids=" not in u:
                return ""
            return u.split("packageids=", 1)[1].split("&", 1)[0]

        def _appids_from_url(u: str) -> list[str]:
            if "appids=" not in u:
                return []
            raw = u.split("appids=", 1)[1].split("&", 1)[0]
            return [s.strip() for s in raw.split(",") if s.strip()]

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                if "storesearch" in url:
                    return {
                        "items": [
                            {
                                "id": 199943,
                                "name": "Fallout 4: Game of the Year Edition",
                                "type": "sub",
                            }
                        ]
                    }
                if "packagedetails" in url:
                    pid = _packageid_from_url(url)
                    return {
                        pid: {
                            "success": True,
                            "data": {"apps": [{"id": 377160, "name": "Fallout 4"}]},
                        }
                    }
                if "appdetails" in url:
                    payload = {}
                    for appid in _appids_from_url(url):
                        payload[appid] = {
                            "success": True,
                            "data": {"type": "game", "name": "Fallout 4"},
                        }
                    return payload
                raise AssertionError(f"unexpected url {url}")

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Fallout 4: Game of the Year Edition")
    assert best is not None
    assert best["id"] == 377160
