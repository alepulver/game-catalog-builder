from __future__ import annotations


def test_steam_rejects_dlc_type_when_appdetails_type_is_not_game(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    def fake_get(url, params=None, timeout=None):
        if "storesearch" in url:
            class Resp:
                status_code = 200
                headers: dict[str, str] = {}

                def raise_for_status(self):
                    return None

                def json(self):
                    return {"items": [{"id": 2112230, "name": "Car Mechanic Simulator 2021: Aston Martin"}]}

            return Resp()

        if "appdetails" in url:
            class Resp:
                status_code = 200
                headers: dict[str, str] = {}

                def raise_for_status(self):
                    return None

                def json(self):
                    # appdetails reveals it's DLC, even though storesearch name is clean.
                    return {
                        "2112230": {
                            "success": True,
                            "data": {"name": "Car Mechanic Simulator 2021 - Aston Martin DLC", "type": "dlc"},
                        }
                    }

            return Resp()

        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    assert client.search_appid("Car Mechanic Simulator 2021") is None

