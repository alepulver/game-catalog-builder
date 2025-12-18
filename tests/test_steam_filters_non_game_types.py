from __future__ import annotations


def test_steam_search_prefers_game_over_soundtrack_via_details(monkeypatch, tmp_path):
    from game_catalog_builder.clients.steam_client import SteamClient

    # storesearch returns a soundtrack-ish item and the real game.
    def fake_get(url, params=None, timeout=None):
        assert "storesearch" in url or "appdetails" in url

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                if "storesearch" in url:
                    return {
                        "items": [
                            {"id": 1, "name": "Half-Life 2: Episode Two Soundtrack", "type": "soundtrack"},
                            {"id": 2, "name": "Half-Life 2: Episode Two", "type": "game"},
                        ]
                    }
                # appdetails: soundtrack is not type=game
                appid = str(params.get("appids"))
                if appid == "1":
                    return {"1": {"success": True, "data": {"type": "music", "name": "Half-Life 2: Episode Two Soundtrack"}}}
                if appid == "2":
                    return {"2": {"success": True, "data": {"type": "game", "name": "Half-Life 2: Episode Two"}}}
                return {appid: {"success": False}}

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    best = client.search_appid("Half-Life 2: Episode Two")
    assert best is not None
    assert best["id"] == 2

