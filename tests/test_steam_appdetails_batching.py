from __future__ import annotations


def test_steam_get_app_details_many_batches_into_single_request(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    calls = {"appdetails": 0}

    def fake_get(url, params=None, timeout=None):
        assert "appdetails" in url
        calls["appdetails"] += 1

        appids = str((params or {}).get("appids") or "")
        ids = [s.strip() for s in appids.split(",") if s.strip()]
        payload = {}
        for s in ids:
            payload[s] = {
                "success": True,
                "data": {"steam_appid": int(s), "name": f"Game {s}", "type": "game", "is_free": True},
            }

        class Resp:
            status_code = 200
            headers: dict[str, str] = {}

            def raise_for_status(self):
                return None

            def json(self):
                return payload

        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    out = client.get_app_details_many([1, 2, 3])
    assert out[1]["name"] == "Game 1"
    assert out[2]["name"] == "Game 2"
    assert out[3]["name"] == "Game 3"
    assert calls["appdetails"] == 1
