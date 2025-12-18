from __future__ import annotations


def test_steam_request_failure_does_not_negative_cache(tmp_path, monkeypatch):
    from game_catalog_builder.clients.steam_client import SteamClient

    calls = {"storesearch": 0}

    def fake_get(url, params=None, timeout=None):
        if "storesearch" in url:
            calls["storesearch"] += 1
            raise RuntimeError("network down")
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("requests.get", fake_get)

    client = SteamClient(cache_path=tmp_path / "steam_cache.json", min_interval_s=0.0)
    assert client.search_appid("Borderlands") is None
    assert client.search_appid("Borderlands") is None
    # If request failures were negative-cached, the second call would not retry.
    assert calls["storesearch"] == 6  # 2 calls * 3 retries

