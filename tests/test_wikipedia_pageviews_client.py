from __future__ import annotations


def test_pageviews_client_caches_and_sums(tmp_path, monkeypatch):
    from game_catalog_builder.clients.wikipedia_pageviews_client import WikipediaPageviewsClient

    calls = {"get": 0}

    class FakeResp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "items": [
                    {"timestamp": "2025010100", "views": 10},
                    {"timestamp": "2025010200", "views": 20},
                ]
            }

    def fake_get(_self, url, timeout, headers):
        calls["get"] += 1
        return FakeResp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = WikipediaPageviewsClient(cache_path=tmp_path / "pv.json", min_interval_s=0.0)
    total1 = client.get_pageviews_summary_enwiki("Doom").days_365
    total2 = client.get_pageviews_summary_enwiki("Doom").days_365

    assert total1 == 30
    assert total2 == 30
    assert calls["get"] == 1


def test_pageviews_first_days_since_release_returns_sum(tmp_path, monkeypatch):
    from datetime import date, timedelta

    from game_catalog_builder.clients.wikipedia_pageviews_client import WikipediaPageviewsClient

    class FakeResp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "items": [
                    {"timestamp": "2025010100", "views": 3},
                    {"timestamp": "2025010200", "views": 4},
                ]
            }

    def fake_get(_self, url, timeout, headers):
        return FakeResp()

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    client = WikipediaPageviewsClient(cache_path=tmp_path / "pv.json", min_interval_s=0.0)
    got = client.get_pageviews_first_days_since_release_enwiki(
        enwiki_title="Doom",
        release_date="2025-01-01",
        days=90,
        earliest_supported=date.today() - timedelta(days=3650),
    )
    assert got == 7


def test_pageviews_client_disables_fetch_on_network_failure(tmp_path, monkeypatch):
    from datetime import date, timedelta

    from game_catalog_builder.clients.wikipedia_pageviews_client import WikipediaPageviewsClient
    import requests

    calls = {"get": 0}

    def fake_get(_self, url, timeout, headers):
        calls["get"] += 1
        raise requests.exceptions.ConnectionError("network down")

    monkeypatch.setattr("requests.sessions.Session.get", fake_get)

    # Seed a cached 365-day window for Doom, but with a non-current end date. This simulates
    # a warm cache when offline (cache keys include the request's start/end stamps).
    end = date.today() - timedelta(days=2)
    start = end - timedelta(days=365 - 1)
    start_s = start.strftime("%Y%m%d") + "00"
    end_s = end.strftime("%Y%m%d") + "00"
    (tmp_path / "pv.json").write_text(
        (
            "{\n"
            '  "by_query": {\n'
            f'    "en.wikipedia.org|all-access|user|Doom|daily|{start_s}|{end_s}": {{\n'
            '      "items": [\n'
            f'        {{"timestamp": "{start_s}", "views": 10}},\n'
            f'        {{"timestamp": "{(start + timedelta(days=1)).strftime("%Y%m%d")}00", "views": 20}}\n'
            "      ]\n"
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    client = WikipediaPageviewsClient(cache_path=tmp_path / "pv.json", min_interval_s=0.0)
    got1 = client.get_pageviews_summary_enwiki("Doom").days_365
    got2 = client.get_pageviews_summary_enwiki("Doom").days_365

    # The first call attempts network, fails, and then falls back to the cached window.
    assert got1 == 30
    # Second call should reuse the cached window without hitting the network.
    assert got2 == 30
    # Only the first call attempts network; subsequent calls are cache-only due to disable flag.
    assert calls["get"] == 1
