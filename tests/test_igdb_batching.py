from __future__ import annotations


def test_igdb_get_by_ids_batches_single_request(tmp_path, monkeypatch):
    from game_catalog_builder.clients.igdb_client import IGDBClient

    calls: list[str] = []

    def fake_post(_self, url, headers=None, data=None, timeout=None):
        calls.append(str(data or ""))
        assert url.endswith("/v4/games")
        q = str(data).replace(" ", "").replace("\n", "")
        assert "whereid=(1,2,3);" in q

        class Resp:
            status_code = 200
            headers = {}

            def raise_for_status(self):
                return None

            def json(self):
                return [
                    {"id": 1, "name": "One", "first_release_date": 0},
                    {"id": 2, "name": "Two", "first_release_date": 0},
                    {"id": 3, "name": "Three", "first_release_date": 0},
                ]

        return Resp()

    monkeypatch.setattr("requests.sessions.Session.post", fake_post)

    client = IGDBClient(
        client_id="x",
        client_secret="y",
        cache_path=tmp_path / "igdb_cache.json",
        language="en",
        min_interval_s=0.0,
    )
    # Avoid OAuth network calls in test.
    client._token = "token"

    out = client.get_by_ids([1, "2", 3])
    assert out["1"]["igdb.id"] == "1"
    assert out["2"]["igdb.name"] == "Two"
    assert out["3"]["igdb.name"] == "Three"
    assert len(calls) == 1
