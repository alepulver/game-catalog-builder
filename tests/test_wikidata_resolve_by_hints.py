from __future__ import annotations


def test_wikidata_resolve_by_hints_uses_cache(tmp_path, monkeypatch) -> None:
    from game_catalog_builder.clients.wikidata_client import (
        WIKIDATA_PROP_STEAM_APPID,
        WikidataClient,
    )

    client = WikidataClient(cache_path=tmp_path / "wikidata_cache.json", min_interval_s=0.0)

    calls = {"sparql": 0, "get_by_id": 0}

    def fake_sparql_select_qids(*, prop: str, value: str) -> list[str]:
        assert prop == WIKIDATA_PROP_STEAM_APPID
        assert value == "620"
        calls["sparql"] += 1
        return ["Q1", "Q2"]

    def fake_get_by_id(qid: str):
        calls["get_by_id"] += 1
        if qid == "Q1":
            return None
        if qid == "Q2":
            return {"Wikidata_QID": "Q2", "Wikidata_Label": "Doom"}
        raise AssertionError(f"unexpected qid {qid}")

    monkeypatch.setattr(client, "_sparql_select_qids", fake_sparql_select_qids)
    monkeypatch.setattr(client, "get_by_id", fake_get_by_id)

    got = client.resolve_by_hints(steam_appid="620")
    assert got is not None
    assert got["Wikidata_QID"] == "Q2"
    assert calls["sparql"] == 1
    assert calls["get_by_id"] == 2

    # Second call should hit the hint cache and avoid SPARQL.
    got2 = client.resolve_by_hints(steam_appid="620")
    assert got2 is not None
    assert got2["Wikidata_QID"] == "Q2"
    assert calls["sparql"] == 1


def test_wikidata_sparql_parses_qids(tmp_path, monkeypatch) -> None:
    from game_catalog_builder.clients.wikidata_client import WikidataClient

    client = WikidataClient(cache_path=tmp_path / "wikidata_cache.json", min_interval_s=0.0)

    class Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "results": {
                    "bindings": [{"item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q279446"}}]
                }
            }

    def fake_get(_url, params=None, timeout=None, headers=None):
        assert params and "query" in params
        return Resp()

    monkeypatch.setattr(client._session, "get", fake_get)

    got = client._sparql_select_qids(prop="P1733", value="620")
    assert got == ["Q279446"]
