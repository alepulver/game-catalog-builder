from __future__ import annotations

import json
from pathlib import Path

from game_catalog_builder.clients.wikidata_client import WikidataClient
from game_catalog_builder.utils.utilities import IDENTITY_NOT_FOUND


def test_wikidata_label_fetch_falls_back_to_enwiki_title(tmp_path: Path) -> None:
    cache_path = tmp_path / "wikidata_cache.json"
    cache_path.write_text(json.dumps({}), encoding="utf-8")
    client = WikidataClient(cache_path=cache_path, min_interval_s=0.0)
    client._cache_io.min_interval_s = 0.0

    def fake_get_entities(self, qids, *, props, purpose="entities", sitefilter=None):  # type: ignore[no-untyped-def]
        assert purpose == "labels"
        assert "sitelinks" in props
        assert sitefilter == "enwiki"
        return {
            "Q10680": {
                "id": "Q10680",
                "labels": {},
                "sitelinks": {"enwiki": {"title": "PlayStation 2"}},
            }
        }

    client._get_entities = fake_get_entities.__get__(client, WikidataClient)  # type: ignore[attr-defined]

    client._ensure_labels({"Q10680"})
    assert client._labels["Q10680"] == "PlayStation 2"

    # Second call should be served from cache (no HTTP call attempted).
    def should_not_be_called(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("_get_entities should not be called when label is cached")

    client._get_entities = should_not_be_called  # type: ignore[assignment]
    client._ensure_labels({"Q10680"})


def test_wikidata_label_fetch_negative_caches_missing_label(tmp_path: Path) -> None:
    cache_path = tmp_path / "wikidata_cache.json"
    cache_path.write_text(json.dumps({}), encoding="utf-8")
    client = WikidataClient(cache_path=cache_path, min_interval_s=0.0)
    client._cache_io.min_interval_s = 0.0

    def fake_get_entities(self, qids, *, props, purpose="entities", sitefilter=None):  # type: ignore[no-untyped-def]
        assert purpose == "labels"
        return {"Q999": {"id": "Q999", "labels": {}, "sitelinks": {}}}

    client._get_entities = fake_get_entities.__get__(client, WikidataClient)  # type: ignore[attr-defined]

    client._ensure_labels({"Q999"})
    assert client._labels["Q999"] == IDENTITY_NOT_FOUND

    # Reloading from cache should preserve the negative entry.
    raw = json.loads(cache_path.read_text(encoding="utf-8"))
    assert raw["labels"]["Q999"] == IDENTITY_NOT_FOUND
