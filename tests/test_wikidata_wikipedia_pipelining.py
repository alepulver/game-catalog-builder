from __future__ import annotations

import threading
from pathlib import Path

import pandas as pd


def test_wikidata_wikipedia_fetch_is_pipelined(tmp_path: Path, monkeypatch) -> None:
    """
    Regression test: Wikidata -> Wikipedia signals should be pipelined (background consumer),
    so slow Wikipedia calls don't block continued Wikidata batching.

    This test is constructed to deadlock if Wikipedia is fetched inline:
      - Wikipedia pageviews blocks until the 2nd Wikidata batch completes.
      - The 2nd Wikidata batch only runs if the main thread isn't blocked on Wikipedia.
    """
    from game_catalog_builder.pipelines.enrich_pipeline import process_wikidata

    started_pageviews = threading.Event()
    allow_pageviews = threading.Event()

    class _FakeWikidataClient:
        def __init__(self, *_args, **_kwargs):
            self.calls = 0

        def get_by_ids(self, qids: list[str]):
            self.calls += 1
            # Unblock the first pageviews call only after the 2nd Wikidata batch is reached.
            if self.calls == 2:
                allow_pageviews.set()
            out: dict[str, dict[str, str]] = {}
            for qid in qids:
                out[qid] = {
                    "Wikidata_QID": qid,
                    "Wikidata_Label": f"Label {qid}",
                    "Wikidata_EnwikiTitle": f"Title {qid}",
                    "Wikidata_ReleaseDate": "2016-05-13",
                }
            return out

        def search(self, *_args, **_kwargs):
            raise AssertionError("search() should not be called in this test")

        def format_cache_stats(self) -> str:
            return "ok"

    class _FakePageviewsClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_pageviews_summary_enwiki(self, _title: str):
            from game_catalog_builder.clients.wikipedia_pageviews_client import PageviewsSummary

            started_pageviews.set()
            allow_pageviews.wait(timeout=5)
            return PageviewsSummary(1, 2, 3)

        def get_pageviews_launch_summary_enwiki(self, *_args, **_kwargs):
            from game_catalog_builder.clients.wikipedia_pageviews_client import PageviewsSummary

            return PageviewsSummary(4, 5, None)

        def format_cache_stats(self) -> str:
            return "ok"

    class _FakeSummaryClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_summary(self, title: str):
            return {
                "extract": f"Summary for {title}",
                "thumbnail": {"source": "https://example.invalid/thumb.jpg"},
                "content_urls": {"desktop": {"page": f"https://example.invalid/wiki/{title}"}},
            }

        def format_cache_stats(self) -> str:
            return "ok"

    monkeypatch.setattr(
        "game_catalog_builder.pipelines.enrich_pipeline.WikidataClient", _FakeWikidataClient
    )
    monkeypatch.setattr(
        "game_catalog_builder.pipelines.enrich_pipeline.WikipediaPageviewsClient",
        _FakePageviewsClient,
    )
    monkeypatch.setattr(
        "game_catalog_builder.pipelines.enrich_pipeline.WikipediaSummaryClient",
        _FakeSummaryClient,
    )
    monkeypatch.setattr(
        "game_catalog_builder.pipelines.enrich_pipeline.WIKIDATA",
        type(
            "_WikidataCfg",
            (),
            {
                "min_interval_s": 0.0,
                "search_limit": 20,
                "labels_batch_size": 50,
                "get_by_ids_batch_size": 1,
            },
        )(),
    )

    input_csv = tmp_path / "catalog.csv"
    output_csv = tmp_path / "Provider_Wikidata.csv"
    cache_path = tmp_path / "wikidata_cache.json"

    df = pd.DataFrame(
        [
            {"RowId": "rid:1", "Name": "G1", "Wikidata_QID": "Q1"},
            {"RowId": "rid:2", "Name": "G2", "Wikidata_QID": "Q2"},
        ]
    )
    df.to_csv(input_csv, index=False)

    error: Exception | None = None

    def _run() -> None:
        nonlocal error
        try:
            process_wikidata(
                input_csv=input_csv,
                output_csv=output_csv,
                cache_path=cache_path,
                required_cols=["Wikidata_Label"],
                identity_overrides=None,
            )
        except Exception as e:  # pragma: no cover
            error = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive(), "process_wikidata() hung; Wikipedia may not be pipelined"
    assert error is None
    assert started_pageviews.is_set(), "Expected Wikipedia pageviews fetch to start"

    out = pd.read_csv(output_csv, dtype=str, keep_default_na=False)
    assert out.loc[0, "Wikidata_Pageviews30d"] == "1"
    assert out.loc[0, "Wikidata_PageviewsFirst30d"] == "4"
    assert out.loc[0, "Wikidata_WikipediaSummary"].startswith("Summary for Title Q1")
    assert out.loc[1, "Wikidata_Pageviews30d"] == "1"
    assert out.loc[1, "Wikidata_WikipediaSummary"].startswith("Summary for Title Q2")
