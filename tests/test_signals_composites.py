from __future__ import annotations


def test_reach_and_now_composites_are_derived(tmp_path):
    import pandas as pd

    from game_catalog_builder.utils.signals import apply_phase1_signals

    tiers = tmp_path / "tiers.yaml"
    tiers.write_text("publishers: {}\ndevelopers: {}\n", encoding="utf-8")

    df = pd.DataFrame(
        [
            {
                "RowId": "1",
                "SteamSpy_Owners": "1,000,000 .. 2,000,000",
                "Steam_ReviewCount": "50000",
                "RAWG_RatingsCount": "2000",
                "RAWG_Added": "100000",
                "IGDB_RatingCount": "3000",
                "IGDB_AggregatedRatingCount": "120",
                "Wikidata_Pageviews365d": "5000000",
                "Wikidata_Pageviews30d": "200000",
                "Wikidata_Pageviews90d": "500000",
                "SteamSpy_Players2Weeks": "10000",
                "SteamSpy_CCU": "250",
            },
            {
                "RowId": "2",
                "SteamSpy_Owners": "",
                "Steam_ReviewCount": "50000",
                "RAWG_RatingsCount": "2000",
                "RAWG_Added": "100000",
                "IGDB_RatingCount": "3000",
                "IGDB_AggregatedRatingCount": "120",
                "Wikidata_Pageviews365d": "",
                "Wikidata_Pageviews30d": "",
                "Wikidata_Pageviews90d": "",
                "SteamSpy_Players2Weeks": "",
                "SteamSpy_CCU": "",
            },
        ]
    )

    out = apply_phase1_signals(df, production_tiers_path=tiers)

    assert out.loc[0, "Reach_Composite"] != ""
    assert out.loc[0, "Now_Composite"] != ""
    assert out.loc[1, "Reach_Composite"] != ""
    assert out.loc[1, "Now_Composite"] == ""

    # Owners present should not reduce reach vs similar row without owners.
    assert int(out.loc[0, "Reach_Composite"]) >= int(out.loc[1, "Reach_Composite"])

    # Basic bounds: composites are 0..100 ints when present.
    assert 0 <= int(out.loc[0, "Reach_Composite"]) <= 100
    assert 0 <= int(out.loc[0, "Now_Composite"]) <= 100
