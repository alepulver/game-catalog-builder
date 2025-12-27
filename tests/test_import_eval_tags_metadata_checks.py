from __future__ import annotations

import pandas as pd


class _RAWGStub:
    def __init__(self, *, released: str, platforms: list[str], genres: list[str]):
        self._released = released
        self._platforms = platforms
        self._genres = genres

    def get_by_id(self, _id: str):
        return {
            "released": self._released,
            "platforms": [{"platform": {"name": p}} for p in self._platforms],
            "genres": [{"name": g} for g in self._genres],
        }


class _IGDBStub:
    def __init__(
        self,
        *,
        year: str,
        platforms: str,
        genres: str,
        version_parent: str = "",
        ports: str = "",
    ):
        self._year = year
        self._platforms = platforms
        self._genres = genres
        self._version_parent = version_parent
        self._ports = ports

    def get_by_id(self, _id: str):
        return {
            "IGDB_Year": self._year,
            "IGDB_Platforms": self._platforms,
            "IGDB_Genres": self._genres,
            "IGDB_VersionParent": self._version_parent,
            "IGDB_Ports": self._ports,
        }


class _HLTBStub:
    def __init__(self, *, year: str, platforms: str):
        self._year = year
        self._platforms = platforms

    def get_by_id(self, _id: str):
        return {"HLTB_ReleaseYear": self._year, "HLTB_Platforms": self._platforms}


class _SteamStub:
    def __init__(self, *, year: int):
        self._year = year

    def get_app_details(self, _appid: int):
        return {"release_date": {"date": f"1 Jan, {self._year}"}}


def test_fill_eval_tags_flags_year_outlier_when_rawg_igdb_agree() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "HLTB_ID": "3",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
                "HLTB_MatchedName": "DOOM",
                "HLTB_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb", "hltb"},
        clients={
            "rawg": _RAWGStub(released="1993-12-10", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(year="1993", platforms="PC", genres="Shooter"),
            "hltb": _HLTBStub(year="2016", platforms="PC"),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "year_outlier:hltb" in tags
    assert "likely_wrong:hltb" in tags
    assert out.iloc[0]["MatchConfidence"] == "LOW"


def test_fill_eval_tags_flags_platform_outlier() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "HLTB_ID": "3",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
                "HLTB_MatchedName": "DOOM",
                "HLTB_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb", "hltb"},
        clients={
            "rawg": _RAWGStub(released="1993-12-10", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(year="1993", platforms="PC", genres="Shooter"),
            "hltb": _HLTBStub(year="1993", platforms="PlayStation 2"),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "platform_outlier:hltb" in tags
    assert out.iloc[0]["MatchConfidence"] == "LOW"


def test_fill_eval_tags_flags_genre_disagree_rawg_igdb() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "RAWG_MatchedName": "DOOM",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb"},
        clients={
            "rawg": _RAWGStub(released="1993-12-10", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(year="1993", platforms="PC", genres="Role-playing (RPG)"),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "genre_disagree" in tags
    assert out.iloc[0]["MatchConfidence"] == "MEDIUM"


def test_fill_eval_tags_adds_provider_outlier_when_two_agree() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Operation Flashpoint: Cold War Crisis",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "HLTB_ID": "3",
                "RAWG_MatchedName": "Operation Flashpoint: Cold War Crisis",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Operation Flashpoint: Cold War Crisis",
                "IGDB_MatchScore": "100",
                "HLTB_MatchedName": "Operation Flashpoint: Dragon Rising",
                "HLTB_MatchScore": "71",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb", "hltb"},
        clients={
            "rawg": _RAWGStub(released="2001-06-22", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(year="2001", platforms="PC", genres="Shooter"),
            "hltb": _HLTBStub(year="2009", platforms="PC"),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "provider_consensus:igdb+rawg" in tags
    assert "provider_outlier:hltb" in tags


def test_fill_eval_tags_adds_provider_no_consensus_when_all_disagree() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "Steam_AppID": "10",
                "HLTB_ID": "3",
                "RAWG_MatchedName": "Doom 3",
                "RAWG_MatchScore": "80",
                "IGDB_MatchedName": "Doom (2016)",
                "IGDB_MatchScore": "80",
                "Steam_MatchedName": "Doom Eternal",
                "Steam_MatchScore": "80",
                "HLTB_MatchedName": "Doom 64",
                "HLTB_MatchScore": "80",
            }
        ]
    )

    out = fill_eval_tags(df, sources={"rawg", "igdb", "steam", "hltb"}, clients={})
    tags = out.iloc[0]["ReviewTags"]
    assert "provider_no_consensus" in tags


def test_fill_eval_tags_downgrades_missing_steam_when_platforms_non_pc() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Console Only",
                "Platform": "PC",
                "Steam_AppID": "",
                "RAWG_ID": "1",
                "RAWG_MatchedName": "Console Only",
                "RAWG_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "steam"},
        clients={
            "rawg": _RAWGStub(
                released="2000-01-01", platforms=["PlayStation 2"], genres=["Action"]
            ),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    tagset = {t.strip() for t in str(tags).split(",") if t.strip()}
    assert "missing_steam_nonpc" in tagset
    assert "missing_steam" not in tagset
    assert out.iloc[0]["MatchConfidence"] == "HIGH"


def test_fill_eval_tags_tags_edition_or_port_suspected_for_steam_year_outlier() -> None:
    from game_catalog_builder.analysis.import_diagnostics import fill_eval_tags

    df = pd.DataFrame(
        [
            {
                "RowId": "rid:1",
                "Name": "Doom",
                "RAWG_ID": "1",
                "IGDB_ID": "2",
                "Steam_AppID": "10",
                "RAWG_MatchedName": "Doom",
                "RAWG_MatchScore": "100",
                "IGDB_MatchedName": "Doom",
                "IGDB_MatchScore": "100",
                "Steam_MatchedName": "DOOM",
                "Steam_MatchScore": "100",
            }
        ]
    )

    out = fill_eval_tags(
        df,
        sources={"rawg", "igdb", "steam"},
        clients={
            "rawg": _RAWGStub(released="1993-12-10", platforms=["PC"], genres=["Shooter"]),
            "igdb": _IGDBStub(
                year="1993",
                platforms="PC",
                genres="Shooter",
                version_parent="Doom (1993)",
            ),
            "steam": _SteamStub(year=2016),
        },
    )
    tags = out.iloc[0]["ReviewTags"]
    assert "year_outlier:steam" in tags
    assert "edition_or_port_suspected" in tags
