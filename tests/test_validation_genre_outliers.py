import pandas as pd


def test_validation_genre_outlier_tag_when_majority_exists() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "RAWG_ID": "1",
                "RAWG_Name": "Example",
                "RAWG_Genres": "Action, Shooter",
                "IGDB_ID": "2",
                "IGDB_Name": "Example",
                "IGDB_Genres": "Action",
                "Steam_AppID": "3",
                "Steam_Name": "Example",
                "Steam_Tags": "Puzzle",
            }
        ]
    )
    report = generate_validation_report(df)
    tags = report.loc[0, "ValidationTags"]
    assert "genre_outlier:steam" in tags


def test_validation_genre_no_consensus_when_disjoint() -> None:
    from game_catalog_builder.utils.validation import generate_validation_report

    df = pd.DataFrame(
        [
            {
                "Name": "Example",
                "RAWG_ID": "1",
                "RAWG_Name": "Example",
                "RAWG_Genres": "Action",
                "IGDB_ID": "2",
                "IGDB_Name": "Example",
                "IGDB_Genres": "Puzzle",
            }
        ]
    )
    report = generate_validation_report(df)
    tags = report.loc[0, "ValidationTags"]
    assert "genre_no_consensus" in tags

