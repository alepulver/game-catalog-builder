from pathlib import Path

from modules.utilities import ProjectPaths
from modules.merger import merge_all

ROOT = Path(__file__).resolve().parent
PATHS = ProjectPaths.from_root(ROOT)
PATHS.ensure()

merge_all(
    personal_csv=PATHS.data_input / "Games_Personal.csv",
    rawg_csv=PATHS.data_processed / "Games_RAWG.csv",
    hltb_csv=PATHS.data_processed / "Games_HLTB.csv",
    steam_csv=PATHS.data_processed / "Games_Steam.csv",
    steamspy_csv=PATHS.data_processed / "Games_SteamSpy.csv",
    output_csv=PATHS.data_processed / "Games_Final.csv",
    igdb_csv=PATHS.data_processed / "Games_IGDB.csv",
)

print("✔ Games_Final.csv generated successfully")
