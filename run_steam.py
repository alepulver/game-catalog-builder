from pathlib import Path

from modules.steam_client import SteamClient
from modules.utilities import (
    ProjectPaths,
    read_csv,
    write_csv,
    ensure_columns,
    is_row_processed,
    PUBLIC_DEFAULT_COLS,
)

# ----------------------------
# CONFIG
# ----------------------------
ROOT = Path(__file__).resolve().parent
PATHS = ProjectPaths.from_root(ROOT)
PATHS.ensure()

INPUT_CSV = PATHS.data_input / "Games_Personal.csv"
OUTPUT_CSV = PATHS.data_processed / "Games_Steam.csv"
CACHE_PATH = PATHS.data_raw / "steam_cache.json"

REQUIRED_COLS = ["Steam_AppID"]

# ----------------------------
# MAIN
# ----------------------------
def main():
    client = SteamClient(
        cache_path=CACHE_PATH,
        min_interval_s=1.0,
    )

    if OUTPUT_CSV.exists():
        df = read_csv(OUTPUT_CSV)
    else:
        df = read_csv(INPUT_CSV)

    df = ensure_columns(df, PUBLIC_DEFAULT_COLS)

    processed = 0

    for idx, row in df.iterrows():
        name = row.get("Name", "").strip()
        if not name:
            continue

        if is_row_processed(df, idx, REQUIRED_COLS):
            continue

        print(f"[STEAM] Processing: {name}")

        search = client.search_appid(name)
        if not search:
            print("  ↳ Not on Steam")
            continue

        appid = search.get("id")
        details = client.get_app_details(appid)
        if not details:
            continue

        fields = client.extract_fields(appid, details)
        for k, v in fields.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            write_csv(df, OUTPUT_CSV)

    write_csv(df, OUTPUT_CSV)
    print("✔ Steam completed:", OUTPUT_CSV)


if __name__ == "__main__":
    main()
