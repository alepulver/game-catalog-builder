from pathlib import Path

from modules.steamspy_client import SteamSpyClient
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

# IMPORTANT:
# SteamSpy depends on AppID → we use the Steam CSV
INPUT_CSV = PATHS.data_processed / "Games_Steam.csv"
OUTPUT_CSV = PATHS.data_processed / "Games_SteamSpy.csv"
CACHE_PATH = PATHS.data_raw / "steamspy_cache.json"

REQUIRED_COLS = ["SteamSpy_Owners"]

# ----------------------------
# MAIN
# ----------------------------
def main():
    client = SteamSpyClient(
        cache_path=CACHE_PATH,
        min_interval_s=1.0,
    )

    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            "Games_Steam.csv not found. Run run_steam.py first"
        )

    if OUTPUT_CSV.exists():
        df = read_csv(OUTPUT_CSV)
    else:
        df = read_csv(INPUT_CSV)

    df = ensure_columns(df, PUBLIC_DEFAULT_COLS)

    processed = 0

    for idx, row in df.iterrows():
        appid = row.get("Steam_AppID", "").strip()
        name = row.get("Name", "").strip()

        if not appid:
            continue

        if is_row_processed(df, idx, REQUIRED_COLS):
            continue

        print(f"[STEAMSPY] {name} (AppID {appid})")

        data = client.fetch(int(appid))
        if not data:
            print("  ↳ No data in SteamSpy")
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            write_csv(df, OUTPUT_CSV)

    write_csv(df, OUTPUT_CSV)
    print("✔ SteamSpy completed:", OUTPUT_CSV)


if __name__ == "__main__":
    main()
