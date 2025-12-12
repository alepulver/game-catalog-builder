from pathlib import Path

from modules.igdb_client import IGDBClient
from modules.utilities import (
    ProjectPaths,
    read_csv,
    write_csv,
    ensure_columns,
    is_row_processed,
    PUBLIC_DEFAULT_COLS,
    load_credentials,
)

# ----------------------------
# CONFIG
# ----------------------------
ROOT = Path(__file__).resolve().parent
credentials = load_credentials()
IGDB_CLIENT_ID = credentials.get("igdb", {}).get("client_id", "")
IGDB_CLIENT_SECRET = credentials.get("igdb", {}).get("client_secret", "")

PATHS = ProjectPaths.from_root(ROOT)
PATHS.ensure()

INPUT_CSV = PATHS.data_input / "Games_Personal.csv"
OUTPUT_CSV = PATHS.data_processed / "Games_IGDB.csv"
CACHE_PATH = PATHS.data_raw / "igdb_cache.json"

REQUIRED_COLS = ["IGDB_ID"]

# ----------------------------
# MAIN
# ----------------------------
def main():
    client = IGDBClient(
        client_id=IGDB_CLIENT_ID,
        client_secret=IGDB_CLIENT_SECRET,
        cache_path=CACHE_PATH,
        min_interval_s=0.8,
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

        print(f"[IGDB] Processing: {name}")

        data = client.search(name)
        if not data:
            print("  ↳ Not found in IGDB")
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            write_csv(df, OUTPUT_CSV)

    write_csv(df, OUTPUT_CSV)
    print("✔ IGDB completed:", OUTPUT_CSV)


if __name__ == "__main__":
    main()
