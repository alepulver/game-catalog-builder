from pathlib import Path
import pandas as pd

from modules.rawg_client import RAWGClient
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
RAWG_API_KEY = credentials.get("rawg", {}).get("api_key", "")
PATHS = ProjectPaths.from_root(ROOT)
PATHS.ensure()

INPUT_CSV = PATHS.data_input / "Games_Personal.csv"
OUTPUT_CSV = PATHS.data_processed / "Games_RAWG.csv"
CACHE_PATH = PATHS.data_raw / "rawg_cache.json"

REQUIRED_COLS = [
    "RAWG_ID",
    "RAWG_Year",
    "RAWG_Genre",
]

# ----------------------------
# MAIN
# ----------------------------
def main():
    client = RAWGClient(
        api_key=RAWG_API_KEY,
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

        print(f"[RAWG] Processing: {name}")

        result = client.search(name)
        if not result:
            print("  ↳ Not found")
            continue

        fields = client.extract_fields(result)
        for k, v in fields.items():
            df.at[idx, k] = v

        processed += 1

        # Incremental save every 10
        if processed % 10 == 0:
            write_csv(df, OUTPUT_CSV)

    write_csv(df, OUTPUT_CSV)
    print("✔ RAWG completed:", OUTPUT_CSV)


if __name__ == "__main__":
    main()
