from pathlib import Path

from modules.hltb_client import HLTBClient
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
OUTPUT_CSV = PATHS.data_processed / "Games_HLTB.csv"
CACHE_PATH = PATHS.data_raw / "hltb_cache.json"

REQUIRED_COLS = ["HLTB_Main"]

# ----------------------------
# MAIN
# ----------------------------
def main():
    client = HLTBClient(cache_path=CACHE_PATH)

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

        print(f"[HLTB] Processing: {name}")

        data = client.search(name)
        if not data:
            print("  ↳ Not found")
            continue

        for k, v in data.items():
            df.at[idx, k] = v

        processed += 1
        if processed % 10 == 0:
            write_csv(df, OUTPUT_CSV)

    write_csv(df, OUTPUT_CSV)
    print("✔ HLTB completed:", OUTPUT_CSV)


if __name__ == "__main__":
    main()
