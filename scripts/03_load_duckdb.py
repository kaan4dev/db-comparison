from __future__ import annotations

import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = ROOT / "data" / "data_10m"

DB_DIR = ROOT / "db"
DB_DIR.mkdir(exist_ok=True)

DUCKDB_PATH = DB_DIR / "duckdb.db"

DUCKDB_THREADS = 4

def main():
    if not PARQUET_DIR.exists():
        raise FileNotFoundError(f"Parquet dataset dir not found: {PARQUET_DIR}")

    parts = sorted(PARQUET_DIR.glob("part_*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No parquet parts found in: {PARQUET_DIR}")

    if DUCKDB_PATH.exists():
        DUCKDB_PATH.unlink()

    glob_path = (PARQUET_DIR / "*.parquet").as_posix()
    print(f"[duckdb] create db: {DUCKDB_PATH}")
    print(f"[duckdb] load start: source={glob_path}")

    con = duckdb.connect(DUCKDB_PATH.as_posix())
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")

    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{glob_path}');")
    t1 = time.perf_counter()

    n = con.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    print(f"[duckdb] load done: seconds={(t1 - t0):.3f} rows={n:,}")

    con.close()


if __name__ == "__main__":
    main()