from __future__ import annotations

import time
import sqlite3
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SQLITE_PATH = ROOT / "db" / "sqlite.db"
DUCKDB_PATH = ROOT / "db" / "duckdb.db"

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_PATH = LOG_DIR / "read_benchmark.log"

BATCH_ROWS = 50_000
DUCKDB_THREADS = 4

def log(line: str) -> None:
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def now_s() -> float:
    return time.perf_counter()

def ensure_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")
    conn.commit()

def sqlite_select_star_stream(conn: sqlite3.Connection) -> tuple[int, float]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM data;")

    total = 0
    t0 = now_s()
    while True:
        batch = cur.fetchmany(BATCH_ROWS)
        if not batch:
            break
        total += len(batch)
    t1 = now_s()

    return total, (t1 - t0)

def duckdb_select_star_stream(conn: duckdb.DuckDBPyConnection) -> tuple[int, float]:
    cur = conn.execute("SELECT * FROM data;")

    total = 0
    t0 = now_s()
    while True:
        batch = cur.fetchmany(BATCH_ROWS)
        if not batch:
            break
        total += len(batch)
    t1 = now_s()

    return total, (t1 - t0)

def main() -> None:
    if LOG_PATH.exists():
        LOG_PATH.unlink()

    log("meta | read_benchmark_start")
    log(f"meta | sqlite_db={SQLITE_PATH}")
    log(f"meta | duckdb_db={DUCKDB_PATH}")
    log(f"meta | batch_rows={BATCH_ROWS}")

    sqlite_conn = sqlite3.connect(SQLITE_PATH.as_posix())
    ensure_sqlite_pragmas(sqlite_conn)

    s_rows = sqlite_conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    log(f"verify | rowcount | sqlite={s_rows}")

    log("read | start | engine=sqlite | query=select_star")
    rows, seconds = sqlite_select_star_stream(sqlite_conn)
    log(f"sqlite | read_select_star | seconds={seconds:.4f} | rows={rows}")

    sqlite_conn.close()

    duck_conn = duckdb.connect(DUCKDB_PATH.as_posix())
    duck_conn.execute(f"PRAGMA threads={DUCKDB_THREADS};")

    d_rows = duck_conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    log(f"verify | rowcount | duckdb={d_rows}")

    log("read | start | engine=duckdb | query=select_star")
    rows, seconds = duckdb_select_star_stream(duck_conn)
    log(f"duckdb | read_select_star | seconds={seconds:.4f} | rows={rows}")

    duck_conn.close()

    log("meta | read_benchmark_done")
    log(f"meta | log_file={LOG_PATH}")

if __name__ == "__main__":
    main()
