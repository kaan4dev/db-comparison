from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = ROOT / "data" / "data_10m"

DB_DIR = ROOT / "db"
DB_DIR.mkdir(exist_ok=True)

SQLITE_PATH = DB_DIR / "sqlite.db"

CHUNK = 200_000
DUCKDB_THREADS = 4


def apply_sqlite_pragmas(cur: sqlite3.Cursor):
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")


def create_table(cur: sqlite3.Cursor):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS data (
        employee_id TEXT,
        date_range TEXT,
        fullname TEXT,
        state_of_residence TEXT,
        performance_score REAL,
        top_performer TEXT,
        top_talent TEXT,
        last_12_months REAL,
        flag_current REAL,
        leave_reason TEXT,
        leave_reason_detail TEXT,
        regretted_status TEXT,
        flag_leave INTEGER,
        flag_hire INTEGER,
        company_name TEXT,
        function TEXT,
        employee_type TEXT,
        year TEXT,
        marital_status TEXT,
        gender TEXT,
        segmentation TEXT,
        flag_turnover INTEGER,
        working_percentage REAL,
        days_in_month INTEGER,
        days_worked INTEGER,
        num_of_children TEXT,
        has_child INTEGER,
        education_level TEXT,
        "year-month" TEXT,
        active_working_days REAL,
        birth_year INTEGER,
        age INTEGER,
        age_group TEXT,
        seniority_start_yearmonth TEXT,
        tenure REAL,
        tenure_group TEXT,
        is_promoted REAL,
        title TEXT,
        grade TEXT,
        survey_date TEXT,
        survey_q1_answer TEXT,
        survey_q2_answer TEXT,
        survey_q3_answer TEXT,
        survey_willingness_to_change TEXT,
        survey_emotional_state TEXT,
        turnover_within_next_6_months REAL,
        turnover_within_next_3_months REAL,
        turnover_in_next_month REAL,
        salary_usd REAL
    );
    """)

def quote_col(c: str) -> str:
    return f'"{c}"' if ("-" in c or " " in c) else c


def main():
    if not PARQUET_DIR.exists():
        raise FileNotFoundError(f"Parquet dataset dir not found: {PARQUET_DIR}")

    parts = sorted(PARQUET_DIR.glob("part_*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No parquet parts found in: {PARQUET_DIR}")

    if SQLITE_PATH.exists():
        SQLITE_PATH.unlink()

    print(f"[sqlite] create db: {SQLITE_PATH}")
    con = sqlite3.connect(SQLITE_PATH.as_posix())
    cur = con.cursor()

    apply_sqlite_pragmas(cur)
    create_table(cur)
    con.commit()

    dcon = duckdb.connect(database=":memory:")
    dcon.execute(f"PRAGMA threads={DUCKDB_THREADS};")

    glob_path = (PARQUET_DIR / "*.parquet").as_posix()
    print(f"[sqlite] load start: source={glob_path} chunk={CHUNK}")

    res = dcon.execute(f"SELECT * FROM read_parquet('{glob_path}')")
    cols = [d[0] for d in res.description]

    col_list = ",".join(quote_col(c) for c in cols)
    placeholders = ",".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO data ({col_list}) VALUES ({placeholders})"

    inserted = 0
    t0 = time.perf_counter()

    cur.execute("BEGIN;")

    while True:
        batch = res.fetchmany(CHUNK)  
        if not batch:
            break

        cur.executemany(insert_sql, batch)
        inserted += len(batch)
        print(f"[sqlite] progress rows_inserted={inserted:,}")

    con.commit()
    t1 = time.perf_counter()

    n = cur.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    print(f"[sqlite] load done: seconds={(t1 - t0):.3f} rows={n:,}")

    con.close()
    dcon.close()

if __name__ == "__main__":
    main()
