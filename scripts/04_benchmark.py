from __future__ import annotations

import time
import sqlite3
from pathlib import Path
from statistics import median

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SQLITE_PATH = ROOT / "db" / "sqlite.db"
DUCKDB_PATH = ROOT / "db" / "duckdb.db"

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_PATH = LOG_DIR / "benchmark.log"

WARMUP = 1
REPEATS = 5
DUCKDB_THREADS = 4

QUERIES: list[tuple[str, str]] = [
    ("Q1_conditional_agg_rates", """
        WITH base AS (
          SELECT
            company_name,
            function,
            "year-month" AS ym,
            state_of_residence AS state,
            gender,
            segmentation,
            salary_usd,
            performance_score,
            flag_leave,
            flag_turnover,
            is_promoted
          FROM data
          WHERE year IN ('2022Y','2023Y','2024Y')
        )
        SELECT
          company_name,
          function,
          ym,
          state,
          gender,
          segmentation,
          COUNT(*) AS n,
          AVG(salary_usd) AS avg_salary,
          AVG(performance_score) AS avg_perf,
          SUM(CASE WHEN performance_score >= 4 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_perf4,
          SUM(CASE WHEN is_promoted = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS promo_rate,
          SUM(flag_leave) * 1.0 / COUNT(*) AS leave_rate,
          SUM(flag_turnover) * 1.0 / COUNT(*) AS turnover_rate
        FROM base
        GROUP BY company_name, function, ym, state, gender, segmentation
    """),

    ("Q2_distinct_counts", """
        SELECT
          "year-month" AS ym,
          company_name,
          COUNT(*) AS rows,
          COUNT(DISTINCT employee_id) AS distinct_employees,
          COUNT(DISTINCT fullname) AS distinct_names
        FROM data
        WHERE state_of_residence IN ('California','New York','Florida')
        GROUP BY ym, company_name
    """),

    ("Q3_topN_per_group", """
        SELECT *
        FROM (
          SELECT
            company_name,
            "year-month" AS ym,
            employee_id,
            salary_usd,
            performance_score,
            ROW_NUMBER() OVER (
              PARTITION BY company_name, "year-month"
              ORDER BY salary_usd DESC
            ) AS rn
          FROM data
          WHERE year='2024Y'
        )
        WHERE rn <= 10
    """),

    ("Q4_running_total", """
        WITH m AS (
          SELECT
            company_name,
            "year-month" AS ym,
            SUM(salary_usd) AS monthly_salary
          FROM data
          GROUP BY company_name, "year-month"
        )
        SELECT
          company_name,
          ym,
          monthly_salary,
          SUM(monthly_salary) OVER (
            PARTITION BY company_name
            ORDER BY ym
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS cumulative_salary
        FROM m
    """),

    ("Q5_join_vs_avg", """
        WITH avg_by_grp AS (
          SELECT
            company_name,
            "year-month" AS ym,
            AVG(salary_usd) AS avg_salary
          FROM data
          GROUP BY company_name, "year-month"
        )
        SELECT
          d.company_name,
          d."year-month" AS ym,
          COUNT(*) AS above_avg_count
        FROM data d
        JOIN avg_by_grp a
          ON d.company_name = a.company_name
         AND d."year-month" = a.ym
        WHERE d.salary_usd > a.avg_salary
        GROUP BY d.company_name, d."year-month"
    """),

    ("Q6_selective_like_filter", """
        SELECT
          company_name,
          function,
          "year-month" AS ym,
          COUNT(*) AS n,
          AVG(salary_usd) AS avg_salary
        FROM data
        WHERE year='2023Y'
          AND state_of_residence='California'
          AND function LIKE '%Sales%'
          AND employee_type='White-Collar'
        GROUP BY company_name, function, ym
    """),
]

def log(line: str) -> None:
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def now_s() -> float:
    return time.perf_counter()

def strip_trailing_semicolon(sql: str) -> str:
    s = sql.strip()
    if s.endswith(";"):
        s = s[:-1].rstrip()
    return s

def wrap_count(sql: str) -> str:
    inner = strip_trailing_semicolon(sql)
    return f"SELECT COUNT(*) FROM ({inner}) t;"

def run_sqlite_count(conn: sqlite3.Connection, sql: str) -> int:
    cur = conn.cursor()
    cur.execute(wrap_count(sql))
    out = cur.fetchone()
    return int(out[0]) if out else 0


def run_duckdb_count(conn: duckdb.DuckDBPyConnection, sql: str) -> int:
    out = conn.execute(wrap_count(sql)).fetchone()
    return int(out[0]) if out else 0

def benchmark_engine(name: str, runner, queries: list[tuple[str, str]]) -> None:
    for qname, sql in queries:
        for _ in range(WARMUP):
            _ = runner(sql)

        times: list[float] = []
        last_rows: int = 0

        for _ in range(REPEATS):
            t0 = now_s()
            last_rows = runner(sql)
            t1 = now_s()
            times.append(t1 - t0)

        med = median(times)
        log(f"{name} | {qname} | {med:.4f}s | rows={last_rows}")


def ensure_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")
    conn.commit()


def main() -> None:
    if not SQLITE_PATH.exists():
        raise FileNotFoundError(f"SQLite db not found: {SQLITE_PATH}")
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB db not found: {DUCKDB_PATH}")

    if LOG_PATH.exists():
        LOG_PATH.unlink()

    log("meta | benchmark_start")
    log(f"meta | warmup={WARMUP} repeats={REPEATS}")
    log(f"meta | sqlite_db={SQLITE_PATH}")
    log(f"meta | duckdb_db={DUCKDB_PATH}")

    sqlite_conn = sqlite3.connect(SQLITE_PATH.as_posix())
    ensure_sqlite_pragmas(sqlite_conn)

    duck_conn = duckdb.connect(DUCKDB_PATH.as_posix())
    duck_conn.execute(f"PRAGMA threads={DUCKDB_THREADS};")

    s_n = sqlite_conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    d_n = duck_conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    log(f"verify | rowcount | sqlite={s_n} duckdb={d_n}")

    log("bench | start | engine=sqlite")
    benchmark_engine("sqlite", lambda q: run_sqlite_count(sqlite_conn, q), QUERIES)

    log("bench | start | engine=duckdb")
    benchmark_engine("duckdb", lambda q: run_duckdb_count(duck_conn, q), QUERIES)

    sqlite_conn.close()
    duck_conn.close()

    log("meta | benchmark_done")
    log(f"meta | log_file={LOG_PATH}")

if __name__ == "__main__":
    main()
