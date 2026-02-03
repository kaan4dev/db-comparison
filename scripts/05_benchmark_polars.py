from __future__ import annotations

import time
from pathlib import Path
from statistics import median

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
PARQUET_GLOB = (ROOT / "data" / "data_10m" / "*.parquet").as_posix()

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_PATH = LOG_DIR / "benchmark_polars.log"

WARMUP = 1
REPEATS = 5

def log(line: str) -> None:
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def now_s() -> float:
    return time.perf_counter()

def scan() -> pl.LazyFrame:
    return pl.scan_parquet(PARQUET_GLOB)

def q1_conditional_agg_rates(lf: pl.LazyFrame) -> pl.LazyFrame:
    base = (
        lf.filter(pl.col("year").is_in(["2022Y", "2023Y", "2024Y"]))
        .select([
            pl.col("company_name"),
            pl.col("function"),
            pl.col("year-month").alias("ym"),
            pl.col("state_of_residence").alias("state"),
            pl.col("gender"),
            pl.col("segmentation"),
            pl.col("salary_usd"),
            pl.col("performance_score"),
            pl.col("flag_leave"),
            pl.col("flag_turnover"),
            pl.col("is_promoted"),
        ])
    )

    return (
        base.group_by(["company_name", "function", "ym", "state", "gender", "segmentation"])
        .agg([
            pl.len().alias("n"),
            pl.col("salary_usd").mean().alias("avg_salary"),
            pl.col("performance_score").mean().alias("avg_perf"),
            (pl.col("performance_score") >= 4).cast(pl.Int64).sum().alias("cnt_perf4"),
            (pl.col("is_promoted") == 1).cast(pl.Int64).sum().alias("cnt_promoted"),
            pl.col("flag_leave").sum().alias("sum_leave"),
            pl.col("flag_turnover").sum().alias("sum_turnover"),
        ])
        .with_columns([
            (pl.col("cnt_perf4") / pl.col("n")).alias("pct_perf4"),
            (pl.col("cnt_promoted") / pl.col("n")).alias("promo_rate"),
            (pl.col("sum_leave") / pl.col("n")).alias("leave_rate"),
            (pl.col("sum_turnover") / pl.col("n")).alias("turnover_rate"),
        ])
        .drop(["cnt_perf4", "cnt_promoted", "sum_leave", "sum_turnover"])
    )


def q2_distinct_counts(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.filter(pl.col("state_of_residence").is_in(["California", "New York", "Florida"]))
        .group_by([pl.col("year-month").alias("ym"), pl.col("company_name")])
        .agg([
            pl.len().alias("rows"),
            pl.col("employee_id").n_unique().alias("distinct_employees"),
            pl.col("fullname").n_unique().alias("distinct_names"),
        ])
    )


def q3_topN_per_group(lf: pl.LazyFrame) -> pl.LazyFrame:
    base = (
        lf.filter(pl.col("year") == "2024Y")
        .select([
            pl.col("company_name"),
            pl.col("year-month").alias("ym"),
            pl.col("employee_id"),
            pl.col("salary_usd"),
            pl.col("performance_score"),
        ])
    )

    ranked = base.with_columns(
        pl.col("salary_usd")
        .rank(method="dense", descending=True)
        .over(["company_name", "ym"])
        .alias("rn")
    )

    return ranked.filter(pl.col("rn") <= 10)


def q4_running_total(lf: pl.LazyFrame) -> pl.LazyFrame:
    monthly = (
        lf.group_by([pl.col("company_name"), pl.col("year-month").alias("ym")])
        .agg(pl.col("salary_usd").sum().alias("monthly_salary"))
        .sort(["company_name", "ym"])
    )

    return monthly.with_columns(
        pl.col("monthly_salary").cum_sum().over("company_name").alias("cumulative_salary")
    )


def q5_join_vs_avg(lf: pl.LazyFrame) -> pl.LazyFrame:
    avg_by_grp = (
        lf.group_by([pl.col("company_name"), pl.col("year-month").alias("ym")])
        .agg(pl.col("salary_usd").mean().alias("avg_salary"))
    )

    joined = (
        lf.select([
            pl.col("company_name"),
            pl.col("year-month").alias("ym"),
            pl.col("salary_usd"),
        ])
        .join(avg_by_grp, on=["company_name", "ym"], how="inner")
        .filter(pl.col("salary_usd") > pl.col("avg_salary"))
    )

    return joined.group_by(["company_name", "ym"]).agg(pl.len().alias("above_avg_count"))


def q6_selective_like_filter(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.filter(
            (pl.col("year") == "2023Y")
            & (pl.col("state_of_residence") == "California")
            & (pl.col("function").str.contains("Sales"))
            & (pl.col("employee_type") == "White-Collar")
        )
        .group_by([pl.col("company_name"), pl.col("function"), pl.col("year-month").alias("ym")])
        .agg([
            pl.len().alias("n"),
            pl.col("salary_usd").mean().alias("avg_salary"),
        ])
    )


QUERIES = [
    ("Q1_conditional_agg_rates", q1_conditional_agg_rates),
    ("Q2_distinct_counts", q2_distinct_counts),
    ("Q3_topN_per_group", q3_topN_per_group),
    ("Q4_running_total", q4_running_total),
    ("Q5_join_vs_avg", q5_join_vs_avg),
    ("Q6_selective_like_filter", q6_selective_like_filter),
]

def run_query_count(qfn) -> int:
    lf = scan()
    out_lf = qfn(lf)

    n = out_lf.select(pl.len().alias("n")).collect().item()
    return int(n)

def benchmark_engine(name: str) -> None:
    for qname, qfn in QUERIES:
        # warmup
        for _ in range(WARMUP):
            _ = run_query_count(qfn)

        times: list[float] = []
        last_rows: int = 0

        for _ in range(REPEATS):
            t0 = now_s()
            last_rows = run_query_count(qfn)
            t1 = now_s()
            times.append(t1 - t0)

        med = median(times)
        log(f"{name} | {qname} | {med:.4f}s | rows={last_rows}")


def main() -> None:
    if LOG_PATH.exists():
        LOG_PATH.unlink()

    log("meta | benchmark_start")
    log(f"meta | warmup={WARMUP} repeats={REPEATS}")
    log(f"meta | parquet_glob={PARQUET_GLOB}")

    t0 = now_s()
    n = scan().select(pl.len().alias("n")).collect().item()
    t1 = now_s()
    log(f"verify | rowcount | polars={int(n)} | seconds={(t1 - t0):.4f}")

    log("bench | start | engine=polars")
    benchmark_engine("polars")

    log("meta | benchmark_done")
    log(f"meta | log_file={LOG_PATH}")


if __name__ == "__main__":
    main()
