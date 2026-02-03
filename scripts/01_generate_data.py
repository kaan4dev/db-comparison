from __future__ import annotations
import time
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

N_ROWS = 10_000_000
CHUNK = 500_000  
SEED = 42
DUCKDB_THREADS = 4

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

PARQUET_DIR = DATA_DIR / "data_10m"

DATE_RANGE = np.array(["2022-01", "2022-02", "2022-03", "2022-04", "2022-05"])
YEAR_MONTH = DATE_RANGE 
STATES = np.array(["California", "Oregon", "Massachusetts", "New York", "Utah", "Florida"])
PERF_SCORE = np.array([1.0, 2.0, 3.0, 4.0])
TOP_PERFORMER = np.array(["More Impact Needed","Performer","Not Meeting Expectations","Top Performer",])
TOP_TALENT = np.array(["Unrated", "Talent", "Top Talent"])
LEAVE_REASON = np.array(["Contract Termination - Employee Resignation","Contract Termination - Employer",])
LEAVE_REASON_DETAIL = np.array(["Retirement","Career Expectation","Job Dissatisfaction","Salary and Benefits","Low Performance",])
REGRETTED_STATUS = np.array(["Regretted", "Unregretted"])
COMPANY = np.array(list("BCAFDEHIG"))
FUNCTION = np.array(["Logistics / Supply Chain","Sales","Maintenance","After-Sales Services & Technical Training","Administrative Affairs",])
EMPLOYEE_TYPE = np.array(["Gray-Collar", "White-Collar"])
YEAR = np.array(["2022Y", "2023Y", "2024Y"])
MARITAL = np.array(["Married", "Single"])
GENDER = np.array(["Male", "Female"])
SEGMENT = np.array(["Work in Place", "Hybrid", "Remote"])
DAYS_IN_MONTH = np.array([28, 29, 30, 31])
NUM_CHILDREN = np.array(["2.0", "0.0", "1.0", "3+"])
EDU = np.array(["High School","Graduate Degree","Bachelor Degree","Associate Degree","Middle School","Grade School","Doctoral Degree",])
AGE_GROUP = np.array(["40-50", "25-30", "20-25", "50+", "30-35", "35-40", "20-"])
TENURE_GROUP = np.array(["10-15 Years","2-3 Years","6 Months-","6-12 Months","5-10 Years","1-2 Years","3-5 Years","15 Years+",])
TITLE = np.array(["Software Engineer","Data Engineer","Analyst","Team Lead","Manager","Director",])
GRADE = np.array(["Officer","Specialist","First-Level Management","Mid-Level Management","Top Management",])
SURVEY_DATE = np.array(["2022-01", "2022-02", "2022-03", "2022-04", "2022-06"])
SURVEY_ANS_1_2 = np.array(["High", "Very High", "Medium", "I have No idea", "Low"])
SURVEY_ANS_3 = np.array(["Very High", "Medium", "High", "I have No idea", "Low"])
SURVEY_WILLINGNESS = np.array(["wishing for change", "satisfied with the situation"])
SURVEY_EMOTIONAL = np.array(["negative", "positive", "neutral"])


def make_chunk(rng: np.random.Generator, n: int) -> pd.DataFrame:
    emp_num = rng.integers(1, 500_000, size=n, dtype=np.int64)
    employee_id = np.char.add("E", np.char.zfill(emp_num.astype(str), 6))
    year_month = rng.choice(YEAR_MONTH, size=n)
    date_range = rng.choice(DATE_RANGE, size=n)
    first = rng.choice(["Alex", "Sam", "Taylor", "Jordan", "Casey", "Morgan", "Jamie"], size=n)
    last = rng.choice(["Smith", "Brown", "Johnson", "Lee", "Garcia", "Miller", "Davis"], size=n)
    fullname = np.char.add(np.char.add(first, " "), last)
    state_of_residence = rng.choice(STATES, size=n)
    performance_score = rng.choice(PERF_SCORE, size=n).astype(np.float64)
    top_performer = rng.choice(TOP_PERFORMER, size=n)
    top_talent = rng.choice(TOP_TALENT, size=n)
    last_12_months = np.ones(n, dtype=np.float64)
    flag_current = np.ones(n, dtype=np.float64)
    leave_reason = rng.choice(LEAVE_REASON, size=n)
    leave_reason_detail = rng.choice(LEAVE_REASON_DETAIL, size=n)
    regretted_status = rng.choice(REGRETTED_STATUS, size=n)
    flag_leave = rng.integers(0, 2, size=n, dtype=np.int64)
    flag_hire = rng.integers(0, 2, size=n, dtype=np.int64)
    flag_turnover = rng.integers(0, 2, size=n, dtype=np.int64)
    company_name = rng.choice(COMPANY, size=n)
    function = rng.choice(FUNCTION, size=n)
    employee_type = rng.choice(EMPLOYEE_TYPE, size=n)
    year = rng.choice(YEAR, size=n)
    marital_status = rng.choice(MARITAL, size=n)
    gender = rng.choice(GENDER, size=n)
    segmentation = rng.choice(SEGMENT, size=n)
    working_percentage = rng.choice([0.5, 0.8, 1.0], size=n).astype(np.float64)
    days_in_month = rng.choice(DAYS_IN_MONTH, size=n).astype(np.int64)
    days_worked = (rng.random(size=n) * days_in_month).astype(np.int64)
    active_working_days = days_worked.astype(np.float64)
    num_of_children = rng.choice(NUM_CHILDREN, size=n)
    has_child = (num_of_children != "0.0").astype(np.int64)
    education_level = rng.choice(EDU, size=n)
    birth_year = rng.integers(1960, 2005, size=n, dtype=np.int64)
    age = (2025 - birth_year).astype(np.int64)
    age_group = rng.choice(AGE_GROUP, size=n)
    seniority_start_yearmonth = rng.choice(["2010-01", "2015-06", "2018-09", "2020-02", "2021-11"],size=n,)
    tenure = (rng.random(size=n) * 20).astype(np.float64)
    tenure_group = rng.choice(TENURE_GROUP, size=n)
    is_promoted = rng.integers(0, 2, size=n).astype(np.float64)
    title = rng.choice(TITLE, size=n)
    grade = rng.choice(GRADE, size=n)
    survey_date = rng.choice(SURVEY_DATE, size=n)
    survey_q1_answer = rng.choice(SURVEY_ANS_1_2, size=n)
    survey_q2_answer = rng.choice(SURVEY_ANS_1_2, size=n)
    survey_q3_answer = rng.choice(SURVEY_ANS_3, size=n)
    survey_willingness_to_change = rng.choice(SURVEY_WILLINGNESS, size=n)
    survey_emotional_state = rng.choice(SURVEY_EMOTIONAL, size=n)
    turnover_within_next_6_months = rng.integers(0, 2, size=n).astype(np.float64)
    turnover_within_next_3_months = rng.integers(0, 2, size=n).astype(np.float64)
    turnover_in_next_month = rng.integers(0, 2, size=n).astype(np.float64)
    salary_usd = rng.lognormal(mean=10.5, sigma=0.3, size=n).astype(np.float64)

    return pd.DataFrame({
        "employee_id": employee_id,
        "date_range": date_range,
        "fullname": fullname,
        "state_of_residence": state_of_residence,
        "performance_score": performance_score,
        "top_performer": top_performer,
        "top_talent": top_talent,
        "last_12_months": last_12_months,
        "flag_current": flag_current,
        "leave_reason": leave_reason,
        "leave_reason_detail": leave_reason_detail,
        "regretted_status": regretted_status,
        "flag_leave": flag_leave,
        "flag_hire": flag_hire,
        "company_name": company_name,
        "function": function,
        "employee_type": employee_type,
        "year": year,
        "marital_status": marital_status,
        "gender": gender,
        "segmentation": segmentation,
        "flag_turnover": flag_turnover,
        "working_percentage": working_percentage,
        "days_in_month": days_in_month,
        "days_worked": days_worked,
        "num_of_children": num_of_children,
        "has_child": has_child,
        "education_level": education_level,
        "year-month": year_month,
        "active_working_days": active_working_days,
        "birth_year": birth_year,
        "age": age,
        "age_group": age_group,
        "seniority_start_yearmonth": seniority_start_yearmonth,
        "tenure": tenure,
        "tenure_group": tenure_group,
        "is_promoted": is_promoted,
        "title": title,
        "grade": grade,
        "survey_date": survey_date,
        "survey_q1_answer": survey_q1_answer,
        "survey_q2_answer": survey_q2_answer,
        "survey_q3_answer": survey_q3_answer,
        "survey_willingness_to_change": survey_willingness_to_change,
        "survey_emotional_state": survey_emotional_state,
        "turnover_within_next_6_months": turnover_within_next_6_months,
        "turnover_within_next_3_months": turnover_within_next_3_months,
        "turnover_in_next_month": turnover_in_next_month,
        "salary_usd": salary_usd,
    })


def main():
    PARQUET_DIR.mkdir(exist_ok=True)

    existing_parts = sorted(PARQUET_DIR.glob("part_*.parquet"))
    if existing_parts:
        print(f"[skip] dataset already exists: {PARQUET_DIR} (parts={len(existing_parts)})")
        return

    rng = np.random.default_rng(SEED)
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")

    print(f"[start] generating parquet dataset: rows={N_ROWS:,} chunk={CHUNK:,} -> {PARQUET_DIR}")
    t0 = time.perf_counter()

    rows_written = 0
    part = 0

    while rows_written < N_ROWS:
        n = min(CHUNK, N_ROWS - rows_written)
        df = make_chunk(rng, n)

        con.register("df", df)

        out_path = (PARQUET_DIR / f"part_{part:06d}.parquet").as_posix()
        con.execute(f"COPY df TO '{out_path}' (FORMAT PARQUET);")

        rows_written += n
        part += 1
        print(f"[progress] rows_written={rows_written:,} parts={part}")

    t1 = time.perf_counter()
    print(f"[done] seconds={(t1 - t0):.3f} dataset_dir={PARQUET_DIR} parts={part}")

    con.close()

if __name__ == "__main__":
    main()
