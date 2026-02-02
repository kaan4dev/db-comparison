## 1. Çok Boyutlu Koşullu Gruplama (KPI Analizi)

**İşlev:** Birden fazla boyut üzerinden temel performans göstergelerini (Maaş ortalaması, Churn hızı vb.) hesaplar.
Çok yüksek kardinaliteye sahip bir `GROUP BY` işlemi yapar. 10 milyon satırı tararken aynı anda matematiksel bölme ve koşullu toplam (`CASE WHEN`) işlemlerini koşturur.

```sql
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
GROUP BY company_name, function, ym, state, gender, segmentation;

```

---

## 2. Benzersiz (DISTINCT) Çalışan Sayımı

**İşlev:** Belirli eyaletlerdeki toplam kayıt sayısını ve bu kayıtlar içindeki benzersiz çalışan sayısını bulur.
`COUNT(DISTINCT)` işlemi işlemciyi ve belleği en çok yoran işlemlerden biridir. Veritabanının "hashing" (karma) algoritmalarının verimliliğini test eder.

```sql
SELECT
  "year-month" AS ym,
  company_name,
  COUNT(*) AS rows,
  COUNT(DISTINCT employee_id) AS distinct_employees,
  COUNT(DISTINCT fullname) AS distinct_names
FROM data
WHERE state_of_residence IN ('California','New York','Florida')
GROUP BY ym, company_name;

```

---

## 3. Grup Bazında En İyiler (Ranking)

**İşlev:** Her şirket ve ay bazında en yüksek maaş alan ilk 10 çalışanı listeler.
`PARTITION BY` ve `ORDER BY` içeren bir **Window Function** kullanır. Verinin bellek içinde parçalanma ve sıralanma hızını ölçer.

```sql
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
WHERE rn <= 10;

```

---

## 4. Kümülatif Maaş Harcaması (Trend Analizi)

**İşlev:** Şirketlerin aylık maaş giderlerini hesaplar ve yıl başından itibaren her ayın üzerine ekleyerek kümülatif toplamı (running total) oluşturur.
Zaman serisi analizlerinde kullanılan "frame" (çerçeve) mantığının hızını ölçer.

```sql
WITH m AS (
  SELECT
    company_name,
    "year-month" AS ym,
    SUM(salary_usd) AS monthly_salary
  FROM data
  GROUP BY company_name, ym
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
FROM m;

```

---

## 5. Ortalamanın Üstünde Alanlar (JOIN & Subquery)

**İşlev:** Kendi şirketinin o ayki maaş ortalamasından daha fazla kazanan kaç çalışan olduğunu bulur.
Önce büyük bir veri setini gruplar (aggregation), ardından bu özeti ana tabloyla birleştirir (JOIN).

```sql
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
GROUP BY d.company_name, d."year-month";

```
