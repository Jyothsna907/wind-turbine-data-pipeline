# Wind Turbine Data Pipeline — Take-Home Assessment

## 1. Solution summary

This proof-of-concept implements a testable PySpark pipeline for 15 wind turbines whose hourly readings arrive in three appended CSV files (five turbines per file).

The pipeline:

1. **Ingests** all CSVs with an explicit schema.
2. **Deduplicates** records by `(turbine_id, timestamp)` so reruns do not double-count appended data.
3. **Detects missed sensor entries** by generating the expected hourly timeline for every turbine.
4. **Cleans bad readings**:
   - wind direction must be in `[0, 360)`;
   - negative wind speed or power is invalid;
   - extreme wind speed/power readings are detected using per-turbine IQR thresholds.
5. **Imputes missing/invalid values**:
   - wind speed and power output use turbine/day median, then turbine-level median as fallback;
   - wind direction uses nearest previous/next valid reading because direction is circular and a numeric median is not meaningful.
6. **Calculates daily statistics per turbine**: minimum, maximum, average and standard deviation of power output, plus data-completeness metrics.
7. **Flags turbine-level performance anomalies** when a turbine's daily average power is outside the fleet daily mean ± 2 standard deviations.
8. **Stores** the cleaned readings and daily summary/anomaly results in Spark SQL managed tables.

The transformations are separated from I/O so they can be unit tested independently.

---

## 2. Architecture

```text
CSV files (3 groups, 5 turbines each)
              |
              v
       Explicit-schema ingest
              |
              v
    key validation + de-duplication
              |
              v
   expected hourly turbine/timestamp grid
              |
              v
      bad-reading/outlier cleaning
              |
              v
          imputation
              |
       +------+-------+
       |              |
       v              v
cleaned readings   daily min/max/avg/stddev
       |              |
       |              v
       |       fleet mean/stddev baseline
       |              |
       |              v
       |       2-sigma anomaly flags
       |              |
       +------+-------+
              v
       Spark SQL database
  - cleaned_turbine_readings
  - daily_turbine_summary
```

---

## 3. Why anomaly detection is separate from outlier cleaning

There are two different problems:

- A sensor can emit a **bad individual reading**, e.g. `wind_speed=100` or `power_output=-5`. That should be cleaned/imputed.
- A turbine can be **genuinely under-performing** for a day. That is a business anomaly and should remain visible.

If I used the same 2-standard-deviation rule to delete power readings before the anomaly step, I could remove genuine performance issues. I therefore use robust IQR/range checks for sensor-quality cleaning, and reserve the requested 2-sigma rule for turbine-level performance anomalies.

---

## 4. Assumptions

1. Turbines are expected to report **once per hour**.
2. `(turbine_id, timestamp)` is the natural measurement key.
3. A turbine remains in the same source group/file, but downstream logic does not depend on the file name.
4. Wind direction is valid from `0` to `<360` degrees.
5. Negative wind speed or negative generated power is invalid.
6. No rated turbine capacity was provided, so I do **not** hard-code a maximum MW value. IQR is used for extreme power/wind readings.
7. "Expected power output over the same time period" is interpreted as the peer fleet's distribution of **24-hour turbine average power** for the day.
8. This PoC uses a full refresh into managed Spark tables because the supplied data is only one month. A production implementation would use date-partitioned incremental MERGE/upsert logic.

---

## 5. Project structure

```text
wind_turbine_assessment/
├── data/
│   ├── data_group_1.csv
│   ├── data_group_2.csv
│   └── data_group_3.csv
├── docs/
│   ├── INTERVIEW_GUIDE.md
│   └── DATA_PROFILE.md
├── scripts/
│   └── make_dirty_sample.py
├── tests/
│   ├── conftest.py
│   ├── test_cleaning.py
│   └── test_transformations.py
├── wind_pipeline/
│   ├── __init__.py
│   ├── io.py
│   ├── schema.py
│   └── transformations.py
├── run_pipeline.py
├── requirements.txt
└── README.md
```

---

## 6. How to run

### Prerequisites

- Python 3.10+
- Java 8/11/17 compatible with your chosen Spark build

### Setup

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate

pip install -r requirements.txt
```

### Run the supplied data

From the project root:

```bash
spark-submit run_pipeline.py --input "data/*.csv"
```

The pipeline writes:

- `wind_turbine.cleaned_turbine_readings`
- `wind_turbine.daily_turbine_summary`

The second table contains the daily min/max/average, completeness measures, fleet baseline, z-score and anomaly flag.

### Run tests

```bash
pytest -q
```

### Demonstrate dirty-data handling

The supplied month is clean, so an optional script creates a deliberately dirty copy without changing the source data:

```bash
python scripts/make_dirty_sample.py
```

It introduces:

- one missing hourly row;
- one null power reading;
- an extreme wind-speed reading;
- negative power output;
- an invalid wind direction.

To run a full dirty-data demo, copy groups 2 and 3 into `data_dirty/` and run:

```bash
spark-submit run_pipeline.py --input "data_dirty/*.csv"
```

---

## 7. Data-quality and test strategy

Unit tests cover the highest-risk logic rather than only testing happy paths:

- a missing turbine/hour is reconstructed and flagged;
- implausible/extreme readings are flagged before imputation;
- null values are imputed;
- invalid wind direction is handled;
- a turbine more than two standard deviations from fleet mean is identified as anomalous.

In production I would add:

- schema-contract tests;
- duplicate-conflict quarantine tests;
- null-rate/completeness thresholds;
- reconciliation between ingested and published counts;
- integration tests against representative object-storage/database infrastructure;
- data-quality alerts and operational metrics.

---

## 8. Supplied-data observations

The provided CSVs contain:

- **15 turbines** across three files;
- **744 hourly timestamps** from `2022-03-01 00:00:00` through `2022-03-31 23:00:00`;
- **11,160 total readings** (`15 × 744`);
- no null fields in the supplied raw month;
- no missing turbine/hour combinations in the supplied raw month.

Using the stated interpretation of the 2-standard-deviation requirement (daily turbine average vs the fleet's daily turbine-average distribution), the supplied data produces **17 turbine/day anomaly flags**. See `docs/DATA_PROFILE.md` for the reference list.

---

## 9. Productionising the PoC

I would keep the transformation functions but change the ingestion/storage/operations layer:

1. Land raw files in cloud object storage and keep them immutable.
2. Use an incremental mechanism (e.g. Spark Structured Streaming/Auto Loader or file manifest/watermark) rather than rereading the whole month.
3. Use Bronze/Silver/Gold layers:
   - Bronze: raw immutable data + ingestion metadata;
   - Silver: validated, deduplicated, cleaned data;
   - Gold: turbine/day statistics and anomaly results.
4. Use Delta/Iceberg/Hudi or a warehouse/lakehouse table supporting ACID MERGE for idempotent upserts.
5. Partition by event date; optimise/clustering by turbine where useful.
6. Quarantine malformed rows instead of silently dropping them.
7. Add orchestration, retries, alerting, SLAs and late-arriving-data handling.
8. Add data lineage/catalogue, role-based access and retention policies.
9. CI/CD: linting, unit tests, integration tests and deployment promotion between environments.
10. Monitoring: input file count, duplicate count, missing-hour count, imputation rate, anomaly count, row counts, processing latency and failed records.

---

## 10. AI usage

If asked, be transparent. A suitable statement is:

> I used a generative-AI assistant to accelerate some project scaffolding, review edge cases and help draft test/documentation structure. I made and validated the design choices myself, particularly the distinction between sensor outlier cleaning and turbine performance anomalies, the imputation strategy, and the productionisation approach. I also walked through the code so I could explain and modify every transformation in the interview.

Do not claim that no AI was used if it was used. The important point in the interview is being able to explain, challenge and change the code yourself.
