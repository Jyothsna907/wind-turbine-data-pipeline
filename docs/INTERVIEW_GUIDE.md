# Interview Guide — Wind Turbine PySpark Assessment

## 1. 90-second walkthrough

"I treated this as a small but production-minded batch data pipeline. The source is three CSV files, each containing five turbines, with hourly readings. I read them with an explicit Spark schema, validate the natural key of turbine ID plus timestamp, and deduplicate because the files are appended and may be reprocessed.

The important reliability requirement is missing sensor entries. Instead of only checking for nulls, I generate the expected hourly timeline for each turbine and left join the actual measurements. That lets me detect an entire missing row, not just a missing value inside an existing row.

For cleaning, I keep sensor-data quality separate from business anomaly detection. I flag invalid wind direction and negative values, and use per-turbine IQR thresholds for extreme wind speed and power readings. I then impute continuous values using a turbine/day median with a turbine-level fallback. For wind direction I use previous/next valid values because direction is circular.

After cleaning, I calculate daily min, max, average and standard deviation for each turbine, plus completeness and imputation metrics. For the requested anomaly rule, I compare each turbine's 24-hour average with the fleet's daily mean, and flag anything outside mean plus or minus two standard deviations. Finally I persist cleaned and summary data into Spark SQL managed tables.

I kept I/O separate from transformations so the logic is easy to unit test. For production I would move to incremental object-storage ingestion and ACID lakehouse tables with partition-level upserts, monitoring and quarantine handling." 

---

## 2. Explain the solution using Scenario, Action, Result

### Scenario

"The wind farm has 15 turbines sending hourly measurements. The files are appended daily, and sensor malfunctions can mean either missing values or an entirely missing hourly measurement. The business wants clean data, daily turbine statistics and a simple two-standard-deviation anomaly flag."

### Action

"I built the pipeline as small PySpark transformations. I used an explicit schema and a turbine/timestamp key, generated the expected hourly grid to find missing records, cleaned bad sensor readings, imputed gaps, aggregated by turbine and day, and then calculated a fleet-level daily baseline for anomaly detection. I also added tests that deliberately remove a sensor entry and inject invalid values because the supplied month itself is clean."

### Result

"The result is an idempotent PoC that produces a cleaned reading table and a daily turbine summary containing min, max, average, completeness, z-score and anomaly flag. The supplied raw month contains all 11,160 expected readings; using the daily fleet 2-sigma interpretation, it produces 17 turbine/day anomaly flags. More importantly, the tests demonstrate how the code responds when the source becomes incomplete or dirty."

---

## 3. Why did you generate an expected hourly grid?

"A normal `isNull()` check is not enough. If the sensor completely misses 13:00, there is no row whose columns are null. I need to know what *should* have arrived. Because the brief tells me measurements are hourly, I generate all expected turbine/timestamp combinations and compare actual against expected. That also gives me a completeness percentage per turbine/day."

---

## 4. Why median imputation rather than mean?

"The median is more robust when a sensor has already produced extreme readings. I first null out values that fail the bad-reading checks, then use the turbine/day median. If a whole day has insufficient valid values, I fall back to that turbine's median across the loaded period. In a real energy platform I would consider model-based imputation using wind-speed/power curves rather than a simple statistical fill."

---

## 5. Why not take a median of wind direction?

"Direction is circular: 359 degrees and 1 degree are both close to north, but their arithmetic median/average can be misleading. For this PoC I use the nearest previous or next valid direction. A production system could use circular statistics or vector averaging with sine/cosine components."

---

## 6. How exactly is an anomaly defined?

"I aggregate power to one daily average per turbine first. For each day, I calculate the fleet mean and sample standard deviation across the 15 turbine averages. Then I flag a turbine if its daily average is below `mean - 2*stddev` or above `mean + 2*stddev`. I also store the z-score so the result is explainable."

Formula:

```text
z = (turbine_daily_avg - fleet_daily_mean) / fleet_daily_stddev
anomaly = abs(z) > 2
```

---

## 7. Why compare against the fleet rather than the turbine's own 24 hourly readings?

"The wording says identify turbines that deviate from their expected output over the same period. My interpretation is that the other turbines provide a peer baseline for that 24-hour period. If I used each turbine's own hourly mean ±2 sigma, I would mostly be flagging individual measurements, not identifying an under-performing turbine. I would confirm this definition with the product/data owner in a real project."

A very strong interview point is to explicitly call this an **assumption**, not a fact hidden in the brief.

---

## 8. What if weather conditions differ across the farm?

"Then a simple fleet 2-sigma baseline can produce false positives. In production I would build an expected-power model using wind speed, direction, turbine power curves, availability/maintenance state and possibly location. The 2-sigma method is appropriate here because it is the assessment's requested rule, not because I think it is the final predictive-maintenance model."

---

## 9. Why IQR for cleaning?

"The brief says the raw data contains outliers but does not give a rated turbine capacity or physical threshold. I avoid inventing a maximum MW rating. Per-turbine IQR gives me a robust generic way to identify extreme sensor readings, while explicit physical checks catch things such as negative power and direction outside 0–359 degrees."

If challenged: "For a real turbine I would replace generic IQR thresholds with engineering limits and manufacturer power-curve rules."

---

## 10. What makes the code scalable?

Good answer:

"I avoid Python row loops and keep the processing in Spark DataFrame operations, aggregations and window functions. The core transformations are distributed. The PoC currently does a full-month overwrite because that is simple and idempotent for the supplied dataset; for real scale I would process only new/changed date partitions and use MERGE/upsert semantics."

Important: do **not** claim the current PoC is already fully production-scaled. Acknowledge what you intentionally left simple.

---

## 11. What makes it testable?

"The transformation functions do not read files or write tables. They accept a DataFrame and return a DataFrame. That allows small deterministic Spark fixtures for specific behaviour: missing-hour reconstruction, dirty-reading cleaning, imputation and the two-sigma anomaly calculation."

---

## 12. What would you productionise first?

Prioritise these in the interview:

1. Incremental ingestion with a file manifest/checkpoint rather than full rereads.
2. Bronze/Silver/Gold ACID tables and partition-level MERGE.
3. Quarantine bad schema/keys instead of dropping them.
4. Monitoring for missing readings, imputation %, duplicates, late data and anomaly rate.
5. CI/CD and integration testing.
6. Replace generic anomaly baseline with expected-output model using wind conditions and turbine characteristics.

---

## 13. Likely technical follow-up questions

### What happens if the same turbine/timestamp appears twice with different values?

"The PoC de-duplicates by key. In production I would not silently choose one conflicting value. I would preserve ingestion metadata, apply a deterministic latest-record rule if the source has an event/version field, and quarantine conflicting duplicates for investigation."

### What happens if the whole turbine is missing for a day?

"The expected-grid design can reveal missing hours as long as the turbine exists in the loaded reference set. For a streaming production design I would maintain a turbine master/reference table so expected turbines do not disappear just because no record arrived in today's file."

### Why sample standard deviation?

"I am treating the 15 turbines as the observed fleet sample for the period. The practical difference from population standard deviation is small here, but I would keep the choice explicit and consistent."

### How do you avoid duplicate results when the CSV is appended daily?

"The input natural key is turbine ID plus timestamp and the final PoC load is a deterministic full refresh. In production I would track processed files/offsets and MERGE by the same key into date-partitioned storage."

### How would you handle late-arriving data?

"I would keep a configurable lookback window, reprocess affected partitions, and MERGE by turbine/timestamp. Monitoring would distinguish late data from permanently missing data."

### Why explicit schema instead of inferSchema?

"It avoids extra scans and prevents silent type drift. It also turns the source contract into code that can be validated."

---

## 14. GenAI answer — be transparent

"I did use GenAI as an accelerator for some scaffolding, edge-case review and documentation/test structure. I did not treat its output as authoritative. I made the assumptions, reviewed the transformations, checked the supplied data profile, and I can explain or change every part of the implementation."

If they ask what you personally changed/decided, focus on:

- detecting missing *rows* through an expected timeline;
- separating bad-reading outliers from business anomalies;
- choosing median/fallback imputation;
- choosing the fleet daily baseline interpretation;
- productionisation trade-offs.

---

## 15. Three points to remember if you get nervous

1. **Expected grid** — catches an entirely missing sensor row.
2. **Cleaning ≠ anomaly detection** — don't delete genuine under-performance.
3. **PoC full refresh; production incremental MERGE** — show you understand the trade-off.
