# Demo 1 — Multi-Task Job Orchestration

This demo shows a complete Databricks multi-task Job: ingest → validate → **If/Else** → transform → gold, with notification on "no data" and failure + repair handling.

Everything lives in catalog **`demo1`**. The Job: **Orchestration Demo 1 - Multi-Task Job**.

---

## The pipeline (6 tasks)

```
ingest (COPY INTO from volume)
   → validate (count NEW rows by ingested_at timestamp)
      → check_row_count   (IF/ELSE condition, no compute: row_count > 0 ?)
         ├── TRUE  → transform_publish (clean + SCD1 MERGE into silver)
         │            → gold_kpi (CREATE OR REPLACE KPI table from silver)
         │            → fail_demo
         └── FALSE → notify_no_data
```

* **Task values:** `validate` publishes `row_count`; the condition task compares `{{tasks.validate.values.row_count}} > 0`.
* **How "new data" is defined:** rows with `ingested_at` inside the last `window_minutes` minutes.
* **COPY INTO is smart:** it loads only NEW files from the volume — files it already loaded are skipped automatically.
* **Silver = SCD Type 1:** one row per `order_id`; if the same order arrives again, it is **updated in place** (latest values win, no history kept).
* **Gold = KPIs:** one `CREATE OR REPLACE TABLE` statement rebuilds `gold.sales_kpi` from silver on every run — simple SQL, nothing else.

---

## Job parameters — what to pass and what NOT to pass

When you run the job (Run now → Parameters), you can override these:

| Parameter | Default | When to change it |
| --- | --- | --- |
| `catalog` | `demo1` | Never — leave the default |
| `window_minutes` | `10` | **Scenario B:** pass `0` |
| `allow_success` | `false` | **Scenario C repair run only:** pass `true` |

**Do NOT pass these (they are automatic / internal):**

* `job_id`, `run_id` — the job fills them itself (dynamic values). They are already written to the audit log.
* `row_count` — it is a **task value** that flows internally from `validate` to the condition task and to `notify`.
* `source_file` — this parameter **no longer exists**. COPY INTO always reads the whole volume folder.

---

## Scenario A — IF condition TRUE (new data → transform → gold)

**Goal:** show the happy path with data.

1. Make sure a CSV with rows is in the volume (the preloaded `sample_data.csv` with 10 rows is enough). To look extra live, upload a NEW csv file with a **new file name** and the standard header (see below).
2. Run the job with **defaults** (change nothing).
3. What happens:
   * `ingest`: COPY INTO loads the new rows into `demo1.bronze.sales_orders` and stamps `ingested_at`.
   * `validate`: row_count = 10 → task value published.
   * Condition TRUE → `transform_publish`: 10 raw → 8 clean rows **SCD1-merged into `demo1.silver.sales_orders`** (first run: "8 inserted, 0 updated").
   * `gold_kpi`: `demo1.gold.sales_kpi` is rebuilt from silver with one `CREATE OR REPLACE TABLE`.
   * `fail_demo`: fails on purpose (that is Scenario C material). For an **all-green run**, pass `allow_success = true`.
4. What to show on screen:
   * The run graph (green flow through the TRUE branch).
   * `validate` task → **Output values** → `row_count = 10`.
   * `SELECT * FROM demo1.silver.sales_orders` — the clean data (one row per order).
   * `SELECT * FROM demo1.gold.sales_kpi` — the final KPI table.

---

## Scenario B — ELSE condition FALSE (no new data → notify)

**Goal:** show the If/Else taking the other path.

**Easy way (recommended):** run the job again with **`window_minutes = 0`** (leave everything else default).

* Nothing can be inside a 0-minute window → row_count = 0 → condition FALSE.
* `transform_publish` and `fail_demo` are **skipped** (never run), `notify_no_data` runs instead, and the job still finishes green.

**Empty-file way (optional):** upload a NEW csv that has **only the header row** (a copy of `empty_data.csv` with a new file name). COPY INTO reads it but ingests 0 rows. Use it together with `window_minutes = 0`, or run it as the first run of the day (so no earlier rows fall inside the window).

What to show on screen:

* `notify_no_data` task output: the "NO NEW DATA" notification with row_count = 0.
* The audit log row with status `NO_DATA`.

---

## Scenario C — failure, retries, email, repair (allow_success)

**Goal:** show what happens when a task fails and how to repair it.

1. Run the job with defaults (`allow_success = false`).
2. What happens:
   * Everything runs fine until `fail_demo` (after transform), which **fails on purpose**.
   * The task retries **2 times** (max_retries = 2) → you see 3 attempts in the run page.
   * The run is marked **FAILED** and a **failure email** is sent to your address.
   * This task only runs on the TRUE branch — in Scenario B it is simply skipped.
3. Repair it:
   * Open the failed run → **Repair run**.
   * Change **`allow_success = true`**.
   * Re-run failed tasks → **only `fail_demo` re-runs**; all successful tasks keep their results and are NOT executed again.
4. What to show on screen: the 3 red attempts, the failure email, and the repair run re-running only the failed task.

---

## Where to see the results

| Table | What it holds |
| --- | --- |
| `demo1.bronze.sales_orders` | Raw ingested rows + `ingested_at` timestamp (full history) |
| `demo1.silver.sales_orders` | Clean data, **one row per order_id (SCD1)**, latest values |
| `demo1.gold.sales_kpi` | Business KPIs per category — rebuilt from silver every run |
| `demo1.bronze.job_run_log` | The diary: one row per task per run, **with job_id and run_id** |

Queries you can paste into a SQL editor during the presentation:

```sql
-- the audit diary (show the real job_run_id per task)
SELECT * FROM demo1.bronze.job_run_log ORDER BY logged_at DESC;

-- the clean silver data (one row per order, SCD1)
SELECT * FROM demo1.silver.sales_orders ORDER BY updated_at DESC;

-- the gold KPI table (rebuilt every run)
SELECT * FROM demo1.gold.sales_kpi;

-- how many rows arrived in the last 10 minutes (the validate logic)
SELECT count(*) FROM demo1.bronze.sales_orders
WHERE ingested_at >= current_timestamp() - INTERVAL 10 MINUTES;
```

---

## Good to know (tips)

* **Standard CSV header** for any new data file: `order_id, customer_name, product, category, quantity, unit_price, order_date`.
* COPY INTO skips files it already loaded — if you re-upload the **same file name**, it may be ignored. Always upload new data under a **new file name**.
* Dirty rows (missing quantity/unit_price, negative values, duplicates by `order_id`) are dropped by the transform — 10 raw → 8 clean is the expected result with `sample_data.csv`.
* Re-running the same data shows SCD1 live: first run "8 inserted, 0 updated", next run "0 inserted, 8 updated" — silver never duplicates an order.
* The daily 9 AM UTC schedule is **paused** — all runs are manual for the demo.
* The gold KPI table is dropped and rebuilt on every run — it can never grow duplicates.
