# Demo 2 — Lakeflow Medallion ETL Pipeline (Bronze → Silver → Gold)

A complete ETL pipeline built with **Lakeflow Spark Declarative Pipelines (SDP)** in Databricks.

> **Catalog note:** the workspace catalog for this project is **`demo2`** (there is no separate
> `demo-2` catalog; `demo2` is the existing catalog used by this project).

---

## 1. Project Architecture

```text
demo-2/
│
├── pipeline/
│   ├── transformation/
│   │   ├── bronze/
│   │   │   ├── sales_bronze.sql        → demo2.bronze.sales      (streaming table, Auto Loader)
│   │   │   └── customers_bronze.sql    → demo2.bronze.customers  (streaming table, Auto Loader)
│   │   ├── silver/
│   │   │   ├── sales_silver.sql        → demo2.silver.sales      (AUTO CDC, SCD Type 1)
│   │   │   └── customers_silver.sql   → demo2.silver.customers  (AUTO CDC, SCD Type 2)
│   │   └── gold/
│   │       ├── sales_gold.sql         → demo2.gold.sales_summary, demo2.gold.daily_sales
│   │       └── customers_gold.sql      → demo2.gold.customer_summary, demo2.gold.customer_history
│   └── pipeline_config/                (documentation folder for pipeline configuration)
│
└── README.md
```

Flow:

```text
CSV files (UC volumes)
   │  Auto Loader (read_files) + schema evolution + rescued data
   ▼
BRONZE  demo2.bronze.sales / demo2.bronze.customers      (raw, as-is + _rescued_data + audit columns)
   │  cleaning in temp views (trim, cast, NOT NULL checks → valid_record flag)
   ▼
SILVER  demo2.silver.sales   AUTO CDC SCD Type 1 (key: order_id, sequence: last_modified)
        demo2.silver.customers AUTO CDC SCD Type 2 (key: customer_id, sequence: updated_at)
   │  business aggregations (materialized views, batch reads)
   ▼
GOLD    demo2.gold.sales_summary / daily_sales / customer_summary / customer_history
```

The pipeline itself is **`demo-2-etl-pipeline`** (Lakeflow Spark Declarative Pipeline,
serverless compute, triggered mode, development mode, catalog `demo2`).

---

## 2. Source Data Schemas (CSV)

### 2.1 Sales data

Header row required. One file per batch; later files may contain **updated versions of
existing orders** (same `order_id`, newer `last_modified`) — that is how CDC corrections flow in.

| Column | Type in CSV | Target type (silver) | Role |
| --- | --- | --- | --- |
| `order_id` | integer | INT | **Primary / business key** |
| `customer_id` | integer | INT | Foreign key to customers |
| `product` | string | STRING | Product name |
| `category` | string | STRING | Product category |
| `quantity` | integer | INT | Units sold (may be blank/NULL → row flagged invalid, not dropped) |
| `unit_price` | decimal | DECIMAL(10,2) | Price per unit |
| `order_date` | date `yyyy-M-d` | DATE | Business date of the order |
| `last_modified` | date `yyyy-M-d` | DATE | **CDC sequence column** (source update timestamp) |

Example row: `3001,501,Laptop,Electronics,1,85000.50,2026-11-10,2026-11-10`

### 2.2 Customers data

Header row required. Later files may contain **updated versions of existing customers**
(same `customer_id`, newer `updated_at`) — every version is preserved in history (SCD 2).

| Column | Type in CSV | Target type (silver) | Role |
| --- | --- | --- | --- |
| `customer_id` | integer | INT | **Primary / business key** |
| `customer_name` | string | STRING | Customer name |
| `email` | string | STRING (lower-cased) | Contact email |
| `city` | string | STRING | City |
| `country` | string | STRING | Country |
| `updated_at` | date `yyyy-M-d` | DATE | **CDC sequence column** (source update timestamp, comes from the data file) |

Example row: `502,Priya Singh,priya.singh@example.com,Delhi,India,2026-11-01`

### 2.3 Current sample files

* Sales (volume `demo2.bronze.data_files1`):
  * `sales_2026-11-18.csv` — orders 3001–3006 (3006 has a blank quantity → invalid, kept & flagged)
  * `sales_2026-11-20.csv` — order 3001 **corrected** (newer `last_modified`), plus new orders 3007–3008
* Customers (volume `demo2.bronze.data_files2`):
  * `customers_batch1.csv` — customers 501–505 (501 has padded spaces in the name)
  * `customers_batch2.csv` — customers 502 (moved Delhi → Mumbai) and 504 (London → Manchester)
    **updated**, plus new customer 506

---

## 3. Volume Locations

* Sales CSVs → `/Volumes/demo2/bronze/data_files1/`
* Customers CSVs → `/Volumes/demo2/bronze/data_files2/`

> Note: the requested volume names `demo2.bronze.sales` / `demo2.bronze.customer` cannot be used,
> because Unity Catalog shares one namespace per schema — a volume cannot have the same name as a
> table (`demo2.bronze.sales` is the bronze **table**). The existing volumes `data_files1`
> (sales) and `data_files2` (customers) are used instead, exactly as referenced in the ingestion spec.

---

## 4. Bronze Layer — Ingestion Logic

Each source is a `CREATE OR REFRESH STREAMING TABLE` using Auto Loader (`read_files`):

* CSV files are read incrementally as they land in the volume (`FROM STREAM(read_files(...))`).
* `schemaEvolutionMode => 'addNewColumns'` — new columns in future files are added automatically.
* `rescuedDataColumn => '_rescued_data'` — column/type mismatches and unexpected extra columns are
  captured in `_rescued_data` instead of being dropped; **nothing is silently discarded**.
* A **warn-only** expectation (`order_id IS NOT NULL` / `customer_id IS NOT NULL`) records quality
  metrics without dropping any row.
* Audit columns added to every row:
  * `ingestion_timestamp` — when the pipeline ingested the row
  * `source_file_name` / `source_file_path` — which CSV the row came from

Targets: `demo2.bronze.sales`, `demo2.bronze.customers`.

---

## 5. Silver Layer — Cleaning Logic

Cleaning happens in temporary views (`sales_clean`, `customers_clean`) before CDC:

* **Trim** all string columns (` 3 ` → `3`, `  Ravi Kumar  ` → `Ravi Kumar`).
* **Cast** into correct types (INT, DECIMAL(10,2), DATE); sales get a computed `total_amount`
  (`quantity * unit_price`).
* **Standardize** formats (emails lower-cased).
* **NOT NULL validation** on mandatory columns → boolean `valid_record` flag. Invalid rows are
  **kept and identifiable** (`valid_record = false`), never silently dropped; `_rescued_data`
  is preserved all the way through.
* **Duplicate removal** is handled by AUTO CDC on the business keys (latest sequence wins),
  so there is exactly one current row per business key.

---

## 6. Sales — SCD Type 1 (via AUTO CDC)

```sql
CREATE FLOW sales_cdc AS AUTO CDC INTO demo2.silver.sales
FROM STREAM(sales_clean)
KEYS (order_id)
SEQUENCE BY last_modified
STORED AS SCD TYPE 1;
```

* **One row per `order_id`** — updates **overwrite** the previous value (no history kept).
* Ordering is driven by the source update timestamp `last_modified` — a later file correcting an
  order (e.g. order 3001) simply replaces the old row.
* No manual MERGE statements — CDC is declarative (`AUTO CDC ... STORED AS SCD TYPE 1`).

## 7. Customers — SCD Type 2 (via AUTO CDC)

```sql
CREATE FLOW customers_cdc AS AUTO CDC INTO demo2.silver.customers
FROM STREAM(customers_clean)
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2
TRACK HISTORY ON (customer_name, email, city, country);
```

* **Full history**: every change to a customer creates a **new version row**; the previous version
  is closed by setting `__END_AT` to the `updated_at` of the next version.
* Effective start/end timestamps are the AUTO CDC-managed columns **`__START_AT` / `__END_AT`**:
  * `__END_AT IS NULL` → current version
  * `__START_AT <= t < __END_AT` → the version valid at time `t`
* `TRACK HISTORY ON (customer_name, email, city, country)` — only business-attribute changes create
  new versions (re-sending an identical record does not).
* No manual MERGE statements — declarative AUTO CDC SCD Type 2.

---

## 8. Gold Layer — Business Tables

All gold tables are **materialized views** (batch reads of silver):

| Table | Contents |
| --- | --- |
| `demo2.gold.sales_summary` | Orders, distinct customers, total revenue and average order value **per product category** (valid rows only) |
| `demo2.gold.daily_sales` | Orders, customers, revenue, average order value **per order date** (valid rows only) |
| `demo2.gold.customer_summary` | **Per country**: current customers (SCD2 rows with `__END_AT IS NULL`), distinct customers ever tracked, and total historical versions |
| `demo2.gold.customer_history` | The complete SCD Type 2 audit trail: every customer version with `valid_from` (`__START_AT`), `valid_to` (`__END_AT`) and `is_current` flag |

The customer tables correctly handle the SCD Type 2 history: "current" counts only use open
validity windows, while `customer_history` exposes the full versioned timeline.

---

## 9. Pipeline Configuration

* **Name:** `demo-2-etl-pipeline`
* **Type:** Lakeflow Spark Declarative Pipeline (SDP)
* **Source directory:** `/Workspace/Users/custom001x@gmail.com/projects/demo-2/pipeline/transformation/` (all six SQL files, recursive glob)
* **Catalog:** `demo2` · **Default schema:** `silver` (every dataset is fully qualified, so tables land in bronze/silver/gold schemas explicitly)
* **Mode:** Triggered (runs on demand and shuts down after each update) · **Development mode:** enabled
* **Compute:** Serverless
* **Edition:** Advanced (required for AUTO CDC + expectations)
* Dependencies Bronze → Silver → Gold are inferred automatically from table references between the SQL files.

---

## 10. Adding New CSV Files

1. Drop new CSV files into the matching volume:
   * Sales → `/Volumes/demo2/bronze/data_files1/` (schema from § 2.1)
   * Customers → `/Volumes/demo2/bronze/data_files2/` (schema from § 2.2)
2. Keep the exact header row and column order. Date format `yyyy-M-d` (e.g. `2026-11-20`).
3. To **update** an existing record, repeat its key with a newer `last_modified` (sales) or
   `updated_at` (customers) — the pipeline applies it as a CDC change.
4. Re-run the pipeline update (see § 11). New columns are picked up automatically by schema
   evolution; unexpected/extra column data lands in `_rescued_data`.

## 11. Running / Refreshing the Pipeline

1. Open the pipeline **`demo-2-etl-pipeline`** in the Databricks UI (Pipelines / Lakeflow).
2. Click **Start** (Triggered mode → the update runs once, processes everything, then stops).
3. Subsequent runs pick up only **new** files and CDC changes (incremental, exactly-once).
4. To re-create a dataset from scratch, select the dataset and use **Full refresh**
   (expensive — normally not needed).
5. Results can be queried directly, e.g.
   `SELECT * FROM demo2.gold.daily_sales ORDER BY order_date;`
   `SELECT * FROM demo2.silver.customers WHERE __END_AT IS NULL; -- current customer versions`
