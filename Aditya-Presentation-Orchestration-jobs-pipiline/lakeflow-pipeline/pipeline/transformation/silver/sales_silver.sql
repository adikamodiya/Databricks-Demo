-- ==============================================================
-- SILVER LAYER - Sales cleaning + AUTO CDC (SCD Type 1)
-- Source : demo2.bronze.sales (streaming table)
-- Target : demo2.silver.sales (streaming table via AUTO CDC)
--
-- Data quality (SDP expectations - ON VIOLATION DROP ROW):
--   * Invalid rows are REMOVED before they reach silver; drop
--     counts appear in the pipeline's Data quality tab / event log
--   * 7 drop-expectations on sales_clean:
--       valid_order_id, valid_customer_id, valid_quantity (> 0),
--       valid_unit_price (> 0), valid_order_date,
--       valid_last_modified, valid_category
--   * Cleaning: trim strings, cast to proper types,
--     computed total_amount = quantity * unit_price
--   * e.g. order 3006 (blank quantity) is dropped here, not in bronze
--   * sales_clean is a PRIVATE streaming table: pipeline-scoped only
--     (not published to UC), so the DROP happens before the CDC flow
--
-- AUTO CDC SCD TYPE 1:
--   * KEYS (order_id)        - one row per order, updates overwrite
--   * SEQUENCE BY last_modified - newer source versions win
-- ==============================================================

CREATE OR REFRESH PRIVATE STREAMING TABLE sales_clean (
  CONSTRAINT valid_order_id      EXPECT (order_id IS NOT NULL)      ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id   EXPECT (customer_id IS NOT NULL)   ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity      EXPECT (quantity > 0)              ON VIOLATION DROP ROW,
  CONSTRAINT valid_unit_price    EXPECT (unit_price > 0)            ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_date    EXPECT (order_date IS NOT NULL)   ON VIOLATION DROP ROW,
  CONSTRAINT valid_last_modified EXPECT (last_modified IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_category      EXPECT (category IS NOT NULL)     ON VIOLATION DROP ROW
)
COMMENT 'Sales cleaning + quality gate: invalid rows DROPPED before CDC (metrics in the Data quality tab)'
AS SELECT
  CAST(order_id    AS INT)                                    AS order_id,
  CAST(customer_id AS INT)                                    AS customer_id,
  TRIM(product)                                               AS product,
  TRIM(category)                                              AS category,
  CAST(quantity    AS INT)                                    AS quantity,
  CAST(unit_price  AS DECIMAL(10, 2))                         AS unit_price,
  CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10, 2))  AS total_amount,
  TO_DATE(TRIM(CAST(order_date    AS STRING)), 'yyyy-M-d')    AS order_date,
  TO_DATE(TRIM(CAST(last_modified AS STRING)), 'yyyy-M-d')    AS last_modified,
  _rescued_data,
  ingestion_timestamp,
  source_file_name,
  source_file_path
FROM STREAM(demo2.bronze.sales);

-- Target streaming table (populated by the AUTO CDC flow below)
CREATE OR REFRESH STREAMING TABLE demo2.silver.sales
COMMENT 'Silver: cleaned sales, AUTO CDC SCD Type 1 by order_id, sequenced by last_modified (invalid rows dropped upstream in private sales_clean)';

-- CDC flow: latest version of each order wins
CREATE FLOW sales_cdc AS AUTO CDC INTO demo2.silver.sales
FROM STREAM(sales_clean)
KEYS (order_id)
SEQUENCE BY last_modified
STORED AS SCD TYPE 1;
