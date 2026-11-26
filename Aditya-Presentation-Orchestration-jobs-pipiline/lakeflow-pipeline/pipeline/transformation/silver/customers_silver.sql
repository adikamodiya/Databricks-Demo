-- ==============================================================
-- SILVER LAYER - Customers cleaning + AUTO CDC (SCD Type 2)
-- Source : demo2.bronze.customers (streaming table)
-- Target : demo2.silver.customers (streaming table via AUTO CDC)
--
-- Data quality (SDP expectations - ON VIOLATION DROP ROW):
--   * Invalid rows are REMOVED before they reach silver; drop
--     counts appear in the pipeline's Data quality tab / event log
--   * 4 drop-expectations on customers_clean:
--       valid_customer_id, valid_name, valid_country, valid_updated_at
--   * Cleaning: trim strings, lower-case email, cast types
--   * customers_clean is a PRIVATE streaming table: pipeline-scoped only
--     (not published to UC), so the DROP happens before the CDC flow
--
-- AUTO CDC SCD TYPE 2 (full history):
--   * KEYS (customer_id)      - one current row + version history
--   * SEQUENCE BY updated_at - source update timestamp drives order
--   * TRACK HISTORY ON customer_name, email, city, country
--     (only changes to these business attributes create new versions)
--   * Validity windows in __START_AT / __END_AT (__END_AT IS NULL = current)
-- ==============================================================

CREATE OR REFRESH PRIVATE STREAMING TABLE customers_clean (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name        EXPECT (customer_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_country     EXPECT (country IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_updated_at  EXPECT (updated_at IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Customers cleaning + quality gate: invalid rows DROPPED before CDC (metrics in the Data quality tab)'
AS SELECT
  CAST(customer_id AS INT)                                  AS customer_id,
  TRIM(customer_name)                                       AS customer_name,
  LOWER(TRIM(email))                                        AS email,
  TRIM(city)                                                AS city,
  TRIM(country)                                             AS country,
  TO_DATE(TRIM(CAST(updated_at AS STRING)), 'yyyy-M-d')    AS updated_at,
  _rescued_data,
  ingestion_timestamp,
  source_file_name,
  source_file_path
FROM STREAM(demo2.bronze.customers);

-- Target streaming table (populated by the AUTO CDC flow below)
CREATE OR REFRESH STREAMING TABLE demo2.silver.customers
COMMENT 'Silver: cleaned customers, AUTO CDC SCD Type 2 by customer_id, sequenced by updated_at, history tracked on business attributes (invalid rows dropped upstream in private customers_clean)';

-- CDC flow: every attribute change creates a new version row
CREATE FLOW customers_cdc AS AUTO CDC INTO demo2.silver.customers
FROM STREAM(customers_clean)
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2
TRACK HISTORY ON customer_name, email, city, country;
