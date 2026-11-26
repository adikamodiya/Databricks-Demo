-- ==============================================================
-- GOLD LAYER - Customers business tables (materialized views)
-- Source : demo2.silver.customers (AUTO CDC SCD Type 2 - full history)
-- Targets: demo2.gold.customer_summary, demo2.gold.customer_history
--
-- SCD Type 2 semantics:
--   * __END_AT IS NULL            -> current version of a customer
--   * __START_AT <= t < __END_AT  -> version valid at time t
--   * every attribute change (customer_name, email, city, country)
--     creates a new version row with its own validity window
-- ==============================================================

-- Current vs historical customers per country
CREATE OR REFRESH MATERIALIZED VIEW demo2.gold.customer_summary
COMMENT 'Gold: per country - current customers (open SCD2 windows), distinct customers ever tracked, and total historical versions'
AS SELECT
  country,
  COUNT(DISTINCT CASE WHEN __END_AT IS NULL THEN customer_id END) AS current_customers,
  COUNT(DISTINCT customer_id)                                     AS distinct_customers_ever,
  COUNT(*)                                                        AS total_versions
FROM demo2.silver.customers
GROUP BY country
ORDER BY current_customers DESC;

-- Full SCD Type 2 audit trail
CREATE OR REFRESH MATERIALIZED VIEW demo2.gold.customer_history
COMMENT 'Gold: complete customer audit trail - every version with validity window (__START_AT/__END_AT) and is_current flag'
AS SELECT
  customer_id,
  customer_name,
  email,
  city,
  country,
  updated_at              AS last_updated,
  __START_AT              AS valid_from,
  __END_AT                AS valid_to,
  (__END_AT IS NULL)       AS is_current
FROM demo2.silver.customers
ORDER BY customer_id, __START_AT;
