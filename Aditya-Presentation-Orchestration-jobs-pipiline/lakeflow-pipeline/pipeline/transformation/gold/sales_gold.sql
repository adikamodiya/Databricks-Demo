-- ==============================================================
-- GOLD LAYER - Sales business tables (materialized views)
-- Source : demo2.silver.sales (AUTO CDC SCD Type 1 - one row per order)
-- Targets: demo2.gold.sales_summary, demo2.gold.daily_sales
--
-- Batch reads (no STREAM) of silver - the standard gold
-- aggregation pattern. Silver already drops invalid rows
-- (ON VIOLATION DROP ROW in sales_clean), so gold needs NO
-- quality filters - every row here is already validated.
-- ==============================================================

-- KPIs per product category
CREATE OR REFRESH MATERIALIZED VIEW demo2.gold.sales_summary
COMMENT 'Gold: orders, distinct customers, units sold, revenue and average order value per category (from validated silver sales)'
AS SELECT
  category,
  COUNT(DISTINCT order_id)    AS total_orders,
  COUNT(DISTINCT customer_id) AS distinct_customers,
  SUM(quantity)               AS total_units_sold,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM demo2.silver.sales
GROUP BY category
ORDER BY total_revenue DESC;

-- KPIs per order date
CREATE OR REFRESH MATERIALIZED VIEW demo2.gold.daily_sales
COMMENT 'Gold: orders, distinct customers, revenue and average order value per order date (from validated silver sales)'
AS SELECT
  order_date,
  COUNT(DISTINCT order_id)    AS total_orders,
  COUNT(DISTINCT customer_id) AS distinct_customers,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM demo2.silver.sales
GROUP BY order_date
ORDER BY order_date;
