# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC #Medallion Architecture
# MAGIC
# MAGIC Medallion Architecture is a data design pattern used in Databricks Lakehouse to organize data into three layers: **Bronze, Silver and Gold.**
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![](/Workspace/Users/rinkimehra068@gmail.com/Multihop Demo/Multihop.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Bronze Layer – Raw Data
# MAGIC - Stores data directly from external source systems.
# MAGIC - Data is stored in its original structure.
# MAGIC - Basic metadata is added, such as ingestion timestamp and source file name, path.
# MAGIC - Maintains historical data.
# MAGIC - Supports auditing, data lineage and reprocessing.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

#ingestion to bronze using CTAS with metadata columns
spark.sql(f"""
create table if not exists {catalog}.bronze.sales as
select *,
        _metadata.file_name as _source_name,
        _metadata.file_path as _source_path,
        current_timestamp() as _ingested_at
from read_files("/Volumes/{catalog}/bronze/raw/sales/sales.csv")
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.bronze.sales limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Silver Layer – Cleaned and Refined Data
# MAGIC - Cleans and validates Bronze data.
# MAGIC - Removes duplicate and invalid records.
# MAGIC - Handles missing and incorrect values.
# MAGIC - Combines and matches data from multiple sources.
# MAGIC - Provides a consistent and reliable view of business entities.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as F

bronze_sales = spark.read.table(f"{catalog}.bronze.sales")

#Handling Duplicates
w = Window.partitionBy("order_id").orderBy(F.col("order_date").desc())
df_ranked = bronze_sales.withColumn("_rn", F.row_number().over(w))

#Handling triming, casing and type casting
tagged = (
    df_ranked
    .withColumn("order_id", trim(upper(col("order_id"))))
    .withColumn("customer_id", trim(upper(col("customer_id"))))
    .withColumn("transaction_id", trim(upper(col("transaction_id"))))
    .withColumn("product_id", trim(upper(col("product_id"))))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("discount_amount", col("discount_amount").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
    .withColumn("order_date", to_date(col("order_date")))
)

#Handle nulls and negative quantity, discount amount and total_amount
# (rows with nulls in these fields are routed to quarantine instead of silently dropped)
# Nulls in key identifier columns also make a record invalid for silver
is_invalid = (
    F.col("order_id").isNull()
    | F.col("customer_id").isNull()
    | F.col("transaction_id").isNull()
    | F.col("product_id").isNull()
    | F.col("quantity").isNull()
    | F.col("discount_amount").isNull()
    | F.col("total_amount").isNull()
    | (F.col("quantity") < 0)
    | (F.col("discount_amount") < 0)
    | (F.col("total_amount") < 0)
)

#create clean_df and quarantine_df from the LATEST record per order_id only (_rn == 1)
# older duplicate rows (_rn > 1) are also routed to quarantine instead of being silently dropped
duplicates_df = tagged.filter(F.col("_rn") > 1).drop("_rn")
latest_df = tagged.filter(F.col("_rn") == 1).drop("_rn")
clean_df = latest_df.filter(~is_invalid)
quarantine_df = latest_df.filter(is_invalid).unionByName(duplicates_df)


# 5. Write silver tables (full refresh of the batch)
clean_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.sales_clean")

quarantine_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.sales_quarantine")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dev.silver.sales_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dev.silver.sales_quarantine

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Gold Layer – Business-Ready Data
# MAGIC - Contains business-level transformations.
# MAGIC - Applies final business rules and data quality checks.
# MAGIC - Creates aggregated and optimized datasets.
# MAGIC - Supports dashboards, reporting, analytics and machine learning.

# COMMAND ----------

# 1. Total sales across all orders
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.gold.total_sales AS
SELECT sum(total_amount) AS total_sales
FROM {catalog}.silver.sales_clean
""")

display(spark.table(f"{catalog}.gold.total_sales"))

# COMMAND ----------

# 2. Total number of orders
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.gold.total_orders AS
SELECT count(DISTINCT order_id) AS total_orders
FROM {catalog}.silver.sales_clean
""")

display(spark.table(f"{catalog}.gold.total_orders"))

# COMMAND ----------

#3. Total sales for each customer
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.gold.sales_by_customer AS
SELECT customer_id, sum(total_amount) AS total_sales
FROM {catalog}.silver.sales_clean
GROUP BY customer_id
""")

display(spark.table(f"{catalog}.gold.sales_by_customer"))