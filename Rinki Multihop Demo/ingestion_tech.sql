-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Ingestion Techniques to Bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###1. CTAS (Create Table As Select)
-- MAGIC - CTAS stands for Create Table As Select.
-- MAGIC - Used to create a new table from an existing query or dataset.
-- MAGIC - Data is loaded into the target table while creating it.
-- MAGIC - Commonly used for **batch ingestion**.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ![](/Workspace/Users/rinkimehra068@gmail.com/Databricks-Demo/Rinki Multihop Demo/batch_ing.JPG)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev.bronze.sales AS
SELECT *,
        _metadata.file_name as _source_name,
        _metadata.file_path as _source_path,
        current_timestamp() as _ingested_at
FROM read_files("/Volumes/dev/bronze/raw/sales/sales.csv");

-- COMMAND ----------

select count(*) from dev.bronze.sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2. COPY INTO
-- MAGIC - SQL command used to load files into Delta tables.
-- MAGIC - Supports formats like CSV, JSON, Parquet and Avro.
-- MAGIC - Tracks previously loaded files.
-- MAGIC - Prevents re-ingestion of already loaded files.
-- MAGIC - Supports incremental batch ingestion.
-- MAGIC - Supports schema evolution with appropriate configuration.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ![](/Workspace/Users/rinkimehra068@gmail.com/Databricks-Demo/Rinki Multihop Demo/incremental.JPG)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev.bronze.sales2;

COPY INTO dev.bronze.sales2
FROM '/Volumes/dev/bronze/raw/sales/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3. Auto Loader
-- MAGIC
-- MAGIC - Databricks feature for incremental file ingestion.
-- MAGIC - Uses cloudFiles format.
-- MAGIC - Supports cloud storage and Unity Catalog Volumes.
-- MAGIC - Automatically detects new files.
-- MAGIC - Supports schema inference and schema evolution.
-- MAGIC - Uses checkpoints to track processed files.
-- MAGIC - Supports scalable file ingestion using Structured Streaming.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ![](/Workspace/Users/rinkimehra068@gmail.com/Databricks-Demo/Rinki Multihop Demo/streaming.JPG)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC
-- MAGIC source_path = "/Volumes/dev/bronze/raw/sales/"
-- MAGIC schema_location = "/Volumes/dev/bronze/raw/sales/_schemas/sales3"
-- MAGIC
-- MAGIC (
-- MAGIC   spark.readStream
-- MAGIC        .format("cloudFiles")
-- MAGIC        .option("cloudFiles.format", "csv")
-- MAGIC        .option("header", "true")
-- MAGIC        .option("cloudFiles.schemaLocation", schema_location)
-- MAGIC        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
-- MAGIC        .load(source_path)
-- MAGIC        .withColumn("_source_name", col("_metadata.file_name"))
-- MAGIC        .withColumn("_source_path", col("_metadata.file_path"))
-- MAGIC        .withColumn("_ingested_at", current_timestamp())
-- MAGIC        .writeStream
-- MAGIC        .option("checkpointLocation", f"{schema_location}/_checkpoint")
-- MAGIC        .trigger(availableNow=True)
-- MAGIC        .toTable("dev.bronze.sales3")
-- MAGIC )

-- COMMAND ----------

select count(*) from dev.bronze.sales3