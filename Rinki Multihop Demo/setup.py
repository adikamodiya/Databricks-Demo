# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists dev;
# MAGIC
# MAGIC create schema if not exists dev.bronze
# MAGIC comment 'store raw data tables with historical truth';
# MAGIC
# MAGIC create schema if not exists dev.silver
# MAGIC comment 'store clean, transform data tables';
# MAGIC
# MAGIC create schema if not exists dev.bronze
# MAGIC comment 'store kpis, business level data';

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists dev.bronze.raw;