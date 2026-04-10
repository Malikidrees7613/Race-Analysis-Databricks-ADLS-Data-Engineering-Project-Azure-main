-- Databricks notebook source
-- Unity Catalog: Create managed schema using 3-level namespace
CREATE SCHEMA IF NOT EXISTS formula1_catalog.f1_processed
MANAGED LOCATION 'abfss://processed@formula1datalakeboss.dfs.core.windows.net/';

-- COMMAND ----------

DESCRIBE SCHEMA formula1_catalog.f1_processed
