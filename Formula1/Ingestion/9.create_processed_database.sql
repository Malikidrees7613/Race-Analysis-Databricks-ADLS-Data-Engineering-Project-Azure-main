-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@formula1datalakeboss.dfs.core.windows.net/"

-- COMMAND ----------

describe database f1_processed
