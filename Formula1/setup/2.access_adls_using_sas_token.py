# DEPRECATED: Use Unity Catalog External Locations for Serverless compatibility.
# Databricks notebook source
# MAGIC %md
# MAGIC # Access Aure Data Lake using SAS Token

# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-1-raw-sas-token')


# COMMAND ----------

storage_account_name = "formula001adls"  # Replace with your actual storage account name


