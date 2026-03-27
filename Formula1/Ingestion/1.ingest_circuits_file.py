# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits JSON File from Jolpica-F1 API (https://api.jolpi.ca/ergast/f1/circuits/)

# COMMAND ----------

# dbutils.widgets.text("p_data_source", "")
# v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV File using Spark API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Jolpica-F1 API returns coordinates as strings and nests location inside a 'Location' object.
# circuitId is now a string slug (e.g. "adelaide"), not an integer.
# Fields removed vs legacy Ergast CSV: circuitRef, alt (altitude).
# Field renamed: name -> circuitName, lng -> long (inside Location)
location_schema = StructType(fields=[
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("country", StringType(), True)
])

circuits_schema = StructType(fields=[
    StructField("circuitId", StringType(), False),    # STRING slug, was IntegerType
    StructField("circuitName", StringType(), True),   # renamed from 'name'
    StructField("url", StringType(), True),
    StructField("Location", location_schema)          # nested object from Jolpica
])

# COMMAND ----------

# Jolpica data is stored as JSON in ADLS (fetched by raw/0.fetch_from_jolpica.py)
circuits_df = spark.read \
    .schema(circuits_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/circuits")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

# Extract nested Location fields; cast coordinates from String to Double.
# circuitRef and alt are no longer available in the Jolpica API.
circuits_selected_df = circuits_df.select(
    col("circuitId").alias("circuit_id"),
    col("circuitName").alias("name"),
    col("Location.locality").alias("location"),
    col("Location.country").alias("country"),
    col("Location.lat").cast(DoubleType()).alias("latitude"),
    col("Location.long").cast(DoubleType()).alias("longitude")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.1 - Select the Required Columns

# COMMAND ----------

# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# Column aliasing is now handled in the select() above.
# Only add the audit file_date column here.
circuits_renamed_df = circuits_selected_df.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 - Add Ingestion Date to the Dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4  - Write data to  the Data Lake as Delta

# COMMAND ----------

# # Drop the table if it exists
# spark.sql("DROP TABLE IF EXISTS f1_processed.circuits")

# # Remove the existing location
# dbutils.fs.rm("abfss://processed@formula001adls.dfs.core.windows.net/circuits", recurse=True)

# Write the DataFrame to the table again
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# overwrite_partition(circuits_final_df, 'f1_processed', 'circuits', 'race_id')

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_processed.circuits
# WHERE name = 'Albert Park Grand Prix Circuit'

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM f1_processed.circuits
# MAGIC
