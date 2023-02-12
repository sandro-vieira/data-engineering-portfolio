# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Storage access setup

# COMMAND ----------

# MAGIC %run ../includes/access_setup

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### API Reference
# MAGIC Click here to access an overview of all public <a href="https://spark.apache.org/docs/latest/api/python/reference/index.html" target="_blank" rel="noreferrer noopener">PySpark modules, classes, functions and methods</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the CSV file using the spark dataframe reader

# COMMAND ----------

filename = f"{bronze}/lap_times"

# COMMAND ----------

# Imports
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# COMMAND ----------

# Schema
file_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("lap", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True)
    ]
)

# COMMAND ----------

schema_df = spark.read.schema(file_schema).csv(filename)

# COMMAND ----------

# Columns select, rename
select_df = (
    schema_df.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
)

# COMMAND ----------

result_df = add_ingestion_timestamp(select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the PARQUET file using the spark dataframe writer

# COMMAND ----------

writeAt = f"{silver}/lap_times"

# COMMAND ----------

result_df.write.mode("overwrite").parquet(writeAt)
