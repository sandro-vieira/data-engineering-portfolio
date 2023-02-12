# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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
# MAGIC ##### Read the JSON file using the spark dataframe reader

# COMMAND ----------

filename = f"{bronze}/drivers.json"

# COMMAND ----------

# Imports
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType
)

# COMMAND ----------

# Nested Schema
name_schema = StructType(
    fields=[
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ]
)

# COMMAND ----------

# Schema
file_schema = StructType(
    fields=[
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

schema_df = spark.read.schema(file_schema).json(filename)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

# Columns select, rename
select_df = (
    schema_df.withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("driverRef", "driver_ref")
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
)

# COMMAND ----------

# Drop
result_df = add_ingestion_timestamp(select_df).drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the PARQUET file using the spark dataframe writer

# COMMAND ----------

writeAt = f"{silver}/drivers"

# COMMAND ----------

result_df.write.mode("overwrite").parquet(writeAt)
