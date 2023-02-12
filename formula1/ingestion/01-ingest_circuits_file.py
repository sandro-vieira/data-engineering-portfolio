# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Storage access setup

# COMMAND ----------

# MAGIC %run ../includes/access_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### API Reference
# MAGIC Click here to access an overview of all public <a href="https://spark.apache.org/docs/latest/api/python/reference/index.html" target="_blank" rel="noreferrer noopener">PySpark modules, classes, functions and methods</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the CSV file using the spark dataframe reader

# COMMAND ----------

filename = f"{bronze}/circuits.csv"

# COMMAND ----------

df = spark.read.option("header", True).csv(filename)

# COMMAND ----------

df.show(10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# Imports
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.sql.functions import *

# COMMAND ----------

# Schema
schema_df = StructType(
    fields=[
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True),
    ]
)

df_schema = spark.read.option("header", True).schema(schema_df).csv(filename)

# COMMAND ----------

display(df_schema)

# COMMAND ----------

# Columns select, rename
df_select = df_schema.select(
    col("circuitId").alias("circuit_id"),
    col("circuitRef").alias("circuit_ref"),
    col("name"),
    col("location"),
    col("country"),
    col("lat").alias("latitude"),
    col("lng").alias("longitude"),
    col("alt").alias("altitude"),
)

# COMMAND ----------

# Add timestamp
df_result = df_select.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the PARQUET file using the spark dataframe writer

# COMMAND ----------

writeAt = f"{silver}/circuits"

# COMMAND ----------

df_result.write.mode("overwrite").parquet(writeAt)

# COMMAND ----------

display(spark.read.parquet(writeAt))
