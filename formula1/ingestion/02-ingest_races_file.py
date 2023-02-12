# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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

filename = f"{bronze}/races.csv"

# COMMAND ----------

df = spark.read.option("header", True).csv(filename)

# COMMAND ----------

# Imports
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType
)
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

# Schema
schema_df = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

df_schema = spark.read.option("header", True).schema(schema_df).csv(filename)

# COMMAND ----------

# Columns select, timestamp, drop
df_result = (
    df_schema.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("year", "race_year")
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumn(
        "race_timestamp",
        to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"),
    )
    .withColumn("ingestion_timestamp", current_timestamp())
    .drop("date", "time", "url")
)

# COMMAND ----------

display(df_result.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the PARQUET file using the spark dataframe writer

# COMMAND ----------

writeAt = f"{silver}/races"

# COMMAND ----------

df_result.write.mode("overwrite").partitionBy("race_year").parquet(writeAt)

# COMMAND ----------

display(dbutils.fs.ls(writeAt))

# COMMAND ----------

display(spark.read.parquet(saveAt).head(100))
