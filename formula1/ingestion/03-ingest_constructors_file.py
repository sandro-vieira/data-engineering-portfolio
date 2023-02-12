# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

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
# MAGIC ##### Read the JSON file using the spark dataframe reader

# COMMAND ----------

filename = f"{bronze}/constructors.json"

# COMMAND ----------

# Schema
file_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

schema_df = spark.read.schema(file_schema).json(filename)

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# Drop column
select_df = schema_df.drop(f.col('url'))

# COMMAND ----------

# Rename and add timestamp
result_df = (
    select_df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumn("ingestion_timestamp", f.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the PARQUET file using the spark dataframe writer

# COMMAND ----------

writeAt = f'{silver}/constructors'

# COMMAND ----------

result_df.write.mode('overwrite').parquet(writeAt)

# COMMAND ----------

display(spark.read.parquet(writeAt))
