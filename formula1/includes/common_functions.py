# Databricks notebook source
# MAGIC %md
# MAGIC ### PEP 8 – Style Guide for Python Code
# MAGIC 
# MAGIC Este <a href="https://peps.python.org/pep-0008/" target="_blank" rel="noreferrer noopener">documento</a> fornece convenções de codificação para o código Python.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_timestamp(input_df):
    output_df = input_df.withColumn("ingestion_timestamp", current_timestamp())
    return output_df
