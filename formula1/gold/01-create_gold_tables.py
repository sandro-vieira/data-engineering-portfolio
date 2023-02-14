# Databricks notebook source
# MAGIC %md
# MAGIC ##### Storage access setup

# COMMAND ----------

# MAGIC %run ../includes/access_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Managed tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_gold;

# COMMAND ----------

rr_df = spark.read.parquet(f"{gold}/races_results")
rr_df.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.races_results")

# COMMAND ----------

ds_df = spark.read.parquet(f"{gold}/drivers_standings");
ds_df.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.drivers_standings");

# COMMAND ----------

cs_df = spark.read.parquet(f"{gold}/constructors_standings");
cs_df.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.constructors_standings");

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_gold.races_results LIMIT 10;
# MAGIC --SELECT * FROM f1_gold.drivers_standings LIMIT 10;
# MAGIC --SELECT * FROM f1_gold.constructors_standings LIMIT 10;

# COMMAND ----------


