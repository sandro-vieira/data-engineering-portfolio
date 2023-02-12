# Databricks notebook source
# MAGIC %md
# MAGIC ##### Storage access setup

# COMMAND ----------

# MAGIC %run ../includes/access_setup

# COMMAND ----------

races_results_df = spark.read.parquet(f"{gold}/races_results")

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

# COMMAND ----------

drivers_standings_df = races_results_df.groupBy(
    "race_year", "driver_name", "driver_nationality", "team"
).agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

display(drivers_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

drivers_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = drivers_standings_df.withColumn("rank", rank().over(drivers_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{gold}/drivers_standings")
