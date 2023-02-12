-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Storage access setup

-- COMMAND ----------

-- MAGIC %run ../includes/access_setup

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuit table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.circuits;

CREATE TABLE f1_bronze.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ddl_drop = "DROP TABLE IF EXISTS f1_bronze.circuits"
-- MAGIC 
-- MAGIC spark.sql(ddl_drop)
-- MAGIC 
-- MAGIC ddl_create = '''
-- MAGIC CREATE TABLE f1_bronze.circuits (
-- MAGIC   circuitId INT,
-- MAGIC   circuitRef STRING,
-- MAGIC   name STRING,
-- MAGIC   location STRING,
-- MAGIC   country STRING,
-- MAGIC   lat DOUBLE,
-- MAGIC   lng DOUBLE,
-- MAGIC   alt INT,
-- MAGIC   url STRING
-- MAGIC )
-- MAGIC USING csv
-- MAGIC OPTIONS (path "{0}/circuits.csv", header true)'''.format(bronze)
-- MAGIC 
-- MAGIC spark.sql(ddl_create)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.races;

CREATE TABLE f1_bronze.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_bronze.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructors table
-- MAGIC - Single line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.constructors;

CREATE TABLE f1_bronze.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC - Single line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.drivers;

CREATE TABLE f1_bronze.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table
-- MAGIC - Sigle line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.results;

CREATE TABLE f1_bronze.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json
OPTIONS (path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC - Multi line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.pit_stops;

CREATE TABLE f1_bronze.pit_stops (
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING json
OPTIONS (path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times table
-- MAGIC - CSV file
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.lap_times;

CREATE TABLE f1_bronze.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS (path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying table
-- MAGIC - JSON file
-- MAGIC - Multiline JSON
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.qualifying;

CREATE TABLE f1_bronze.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS (path "abfss://files@dlavanadeanalyticslab.dfs.core.windows.net/raw/qualifying", multiLine true)

-- COMMAND ----------

DESCRIBE EXTENDED f1_bronze.qualifying
