-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC fileroot = "/FileStore/tables/clinicaltrial_2021"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import os
-- MAGIC os.environ['fileroot'] = fileroot

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC from pyspark.sql.types import *
-- MAGIC                                     
-- MAGIC clinicaltrailDF = spark.read.options(delimiter = "|", header = True ).csv(fileroot)
-- MAGIC                                     
-- MAGIC clinicaltrailDF.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC clinicaltrailDF.createOrReplaceTempView("clinicaltrail_sql")

-- COMMAND ----------

SELECT * FROM clinicaltrail_sql LIMIT 10

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TABLE default.clinicaltrial_2021 AS SELECT * FROM clinicaltrail_sql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM clinicaltrial_2021 LIMIT 10

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS clinicaltrialDB

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

CREATE OR REPLACE TABLE clinicaltrialDB.clinicaltrial_2021 AS SELECT * FROM default.clinicaltrial_2021

-- COMMAND ----------

SHOW TABLES IN clinicaltrialDB

-- COMMAND ----------

SELECT * FROM clinicaltrialDB.clinicaltrial_2021 LIMIT 10

-- COMMAND ----------

SELECT DISTINCT COUNT(*) FROM clinicaltrialDB.clinicaltrial_2021

-- COMMAND ----------

SELECT Type, COUNT(*) AS CountType 
FROM clinicaltrialDB.clinicaltrail_2021 
GROUP BY Type
ORDER BY CountType DESC
LIMIT 5

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW split_conditions
AS 
SELECT split(Conditions, ',') AS split_condition
FROM clinicaltrialDB.clinicaltrial_2021;



-- COMMAND ----------

SELECT * FROM split_conditions LIMIT 10

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW exploded_conditions AS SELECT explode(split_condition) AS single_condition FROM split_conditions;

-- COMMAND ----------

SELECT * FROM exploded_conditions LIMIT 10

-- COMMAND ----------

SELECT single_condition, COUNT(*) AS frequency
FROM exploded_conditions
WHERE single_condition IS NOT NULL
GROUP BY single_condition
ORDER BY frequency DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC fileroot1 = "/FileStore/tables/pharma"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import os
-- MAGIC os.environ['fileroot'] = fileroot1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.types import *
-- MAGIC                                     
-- MAGIC pharmaDF = spark.read.options(delimiter = ",", header = True ).csv(fileroot1)
-- MAGIC                                     
-- MAGIC pharmaDF.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC pharmaDF.createOrReplaceTempView("pharma_sql")

-- COMMAND ----------

SELECT * FROM pharma_sql LIMIT 10

-- COMMAND ----------

CREATE OR REPLACE TABLE clinicaltrialDB.pharma AS SELECT * FROM pharma_sql

-- COMMAND ----------

WITH nonpharmaspons AS (
SELECT c.Sponsor
FROM clinicaltrialDB.clinicaltrail_2021 AS c
LEFT OUTER JOIN clinicaltrialDB.pharma AS p
ON c.Sponsor = p.Parent_Company )
 
 
SELECT Sponsor, COUNT(*) AS frequency
FROM nonpharmaspons
GROUP BY Sponsor
ORDER BY frequency DESC

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS Count
FROM clinicaltrialDB.clinicaltrail_2021 AS c
WHERE Sponsor NOT IN (SELECT Parent_Company FROM clinicaltrialDB.pharma)
GROUP BY Sponsor
ORDER BY Count DESC
LIMIT 10

-- COMMAND ----------

SELECT SUBSTRING_INDEX(Completion, ' ', 1) AS Month, COUNT(*) AS studies_completed
FROM clinicaltrialDB.clinicaltrail_2021
WHERE Completion LIKE '%2021%' AND Status = 'Completed'
GROUP BY Month
ORDER BY studies_completed DESC

-- COMMAND ----------


