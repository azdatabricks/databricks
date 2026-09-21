-- Databricks notebook source
-- MAGIC %md
-- MAGIC Disable delta lake autooptimize feature to understand background of delta lake

-- COMMAND ----------

SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false;

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

CREATE External TABLE az_dev.bronze.emp(
  id INT,
  name STRING,
  age INT,
  active BOOLEAN
) 
USING DELTA
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp'

-- COMMAND ----------

desc formatted az_dev.bronze.emp

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

ALTER TABLE az_dev.bronze.emp ALTER COLUMN id SET NOT NULL;
ALTER TABLE az_dev.bronze.emp ADD CONSTRAINT age_check CHECK (age >= 18 and age<100)

-- COMMAND ----------

DESCRIBE az_dev.bronze.emp

-- COMMAND ----------

DESCRIBE EXTENDED az_dev.bronze.emp

-- COMMAND ----------

DESCRIBE DETAIL az_dev.bronze.emp

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000002.json`

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000002.crc`

-- COMMAND ----------

DESCRIBE HISTORY az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delta Lake is efficient in handling data using `statastics`
-- MAGIC
-- MAGIC Lets Userstand how it will maintains stats when we insert 1 record vs multiple records

-- COMMAND ----------

-- DBTITLE 1,Inserting single record
INSERT INTO az_dev.bronze.emp values
(1,'rama',32,True)

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

select * from  json.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000003.json`

-- COMMAND ----------

INSERT INTO az_dev.bronze.emp values
(2,'krishna',16,True)

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000003.json`

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000003.crc`

-- COMMAND ----------

DESCRIBE HISTORY az_dev.bronze.emp

-- COMMAND ----------

-- DBTITLE 1,Inserting Multiple Records
INSERT INTO az_dev.bronze.emp values
(2,'krishna',22,True),
(3,'sai',33,True),
(4,'srinu',44,False),
(5,'venkat',55,True)

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000004.json`

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000004.crc`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Update record in emp table

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp where id in (1,2)

-- COMMAND ----------

UPDATE az_dev.bronze.emp
SET name='NewVenkat'
WHERE id=5

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000005.json`

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000005.crc`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delta Lake is smartly read/scan only relavant files which has latest update data 
-- MAGIC and skip irrelevant file scan
-- MAGIC
-- MAGIC We can also see number of files read through SPARK UI

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp
where id=1

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp WHERE id in (2,3,4,5)

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp WHERE id in (1,2)

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp WHERE id=10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delete record in emp table

-- COMMAND ----------

DELETE FROM az_dev.bronze.emp
where name='NewVenkat'

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000006.json`

-- COMMAND ----------

DESCRIBE HISTORY az_dev.bronze.emp

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Merge Operation on emp table

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp

-- COMMAND ----------

create table az_dev.bronze.emp_updates(id INT,name STRING,age INT,active BOOLEAN);
INSERT INTO az_dev.bronze.emp_updates values(3,'sai',33,false),
(5,'sesi',29,true)

-- COMMAND ----------

select * from az_dev.bronze.emp_updates

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

MERGE INTO az_dev.bronze.emp
USING az_dev.bronze.emp_updates
ON emp.id=emp_updates.id

WHEN MATCHED THEN 
UPDATE SET name=emp_updates.name,age=emp_updates.age,active=emp_updates.active

WHEN NOT MATCHED THEN
INSERT(id,name,age,active) VALUES(emp_updates.id,emp_updates.name,emp_updates.age,emp_updates.active);

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp

-- COMMAND ----------

DESC HISTORY az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000007.json`

-- COMMAND ----------

desc history az_dev.bronze.emp

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

select * from az_dev.bronze.emp where id in (3,5)

-- COMMAND ----------

select * from az_dev.bronze.emp where id in (2,4)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checkpointing

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC n=1
-- MAGIC for x in range(n):
-- MAGIC   spark.sql(f"INSERT INTO az_dev.bronze.emp values({x}+100,'test',{x}+18,True)")
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.parquet("dbfs:abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000010.checkpoint.parquet"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Summary

-- COMMAND ----------

-- Add Constraints
ALTER TABLE <table> ALTER COLUMN id SET NOT NULL;
ALTER TABLE <table> ADD CONSTRAINT <c_name> CHECK (<condition>)

-- Update Records in Delta Table
UPDATE <table>
SET <column>=<value>
WHERE <condition>

-- Delete Records in Delta Table
DELETE FROM <table>
WHERE <condition>

-- Merge records in Delta Table
MERGE INTO <target>
USING <updates>
ON <target>.<key>= <updates> <key> 
WHEN MATCHED
UPDATE SET=<action>
WHEN NOT MATCHED
INSERT Values=<action>