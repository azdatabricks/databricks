-- Databricks notebook source
-- MAGIC %md
-- MAGIC Disable delta lake autooptimize feature to understand background of delta lake

-- COMMAND ----------

SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false;

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

CREATE TABLE az_dev.bronze.emp(
  id INT,
  name STRING,
  age INT,
  active BOOLEAN
) 
USING DELTA
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp'
TBLPROPERTIES('delta.enableDeletionVectors'=true)

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

select * from json.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000003.json`

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

desc history az_dev.bronze.emp

-- COMMAND ----------

UPDATE az_dev.bronze.emp
SET name='NewVenkat'
WHERE id=5

-- COMMAND ----------

desc history az_dev.bronze.emp

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000006.json`

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000006.crc`

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

SELECT * FROM az_dev.bronze.emp WHERE age=55

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delete record in emp table

-- COMMAND ----------

DELETE FROM az_dev.bronze.emp
where name='NewVenkat'

-- COMMAND ----------

desc history az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000008.json`

-- COMMAND ----------

DESCRIBE HISTORY az_dev.bronze.emp

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

desc history az_dev.bronze.emp

-- COMMAND ----------

SELECT * FROM az_dev.bronze.emp

-- COMMAND ----------

DESC HISTORY az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/00000000000000000010.json`

-- COMMAND ----------

desc history az_dev.bronze.emp

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

select * from az_dev.bronze.emp where id in (3,5)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checkpointing

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC n=50
-- MAGIC for x in range(n):
-- MAGIC   spark.sql(f"INSERT INTO az_dev.bronze.emp values({x}+100,'test',{x}+18,True)")
-- MAGIC

-- COMMAND ----------

desc history az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

select * from az_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Deletion Vectors
-- MAGIC Delete,Update and Merge Operations use deletion vectors to mark existing rows as removed or changed without rewriting the parquet **file**

-- COMMAND ----------

SET spark.databricks.delta.optimizeWrite.enabled=FALSE;
SET spark.databricks.delta.autoCompact.enabled=FALSE;

-- COMMAND ----------

create database deltadb

-- COMMAND ----------

use database deltadb

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Usecase1

-- COMMAND ----------

drop table if exists cust

-- COMMAND ----------

DROP TABLE IF EXISTS cust;
CREATE OR REPLACE TABLE cust(id INT,
                            cust_name STRING,
                            cust_location STRING,
                            is_actibe BOOLEAN)
USING DELTA 
LOCATION '/mnt/global/india/silver/cust'
TBLPROPERTIES('delta.enableDeletionVectors'=true)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inserting Records into cust table

-- COMMAND ----------

INSERT INTO cust VALUES
  (1,'rama','bangalore',True)

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/cust

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/cust/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`/mnt/global/india/silver/cust/_delta_log/00000000000000000001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delete Record from cust table

-- COMMAND ----------

DELETE FROM cust
WHERE id=1

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/cust

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/cust/_delta_log

-- COMMAND ----------

SELECT * FROM JSON.`/mnt/global/india/silver/cust/_delta_log/00000000000000000002.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #usecase2

-- COMMAND ----------

DROP TABLE IF EXISTS product

-- COMMAND ----------

CREATE OR REPLACE TABLE product(id INT,
                                name STRING,
                                man_year INT,
                                is_expired BOOLEAN)
USING DELTA
LOCATION '/mnt/global/india/silver/product'
TBLPROPERTIES('delta.enableDeletionVectors'=true)

-- COMMAND ----------

INSERT INTO product (id, name, man_year, is_expired)
VALUES
    (1, 'Product A', 2020, FALSE),
    (2, 'Product B', 2021, TRUE),
    (3, 'Product C', 2022, FALSE)

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/product/

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/product/_delta_log

-- COMMAND ----------

SELECT * FROM json.`dbfs:/mnt/global/india/silver/product/_delta_log/00000000000000000001.json`

-- COMMAND ----------

DELETE FROM product where id=3

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/product/

-- COMMAND ----------

-- MAGIC %fs ls /mnt/global/india/silver/product/_delta_log

-- COMMAND ----------

SELECT * FROM json.`dbfs:/mnt/global/india/silver/product/_delta_log/00000000000000000003.json`

-- COMMAND ----------

desc history product