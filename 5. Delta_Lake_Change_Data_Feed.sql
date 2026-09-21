-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Change Data Feed to Build Audit Trail table

-- COMMAND ----------

-- set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

-- COMMAND ----------

CREATE TABLE departments (
    dept_id INT,
    dept_name VARCHAR(100),
    location VARCHAR(100)
)
using delta
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments'

-- COMMAND ----------

INSERT INTO departments (dept_id, dept_name, location)
VALUES
(1, 'Engineering', 'San Francisco'),
(2, 'HR', 'New York'),
(3, 'Marketing', 'Los Angeles'),
(4, 'Sales', 'Chicago');

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments/_delta_log

-- COMMAND ----------

desc history departments

-- COMMAND ----------

ALTER TABLE departments SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments/_delta_log

-- COMMAND ----------

desc history departments

-- COMMAND ----------

INSERT INTO departments (dept_id, dept_name, location) 
VALUES
(5, 'Finance', 'Boston'),
(6, 'Customer Support', 'Austin')

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments

-- COMMAND ----------

select * from departments

-- COMMAND ----------

update departments set dept_name='Stocks' where dept_id=3

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments

-- COMMAND ----------

delete from departments where dept_id=2

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments/_change_data/

-- COMMAND ----------

DESC HISTORY departments

-- COMMAND ----------

SELECT * FROM table_changes('departments',1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - How to captures changes and collects stats

-- COMMAND ----------

create or replace table departments_audit_data (dept_id INT,dept_name STRING,location STRING, operation STRING,commit_version LONG,commit_timestamp timestamp)
using delta
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/departments_audit_data'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC spark.readStream.format("delta")\
-- MAGIC     .option("readChangeFeed","true")\
-- MAGIC     .option("startingVersion",2)\
-- MAGIC     .table("departments")\
-- MAGIC     .filter(col("_change_type").isin(['delete','update_preimage','update_postimage','insert']))\
-- MAGIC     .select("dept_id","dept_name","location",col("_change_type").alias("operation"),col('_commit_version').alias('commit_version'),col('_commit_timestamp').alias("commit_timestamp"))\
-- MAGIC   .writeStream\
-- MAGIC     .outputMode("append")\
-- MAGIC     .option("checkpointLocation","abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deptcheckpoint")\
-- MAGIC     .trigger(once=True)\
-- MAGIC     .table("departments_audit_data")

-- COMMAND ----------

select * from departments_audit_data

-- COMMAND ----------

select * from departments

-- COMMAND ----------

delete from departments where dept_id=3

-- COMMAND ----------

update departments set dept_name='IT HELPDESK' where dept_id=6

-- COMMAND ----------

select * from departments_audit_data

-- COMMAND ----------

DESC HISTORY DEPARTMENTS

-- COMMAND ----------

select * from table_changes('departments',2)

-- COMMAND ----------

desc history departments

-- COMMAND ----------

select * from table_changes('departments',2,5)

-- COMMAND ----------

select * from table_changes('departments','2026-05-08T02:52:26.000+00:00')

-- COMMAND ----------

select * from table_changes('departments','2026-05-08T02:52:26.000+00:00','2026-05-08T02:57:26.000+00:00')

-- COMMAND ----------

select * from departments_audit_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###CDF for buidling silver and gold layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Create Silver table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.createDataFrame([('IND',500,700),('AUS',7000,10000),('RUS',500,550),('ISR',1000,1500),('USA',10000,20000)],['country','numvaccinated','availabledoses'])
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # dbutils.fs.rm("dbfs:/user/hive/warehouse/",True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").saveAsTable("sl_country_vaccination")

-- COMMAND ----------

select * from sl_country_vaccination

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Create Gold table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=spark.createDataFrame([('IND',71.42),('AUS',70.0),('RUS',90.90909090),('ISR',66.66666666666),('USA',50.0)],['country','vaccinationRate'])
-- MAGIC display(df1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1.write.format("delta").saveAsTable("gl_vaccinationrate")

-- COMMAND ----------

select * from gl_vaccinationrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Enable Change Data Feed for Silver table

-- COMMAND ----------

alter table sl_country_vaccination SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Update operation on Silver table

-- COMMAND ----------

update sl_country_vaccination SET numvaccinated = 19000 where country='USA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Create source table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.createDataFrame([('CAN',1000,3000),('CHN',700,750),('ISR',1500,2000)],['country','numvaccinated','availabledoses'])
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").saveAsTable("src")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Merge source records into Silver table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC silvertable=DeltaTable.forName(spark,'sl_country_vaccination')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC silvertable.alias("a").merge(df.alias("b"),col("a.country")==col("b.country"))\
-- MAGIC   .whenMatchedUpdate(\
-- MAGIC     set={'numvaccinated':'b.numvaccinated','availabledoses':'b.availabledoses'})\
-- MAGIC   .whenNotMatchedInsert(\
-- MAGIC     values={'country':'b.country','numvaccinated':'b.numvaccinated','availabledoses':'b.availabledoses'})\
-- MAGIC   .execute()

-- COMMAND ----------

select * from sl_country_vaccination

-- COMMAND ----------

desc history sl_country_vaccination

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #####Read change data feed from silver table using table_changes function

-- COMMAND ----------

select * from table_changes('sl_country_vaccination',2,4 )

-- COMMAND ----------

-- DBTITLE 1,Filter to take updated & New Records
select * from table_changes('sl_country_vaccination',2,4 ) where _change_type!='update_preimage'

-- COMMAND ----------

select * from gl_vaccinationrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Consume tge change data deed from the Silver table to update the Gold Table

-- COMMAND ----------

-- Merge the changes to gold
Merge into gl_vaccinationrate a 
using (select * from table_changes('sl_country_vaccination',2,4 ) where _change_type!='update_preimage') b
on a.country=b.country
when Matched AND b._change_type='update_postimage'
  then update set vaccinationRate=(b.numvaccinated/b.availabledoses)*100
when not matched
  then insert (country,vaccinationRate) values (b.country,(b.numvaccinated/b.availabledoses)*100)

-- COMMAND ----------

select * from gl_vaccinationrate

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.format("delta") \
-- MAGIC   .option("readChangeFeed", "true") \
-- MAGIC   .option("startingVersion", 2) \
-- MAGIC   .option("endingVersion", 6) \
-- MAGIC   .table("sl_country_vaccination")
-- MAGIC display(df)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read \
-- MAGIC   .option("readChangeFeed", "true") \
-- MAGIC   .option("startingTimestamp", '2026-05-08T03:28:52.000+00:00') \
-- MAGIC   .option("endingTimestamp", '2026-05-08T03:31:14.000+00:00') \
-- MAGIC   .table("sl_country_vaccination")
-- MAGIC display(df)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read \
-- MAGIC   .option("readChangeFeed", "true") \
-- MAGIC   .option("startingVersion",2) \
-- MAGIC   .table("sl_country_vaccination")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime,timedelta
-- MAGIC import pytz
-- MAGIC print(datetime.now(pytz.timezone("Asia/Kolkata"))-timedelta(minutes=5))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime,timedelta
-- MAGIC import pytz
-- MAGIC print(datetime.now(pytz.timezone("Asia/Kolkata")))