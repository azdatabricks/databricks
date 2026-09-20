# Databricks notebook source
# MAGIC %md
# MAGIC #Schema Enforcement
# MAGIC (write-time Validation)
# MAGIC
# MAGIC - Additional Columns
# MAGIC - Invalid types
# MAGIC - Column names that differ only by case
# MAGIC
# MAGIC Delta Lake Supports Schema Evolution, but its controlled and explicit
# MAGIC - Additional Columns
# MAGIC - Minor data types changes, like int-->bigint
# MAGIC
# MAGIC # Schame Evaluation
# MAGIC - spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",True)
# MAGIC - SET spark.databricks.delta.schema.autoMerge.enabled = True
# MAGIC - df1.write.mode("overwrite").format('delta').option("mergeSchema",True).save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/")

# COMMAND ----------

dbutils.fs.rm("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/parquetdata/",True)
dbutils.fs.rm("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/",True)

# COMMAND ----------

# MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/parquetdata/

# COMMAND ----------

# MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/

# COMMAND ----------

df=spark.createDataFrame([(1,"rama","bangalore"),(2,"krishna","hyderbad"),(3,"sai","chennai")],["cid","cname","clocation"])
display(df)

# COMMAND ----------

df.write.mode("overwrite").format('parquet').save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/parquetdata/")

# COMMAND ----------

df.write.mode("overwrite").format('delta').save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/parquetdata/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/`

# COMMAND ----------

df1=spark.createDataFrame([(1,"rama","bangalore",8904424822),(2,"krishna","chennai",896897897),(3,"sai","hyderbad",8968969)],["cid","cname","clocation","ccontact"])
display(df1)

# COMMAND ----------

df1.write.mode("append").format('parquet').save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/parquetdata/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/parquetdata/`

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Enforcement

# COMMAND ----------

df1.write.mode("overwrite").format('delta').save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/")

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Evaluation

# COMMAND ----------

df1.write.mode("append").format('delta').option("mergeSchema",True).save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata/`

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",True)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltacustdata/

# COMMAND ----------

df.write.mode("overwrite").format('delta').save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltacustdata/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltacustdata/`

# COMMAND ----------

df1.write.mode("overwrite").format('delta').save("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltacustdata/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltacustdata/`

# COMMAND ----------

# MAGIC %md
# MAGIC #Time Travel & Versioning
# MAGIC - Read Previous Version data in Python
# MAGIC   - srcpath=""/mnt/global/india/deltadata/""
# MAGIC   - spark.read.format("delta").option("timestampAsOf", "2024-11-15T10:00:00Z").load(srcpath)
# MAGIC   - spark.read.format("delta").option("versionAsOf", "2").load(srcpath)
# MAGIC   - spark.read.format("delta").option("timestampAsOf", "2024-11-15T10:00:00Z").table("product")
# MAGIC   - spark.read.format("delta").option("versionAsOf", "2").table("product")
# MAGIC
# MAGIC - Read Previous Version data in SQL
# MAGIC
# MAGIC   - SELECT * FROM product TIMESTAMP AS OF '2024-11-15T10:15:12.013Z';
# MAGIC   - SELECT * FROM product VERSION AS OF 2;
# MAGIC
# MAGIC   - SELECT * FROM product@20241115000000000
# MAGIC   - SELECT * FROM product@v2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Delta Lake also supports query or even restore previous versions of the table/files
# MAGIC - We should have parquet files and corresponding delta logs should be available
# MAGIC - We dont want to keep so long delta logs or parquet files which can become bottle neck for delta lake to query
# MAGIC - We should clean up unnecessary parquet files and delta logs
# MAGIC - default value for delta logs is 30 days 
# MAGIC - default value for parquet files are 7 days
# MAGIC - delta.logRetentionDuration = "interval <interval>": controls how long the history for a table is kept. The default is interval 30 days.
# MAGIC - delta.deletedFileRetentionDuration = "interval <interval>": determines the threshold VACUUM uses to remove data files no longer referenced in the current table version. The default is interval 7 days.

# COMMAND ----------

# MAGIC %sql
# MAGIC create table product(id INT,name STRING,color String)
# MAGIC using delta
# MAGIC location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into product values (1,'Product A','Blue');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into product values (2,'Product B','Black');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into product values (3,'Product C','Green');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history product

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from product where id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from product

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history product

# COMMAND ----------

# MAGIC %md
# MAGIC # TimeTravel in Python
# MAGIC - Read previous version using Python

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history product

# COMMAND ----------

df=spark.read.format("delta").load("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product")
display(df)

# COMMAND ----------

df=spark.read.format("delta").option("versionAsOf",2).load("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product")
display(df)

# COMMAND ----------

df=spark.read.format("delta").option("versionAsOf",1).table("product")
display(df)

# COMMAND ----------

df=spark.read.format("delta").option("timestampAsOf","2026-05-06T03:03:14.000+00:00").table("product")
display(df)

# COMMAND ----------

df=spark.read.format("delta").option("timestampAsOf","2026-05-06T03:04:16.000+00:00").load("abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product

# COMMAND ----------

# MAGIC %sql
# MAGIC show tblproperties product

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum product Retain 0 HOURS dry run;

# COMMAND ----------

# %sql
# vacuum product Retain 0 HOURS;

# COMMAND ----------

from delta.tables import DeltaTable
dtable = DeltaTable.forPath(spark, "abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product")
dtable.vacuum(0)

# COMMAND ----------

# MAGIC %md
# MAGIC - https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-tblproperties

# COMMAND ----------

# MAGIC %sql
# MAGIC alter TABLE PRODUCT
# MAGIC SET TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 10 days')

# COMMAND ----------

# MAGIC %sql
# MAGIC alter TABLE PRODUCT
# MAGIC SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 60 days')

# COMMAND ----------

### Explanation: `tblproperties ('delta.enableTypeWidening' = True)`

- **delta.enableTypeWidening**: When set to `True`, this property allows Delta Lake to automatically widen column types during schema evolution. For example, it permits changing a column from `INT` to `LONG` or from `FLOAT` to `DOUBLE` when merging or appending data.
- **Use case**: Useful for scenarios where incoming data may require broader types than the current schema, enabling seamless schema updates without manual intervention.

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists produc2
# MAGIC using delta
# MAGIC location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deltadata2/'
# MAGIC tblproperties ('delta.enableTypeWidening' = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### END OF THE NOTEBOOK

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Enforcement & Evaluation in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop database deltadb cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database deltadb;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use database deltadb;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists emp
# MAGIC (id INT NOT NULL,emp_name STRING,emp_salary INT,active BOOLEAN)
# MAGIC USING delta
# MAGIC location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/empdata'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into emp values
# MAGIC (1,'rama',1000,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into emp values
# MAGIC (2,'krishna',2000,True,"Data Engineer")

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into emp values
# MAGIC ("two",'krishna',2000,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into emp values
# MAGIC ("2",'krishna',2000,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into emp values
# MAGIC (10000000000000000000,'krishna',2000,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

df2=spark.createDataFrame([(3,'shiva',3000,True,"Devops Engineer")],schema="id INT,emp_name STRING, emp_salary INT,active BOOLEAN, role STRING")
display(df2)

# COMMAND ----------

df2.write.option("mergeSchema",True)\
    .mode("append")\
    .saveAsTable("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

df3=spark.createDataFrame([("three",'shiva',3000,True,"Devops Engineer")],schema="id STRING,emp_name STRING, emp_salary INT,active BOOLEAN, role STRING")
display(df3)

# COMMAND ----------

df3.write.option("mergeSchema",True)\
    .mode("append")\
    .saveAsTable("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SET spark.databricks.delta.schema.autoMerge.enabled = True

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into emp values
# MAGIC (4,'venkat',4000,True,"Data Architect")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  delta.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/empdata`

# COMMAND ----------

df=spark.createDataFrame([(1,"rama","bangalore"),(2,"krishna","hyderbad"),(3,"sai","chennai")],["cid","cname","clocation"])
display(df)

# COMMAND ----------

df.write.option("overwriteSchema","true")\
  .mode("overwrite")\
  .saveAsTable("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %md
# MAGIC #TimeTravel in SQL
# MAGIC - Read previous versions using SQL (Time Travel)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product@v3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product@20241118030248000

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product TIMESTAMP AS OF "2024-11-18T03:02:48Z"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product` TIMESTAMP AS OF "2024-11-18T03:02:39Z"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product` VERSION AS OF 4

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history product

# COMMAND ----------

# MAGIC %md
# MAGIC