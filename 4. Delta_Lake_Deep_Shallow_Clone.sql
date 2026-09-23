-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Table Clone
-- MAGIC - DEEP vs SHALLOW CLONE

-- COMMAND ----------

create table product(id INT,name STRING,color String)
using delta
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product'

-- COMMAND ----------

INSERT INTO product (id, name, color)
VALUES
    (1, 'Apple', 'Red'),
    (2, 'Banana', 'Yellow'),
    (3, 'Grape', 'Purple'),
    (4, 'Orng', 'Orange'),
    (5, 'Lemon', 'Yellow');

-- COMMAND ----------

DELETE FROM product where id=5


-- COMMAND ----------

desc history product

-- COMMAND ----------

UPDATE product SET name='orange' where id=4

-- COMMAND ----------

ALTER TABLE product Add Constraint id_constraint CHECK (id > 0);

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log

-- COMMAND ----------

select * from json.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log/00000000000000000006.json`

-- COMMAND ----------

DESC HISTORY product

-- COMMAND ----------

DROP TABLE IF EXISTS deep_clone_product

-- COMMAND ----------

CREATE OR REPLACE TABLE deep_clone_product
DEEP CLONE product
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deep_clone_product'


-- COMMAND ----------

DESC HISTORY deep_clone_product

-- COMMAND ----------

DESC detail deep_clone_product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deep_clone_product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deep_clone_product/_delta_log

-- COMMAND ----------

select * from product

-- COMMAND ----------

select * from deep_clone_product

-- COMMAND ----------

INSERT INTO product (id, name, color)
VALUES (5, 'Lemon', 'Yellow');

-- COMMAND ----------

select * from product

-- COMMAND ----------

select * from deep_clone_product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

select * from json.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log/00000000000000000007.json`

-- COMMAND ----------

delete from product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

select * from product

-- COMMAND ----------

select * from deep_clone_product

-- COMMAND ----------

VACUUM product RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM product RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log

-- COMMAND ----------

select * from json.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log/00000000000000000010.json`

-- COMMAND ----------

select * from product

-- COMMAND ----------

SELECT * FROM deep_clone_product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/deep_clone_product

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Shallow clone

-- COMMAND ----------

CREATE TABLE emp (
    emp_id INT,
    emp_name VARCHAR(255),
    emp_salary INT
)
using DELTA
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp'

-- COMMAND ----------

INSERT INTO emp (emp_id, emp_name, emp_salary)
VALUES
(1, 'John Doe', 75000),
(2, 'Jane Smith', 82000),
(3, 'Alice Johnson', 95000),
(4, 'RK', 60000),
(5, 'Charlie Davis', 70000);

-- COMMAND ----------

update emp set emp_name='RamaKrishna' where emp_id=4

-- COMMAND ----------

DELETE FROM emp where emp_id=5;

-- COMMAND ----------

alter table emp add constraint emp_id_check CHECK (emp_id>0)

-- COMMAND ----------

desc history emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp/_delta_log

-- COMMAND ----------

select * from emp

-- COMMAND ----------

DROP TABLE if exists shallow_clone_emp

-- COMMAND ----------

CREATE or REPLACE table shallow_clone_emp
SHALLOW CLONE emp
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/shallow_clone_emp'


-- COMMAND ----------

desc history shallow_clone_emp

-- COMMAND ----------

desc detail shallow_clone_emp

-- COMMAND ----------

-- MAGIC %fs ls dbfs:abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/shallow_clone_emp

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from shallow_clone_emp

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/shallow_clone_emp

-- COMMAND ----------

INSERT INTO emp (emp_id, emp_name, emp_salary)
VALUES
(5, 'Charlie Davis', 70000);

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from shallow_clone_emp

-- COMMAND ----------

DELETE FROM emp

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from shallow_clone_emp

-- COMMAND ----------

vacuum emp retain 0 hours;

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

-- SET spark.databricks.delta.retensionDurationCheck.enabled = false

-- COMMAND ----------

-- vacuum emp retain 0 hours;

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from shallow_clone_emp