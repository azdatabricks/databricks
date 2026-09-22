-- Databricks notebook source
SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false;

-- COMMAND ----------

CREATE TABLE small_departments (
    dept_id INT,
    dept_name VARCHAR(100),
    location VARCHAR(100)
)
using delta
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/small_departments'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in range(1,101):
-- MAGIC   query=f"INSERT INTO small_departments values({i},'rama{i}','Bangalore{i}');"
-- MAGIC   print(query)
-- MAGIC

-- COMMAND ----------

-- INSERT INTO small_departments values(1,'rama1','Bangalore1');
-- INSERT INTO small_departments values(2,'rama2','Bangalore2');
-- INSERT INTO small_departments values(3,'rama3','Bangalore3');
-- INSERT INTO small_departments values(4,'rama4','Bangalore4');
-- INSERT INTO small_departments values(5,'rama5','Bangalore5');
-- INSERT INTO small_departments values(6,'rama6','Bangalore6');
-- INSERT INTO small_departments values(7,'rama7','Bangalore7');
-- INSERT INTO small_departments values(8,'rama8','Bangalore8');
-- INSERT INTO small_departments values(9,'rama9','Bangalore9');
-- INSERT INTO small_departments values(10,'rama10','Bangalore10');
-- INSERT INTO small_departments values(11,'rama11','Bangalore11');
-- INSERT INTO small_departments values(12,'rama12','Bangalore12');
-- INSERT INTO small_departments values(13,'rama13','Bangalore13');
-- INSERT INTO small_departments values(14,'rama14','Bangalore14');
-- INSERT INTO small_departments values(15,'rama15','Bangalore15');
-- INSERT INTO small_departments values(16,'rama16','Bangalore16');
-- INSERT INTO small_departments values(17,'rama17','Bangalore17');
-- INSERT INTO small_departments values(18,'rama18','Bangalore18');
-- INSERT INTO small_departments values(19,'rama19','Bangalore19');
-- INSERT INTO small_departments values(20,'rama20','Bangalore20');
-- INSERT INTO small_departments values(21,'rama21','Bangalore21');
-- INSERT INTO small_departments values(22,'rama22','Bangalore22');
-- INSERT INTO small_departments values(23,'rama23','Bangalore23');
-- INSERT INTO small_departments values(24,'rama24','Bangalore24');
-- INSERT INTO small_departments values(25,'rama25','Bangalore25');
-- INSERT INTO small_departments values(26,'rama26','Bangalore26');
-- INSERT INTO small_departments values(27,'rama27','Bangalore27');
-- INSERT INTO small_departments values(28,'rama28','Bangalore28');
-- INSERT INTO small_departments values(29,'rama29','Bangalore29');
-- INSERT INTO small_departments values(30,'rama30','Bangalore30');
-- INSERT INTO small_departments values(31,'rama31','Bangalore31');
-- INSERT INTO small_departments values(32,'rama32','Bangalore32');
-- INSERT INTO small_departments values(33,'rama33','Bangalore33');
-- INSERT INTO small_departments values(34,'rama34','Bangalore34');
-- INSERT INTO small_departments values(35,'rama35','Bangalore35');
-- INSERT INTO small_departments values(36,'rama36','Bangalore36');
-- INSERT INTO small_departments values(37,'rama37','Bangalore37');
-- INSERT INTO small_departments values(38,'rama38','Bangalore38');
-- INSERT INTO small_departments values(39,'rama39','Bangalore39');
-- INSERT INTO small_departments values(40,'rama40','Bangalore40');
-- INSERT INTO small_departments values(41,'rama41','Bangalore41');
-- INSERT INTO small_departments values(42,'rama42','Bangalore42');
-- INSERT INTO small_departments values(43,'rama43','Bangalore43');
-- INSERT INTO small_departments values(44,'rama44','Bangalore44');
-- INSERT INTO small_departments values(45,'rama45','Bangalore45');
-- INSERT INTO small_departments values(46,'rama46','Bangalore46');
-- INSERT INTO small_departments values(47,'rama47','Bangalore47');
-- INSERT INTO small_departments values(48,'rama48','Bangalore48');
-- INSERT INTO small_departments values(49,'rama49','Bangalore49');
-- INSERT INTO small_departments values(50,'rama50','Bangalore50');
-- INSERT INTO small_departments values(51,'rama51','Bangalore51');
-- INSERT INTO small_departments values(52,'rama52','Bangalore52');
-- INSERT INTO small_departments values(53,'rama53','Bangalore53');
-- INSERT INTO small_departments values(54,'rama54','Bangalore54');
-- INSERT INTO small_departments values(55,'rama55','Bangalore55');
-- INSERT INTO small_departments values(56,'rama56','Bangalore56');
-- INSERT INTO small_departments values(57,'rama57','Bangalore57');
-- INSERT INTO small_departments values(58,'rama58','Bangalore58');
-- INSERT INTO small_departments values(59,'rama59','Bangalore59');
-- INSERT INTO small_departments values(60,'rama60','Bangalore60');
-- INSERT INTO small_departments values(61,'rama61','Bangalore61');
-- INSERT INTO small_departments values(62,'rama62','Bangalore62');
-- INSERT INTO small_departments values(63,'rama63','Bangalore63');
-- INSERT INTO small_departments values(64,'rama64','Bangalore64');
-- INSERT INTO small_departments values(65,'rama65','Bangalore65');
-- INSERT INTO small_departments values(66,'rama66','Bangalore66');
-- INSERT INTO small_departments values(67,'rama67','Bangalore67');
-- INSERT INTO small_departments values(68,'rama68','Bangalore68');
-- INSERT INTO small_departments values(69,'rama69','Bangalore69');
-- INSERT INTO small_departments values(70,'rama70','Bangalore70');
-- INSERT INTO small_departments values(71,'rama71','Bangalore71');
-- INSERT INTO small_departments values(72,'rama72','Bangalore72');
-- INSERT INTO small_departments values(73,'rama73','Bangalore73');
-- INSERT INTO small_departments values(74,'rama74','Bangalore74');
-- INSERT INTO small_departments values(75,'rama75','Bangalore75');
-- INSERT INTO small_departments values(76,'rama76','Bangalore76');
-- INSERT INTO small_departments values(77,'rama77','Bangalore77');
-- INSERT INTO small_departments values(78,'rama78','Bangalore78');
-- INSERT INTO small_departments values(79,'rama79','Bangalore79');
-- INSERT INTO small_departments values(80,'rama80','Bangalore80');
-- INSERT INTO small_departments values(81,'rama81','Bangalore81');
-- INSERT INTO small_departments values(82,'rama82','Bangalore82');
-- INSERT INTO small_departments values(83,'rama83','Bangalore83');
-- INSERT INTO small_departments values(84,'rama84','Bangalore84');
-- INSERT INTO small_departments values(85,'rama85','Bangalore85');
-- INSERT INTO small_departments values(86,'rama86','Bangalore86');
-- INSERT INTO small_departments values(87,'rama87','Bangalore87');
-- INSERT INTO small_departments values(88,'rama88','Bangalore88');
-- INSERT INTO small_departments values(89,'rama89','Bangalore89');
-- INSERT INTO small_departments values(90,'rama90','Bangalore90');
-- INSERT INTO small_departments values(91,'rama91','Bangalore91');
-- INSERT INTO small_departments values(92,'rama92','Bangalore92');
-- INSERT INTO small_departments values(93,'rama93','Bangalore93');
-- INSERT INTO small_departments values(94,'rama94','Bangalore94');
-- INSERT INTO small_departments values(95,'rama95','Bangalore95');
-- INSERT INTO small_departments values(96,'rama96','Bangalore96');
-- INSERT INTO small_departments values(97,'rama97','Bangalore97');
-- INSERT INTO small_departments values(98,'rama98','Bangalore98');
-- INSERT INTO small_departments values(99,'rama99','Bangalore99');
-- INSERT INTO small_departments values(100,'rama100','Bangalore100');

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/small_departments

-- COMMAND ----------

desc history small_departments

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in range(1,101):
-- MAGIC   query=f"INSERT INTO small_departments values({i},'rama{i}','Bangalore{i}');"
-- MAGIC   spark.sql(query)

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/small_departments

-- COMMAND ----------

desc history small_departments

-- COMMAND ----------

desc detail small_departments

-- COMMAND ----------

select * from small_departments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Table with single BigFile

-- COMMAND ----------

CREATE TABLE big_departments (
    dept_id INT,
    dept_name VARCHAR(100),
    location VARCHAR(100)
)
using delta
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/big_departments'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC l=[]
-- MAGIC for i in range(1,101):
-- MAGIC   query=f"({i},'rama{i}','Bangalore{i}')"
-- MAGIC   l.append(query)
-- MAGIC   data=", ".join(l)
-- MAGIC final_query=f"INSERT INTO big_departments VALUES {data}"
-- MAGIC print(final_query)
-- MAGIC
-- MAGIC

-- COMMAND ----------

INSERT INTO big_departments VALUES (1,'rama1','Bangalore1'), (2,'rama2','Bangalore2'), (3,'rama3','Bangalore3'), (4,'rama4','Bangalore4'), (5,'rama5','Bangalore5'), (6,'rama6','Bangalore6'), (7,'rama7','Bangalore7'), (8,'rama8','Bangalore8'), (9,'rama9','Bangalore9'), (10,'rama10','Bangalore10'), (11,'rama11','Bangalore11'), (12,'rama12','Bangalore12'), (13,'rama13','Bangalore13'), (14,'rama14','Bangalore14'), (15,'rama15','Bangalore15'), (16,'rama16','Bangalore16'), (17,'rama17','Bangalore17'), (18,'rama18','Bangalore18'), (19,'rama19','Bangalore19'), (20,'rama20','Bangalore20'), (21,'rama21','Bangalore21'), (22,'rama22','Bangalore22'), (23,'rama23','Bangalore23'), (24,'rama24','Bangalore24'), (25,'rama25','Bangalore25'), (26,'rama26','Bangalore26'), (27,'rama27','Bangalore27'), (28,'rama28','Bangalore28'), (29,'rama29','Bangalore29'), (30,'rama30','Bangalore30'), (31,'rama31','Bangalore31'), (32,'rama32','Bangalore32'), (33,'rama33','Bangalore33'), (34,'rama34','Bangalore34'), (35,'rama35','Bangalore35'), (36,'rama36','Bangalore36'), (37,'rama37','Bangalore37'), (38,'rama38','Bangalore38'), (39,'rama39','Bangalore39'), (40,'rama40','Bangalore40'), (41,'rama41','Bangalore41'), (42,'rama42','Bangalore42'), (43,'rama43','Bangalore43'), (44,'rama44','Bangalore44'), (45,'rama45','Bangalore45'), (46,'rama46','Bangalore46'), (47,'rama47','Bangalore47'), (48,'rama48','Bangalore48'), (49,'rama49','Bangalore49'), (50,'rama50','Bangalore50'), (51,'rama51','Bangalore51'), (52,'rama52','Bangalore52'), (53,'rama53','Bangalore53'), (54,'rama54','Bangalore54'), (55,'rama55','Bangalore55'), (56,'rama56','Bangalore56'), (57,'rama57','Bangalore57'), (58,'rama58','Bangalore58'), (59,'rama59','Bangalore59'), (60,'rama60','Bangalore60'), (61,'rama61','Bangalore61'), (62,'rama62','Bangalore62'), (63,'rama63','Bangalore63'), (64,'rama64','Bangalore64'), (65,'rama65','Bangalore65'), (66,'rama66','Bangalore66'), (67,'rama67','Bangalore67'), (68,'rama68','Bangalore68'), (69,'rama69','Bangalore69'), (70,'rama70','Bangalore70'), (71,'rama71','Bangalore71'), (72,'rama72','Bangalore72'), (73,'rama73','Bangalore73'), (74,'rama74','Bangalore74'), (75,'rama75','Bangalore75'), (76,'rama76','Bangalore76'), (77,'rama77','Bangalore77'), (78,'rama78','Bangalore78'), (79,'rama79','Bangalore79'), (80,'rama80','Bangalore80'), (81,'rama81','Bangalore81'), (82,'rama82','Bangalore82'), (83,'rama83','Bangalore83'), (84,'rama84','Bangalore84'), (85,'rama85','Bangalore85'), (86,'rama86','Bangalore86'), (87,'rama87','Bangalore87'), (88,'rama88','Bangalore88'), (89,'rama89','Bangalore89'), (90,'rama90','Bangalore90'), (91,'rama91','Bangalore91'), (92,'rama92','Bangalore92'), (93,'rama93','Bangalore93'), (94,'rama94','Bangalore94'), (95,'rama95','Bangalore95'), (96,'rama96','Bangalore96'), (97,'rama97','Bangalore97'), (98,'rama98','Bangalore98'), (99,'rama99','Bangalore99'), (100,'rama100','Bangalore100')

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/big_departments

-- COMMAND ----------

desc history big_departments

-- COMMAND ----------

desc detail big_departments

-- COMMAND ----------

select * from big_departments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimize table with small files

-- COMMAND ----------

desc history small_departments

-- COMMAND ----------

OPTIMIZE small_departments

-- COMMAND ----------

desc history small_departments

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/small_departments

-- COMMAND ----------

select * from small_departments

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/small_departments

-- COMMAND ----------

vacuum small_departments retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/small_departments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Example2 for Optimize

-- COMMAND ----------

CREATE TABLE Product (
    ProductID INT,
    ProductName STRING NOT NULL,
    Category STRING NOT NULL,
    Price INT NOT NULL,
    StockQuantity INT NOT NULL,
    Supplier STRING NOT NULL
)
USING DELTA
LOCATION 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product'

-- COMMAND ----------

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES (1, 'Wireless Mouse', 'Electronics', 26, 120, 'TechWorld Co.');





-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES (2, 'Office Chair', 'Furniture', 150, 45, 'HomeComfort Inc.');



-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES (3, 'Smartphone', 'Electronics', 700, 60, 'GadgetPro Ltd.');



-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES (4, 'Water Bottle', 'Accessories', 16, 200, 'EcoLife Goods'),
(5, 'Running Shoes', 'Footwear', 90, 80, 'FitWell Supplies'),
(6, 'Desk Lamp', 'Furniture', 35, 100, 'BrightHome LLC');




-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

select * from product

-- COMMAND ----------

desc history product

-- COMMAND ----------

update product set productname='Flask' where productid=4

-- COMMAND ----------

desc history product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log

-- COMMAND ----------

select * from json.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log/00000000000000000006.json`

-- COMMAND ----------

delete from product where productid=5

-- COMMAND ----------

desc history product

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

select * from json.`abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product/_delta_log/00000000000000000008.json`

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

vacuum product retain 0 hours

-- COMMAND ----------

-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/product

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Turning on Auto-Compact

-- COMMAND ----------

SET spark.databricks.delta.autoCompact.enabled=true

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.databricks.delta.autoCompact.minNumFiles")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.databricks.delta.optimize.maxFileSize")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 1073741824/(1024*1024)

-- COMMAND ----------

create table emp(emp_id int,emp_name string,emp_salary long)
using delta
location 'abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in range(1,110):
-- MAGIC   query=f"INSERT INTO emp values({i},'rama{i}',100*{i})"
-- MAGIC   spark.sql(query)

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

desc history emp

-- COMMAND ----------

vacuum emp retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls abfss://global@cloudpandithdevadls0405.dfs.core.windows.net/dev/bronze/emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### second example

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.databricks.delta.optimize.maxFileSize")

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.optimize.maxFileSize","1073741824")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/databricks-datasets/asa/airlines")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 689413344/(1024*1024)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("/databricks-datasets/asa/airlines",header=True)
-- MAGIC df.repartition(100).write.format("delta").saveAsTable("airlines")
-- MAGIC

-- COMMAND ----------

desc history airlines