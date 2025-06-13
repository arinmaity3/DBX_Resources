# Databricks notebook source
# MAGIC %md
# MAGIC "spark-submit" is a command line tool that allows you to submitthe spark application to the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy Modes:
# MAGIC 1. Cluster Mode
# MAGIC 2. Client Mode

# COMMAND ----------

# MAGIC %md
# MAGIC spark dataframe API catagories:
# MAGIC 1. Tranformations
# MAGIC  - narrow dependency: can be performed in parallel on data partitions [select(),filter(),drop(),withColumn()]
# MAGIC  - wide dependency : performed after grouping data from multiple partitions [groupBy(),join()]
# MAGIC 2. Actions : Used to trigger some work [count(),collect(),take(),read(),write()]
# MAGIC All spark action triggers one or more jobs

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Job: Spark create one job for each action. This job may contain series of multiple transformations
# MAGIC 2. stage: depends on wide dependency 
# MAGIC 3. shuffle/sort: data from one stage to another stage shared though this operation.
# MAGIC 4. Task: Each stage may be executed as one or more parallel tasks depend on partition. Task is the smallest unit of a spark job.

# COMMAND ----------

# MAGIC %md
# MAGIC spark sql/dataframe --> spark job --> Unresolved logical plan --> spark sql engine

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Analysis: parse your code for error and incorrectiveness.
# MAGIC 2. Logical Optimization:
# MAGIC 3. Physical Planning:
# MAGIC 4. Code Generaion:

# COMMAND ----------

# MAGIC %md
# MAGIC - spark.driver.memory -> JVM memory
# MAGIC - spark.driver.memoryOverhead -> max(10% of driver memory or 384MB)
# MAGIC
# MAGIC overhead memory is used for container process or any non-jvm processess within the container

# COMMAND ----------

# MAGIC %md
# MAGIC Total memory allocated to executor container is sum of following:
# MAGIC 1. Overhead Memory : spark.executor.memeryOverhead
# MAGIC 2. Heap Memory : spark.executor.memory
# MAGIC 3. Off Heap Memory : spark.memory.offHeap.size
# MAGIC 4. PySpark Memory : spark.executor.pyspark.memory
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC Adaptive Query Execution(AQE): spark.sql.adaptive.enabled= True
# MAGIC 1. Dynamically coalescing shuffle partitions
# MAGIC 2. Dynamically switching join strategies
# MAGIC 3. Dynamically optimizing skew joins

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tower_location,
# MAGIC sum(call_duraion) AS duration_served
# MAGIC FROM call_records
# MAGIC GROUP BY tower_location
# MAGIC

# COMMAND ----------

df.groupBy("tower_location").agg(sum("call_duration").alias("duration_served"))

# COMMAND ----------

spark.sql.autoBroadcastJoinThreshold = 10MB 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Broadcast shuffle join is the most performant join strategy
# MAGIC 2. We can apply broadcast join if one side of the join can fit well in memory
# MAGIC 3. One table must be shorter than spark.sql.autoBroadcastJoinThreshold
# MAGIC 4. AQE can help
# MAGIC  - by computing table size at shuffle time
# MAGIC  - Replans the join strategy at runtime converting sort-merge join to a broadcast hash join
