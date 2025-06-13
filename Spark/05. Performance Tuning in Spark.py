# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Data Frame over RDD
# MAGIC - Using **RDD** directly leads to performance issues as Spark doesn't know how to apply the optimization techniques as RDD **serialize and de-serialize** the data when it distributes across a cluster(repartition and cluster).
# MAGIC
# MAGIC - Serialization and de-serialization are very **expensive operations** for spark application or any distributed systems, most of our time is spent only on serialization of data rather than exacuting the operations hence try to avoid using RDD
# MAGIC
# MAGIC - Since Spark/PySpark **Dataframe** internally stores data in **binary**, there is no need of serialization and de-serialization data when it distributes across a cluster hence you would see **performance improvement**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Caching Data in memory
# MAGIC - Spark SQL can cache tables using an **in-memory columnar** format by calling:
# MAGIC
# MAGIC **`spark.catalog.cacheTable("table_name")`**
# MAGIC or
# MAGIC **`dataFrame.cache()`**
# MAGIC
# MAGIC - Then Spark SQL will only scan required columns and will automatically tune compression to minimize memory usage and GC pressure.
# MAGIC For removeing cache:
# MAGIC **`spark.catalog.uncacheTable("table_name")`** or **`dataFrame.unpersist()`**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Use Serialized Data Format 
# MAGIC Most of the Spark jobs run as pipeline where one Spark job writes data into a File and another Spark jobs read the data,process it and writes to another file for another Spark job to pick up.
# MAGIC
# MAGIC When you have this kind of use cases, always prefer writing an intermidiate file in Serialized and optimized formats, like Avro, Kryo,parquet etc. Any transformations on these formats perform better than text, csv and json.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Avoid User Defined Functions(UDF)
# MAGIC UDFs are black box to Spark. Hence it can't apply optimization and you will lose all the optimzation spark does on DataFrame/Dataset.
# MAGIC
# MAGIC When possible you should use Spark SQL built-in functions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. JOIN STRATEGIES HINTS FOR SQL QUERIES[broadcast,shuffle_merge,shuffle_hash,shuffle_replicate_nl]
# MAGIC
# MAGIC Hints are way to override the behaviour of the query optimizer and force it to use a specific join strategy or an index.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Coalesce Hints for SQl Quries
# MAGIC Coalesce hints allow the Spark SQL users to control the number of output files just like the coalesce,repartition,repartitionByRange in Dataset API, they can be used for performance tuning and reducing the number of output files.
# MAGIC
# MAGIC - **"COALESCE"** hint: only has partition number as parameter 
# MAGIC - **"REPARTITION"** hint: has an initial partition number,columns or both/neither as parameter 
# MAGIC - **"REPARTITION_BY_RANGE"** hint: must have column names, partition number is optional 
# MAGIC - **"REBALANCE"** hint: has an initial partition number,columns or both/neither as parameter 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 7. Adaptive Query Execution
# MAGIC
# MAGIC - AQE is an optimization technique in Spark SQL that makes use of **runtime statistics** to choose the most efficient **query execution plan**. AQE is enabled by default since Apache Spark 3.2.0.
# MAGIC
# MAGIC - Spark SQL can turn on and off AQE by `spark.sql.adaptive.enabled` as an umbrella configuration.
# MAGIC
# MAGIC - As of Spark 3.0 there are three mejor features in AQE:
# MAGIC
# MAGIC 1. Coalescing post-shuffle partition
# MAGIC 2. converting sort-merge join to broadcast join
# MAGIC 3. skew join optimization

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 8. Choose right cluster configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9. Delta Optimization Technique
# MAGIC ##### 9.1 Correctly partition the data

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10. File management optimization
# MAGIC ##### 10.1 z-order
# MAGIC ##### 10.2 compacting
