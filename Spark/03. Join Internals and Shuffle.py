# Databricks notebook source
# MAGIC %md
# MAGIC 1. Shuffle Sort Merge Join : Each executor will map the records using joining key and send it to the MAP exchange. In this first stage all records are identified by keys and made availabe in this exchange. MAP exchange is like record buffer at the executor. Now spark framework will pick these records and send these to the Reduce Exchange. Reduce exhange is going to collect records for the same keys. These are known as shuffle partitions.
# MAGIC 2. Broadcast Hash Join

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Two scenarios for joining:
# MAGIC 1. large to large : Shuffle Join
# MAGIC 2. large to small : Broadcast Join
# MAGIC
# MAGIC We can consider small when dataframe can fit into single executor.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Shuffle Join

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls "dbfs:/FileStore"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bucket Joins

# COMMAND ----------

display(dbutils.fs.mounts())
