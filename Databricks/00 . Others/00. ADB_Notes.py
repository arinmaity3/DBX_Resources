# Databricks notebook source
# MAGIC %md
# MAGIC In Databricks cluster is a set of computational resources. If you have a multi node cluster, then you have a driver node and worker nodes that help us execute tasks.
# MAGIC - All Purpose CLuster : Analyze data collaboratively using interactive notebooks
# MAGIC - Job CLuster : is used to run fast and robust automated jobs

# COMMAND ----------

# MAGIC %md
# MAGIC - Cluster Policy
# MAGIC - Node: Single | Multi
# MAGIC - Access Mode: Single User | Shared | No Isolation Shared
# MAGIC - Databricks runtime version : 12.2 LTS (Scala 2.12, Spark 3.3.2) LTS: Long Term Support
# MAGIC - Photon Acceleration :
# MAGIC - Node Type: Standard_DS3_v2 (14GB Memory, 4 Cores)
# MAGIC - Terminate after : 

# COMMAND ----------

# MAGIC %md
# MAGIC DBFS is a distributed file system mounted on Azure Databricks workspace and its available in Azure Databricks clusters. 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/external-data/

# COMMAND ----------

# MAGIC %md
# MAGIC - StructType:Defines the structure of the dataframe
# MAGIC - StructField: Defines the the metadata of the columns within as dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC DF Write Modes:
# MAGIC - append: Append contents of this DF to the existing data which already exists
# MAGIC - overwrite: Overwrite existing data
# MAGIC - ignore: Siliently ignore if data already exists
# MAGIC - error or errorifexists: Default,Throw an exception if data already exists

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.databricks.com/glossary/what-is-parquet#:~:text=What%20is%20Parquet%3F,handle%20complex%20data%20in%20bulk.

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Live Table: A framework for building reliable, maintainable and testable data processing pipelines built on top of a Delta Lake.
# MAGIC
# MAGIC a Delta Live Table Pipeline is a Directed Acyclic Graph(DAG) linking data sources to target datasets.
