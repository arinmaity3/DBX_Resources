# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data Flow Overviews: Transforming the data before it reaches its final destination
# MAGIC - Mapping Data Flows are visually designed data transformation in ADF. Data Flow allow us to develop data transformation logic without writing code.The resulting data flows are executed as activities within ADF pipelines that use scale-out Apache Spark Clusters. Addiotionally Data Flows run on ADF-managed execution clusters for scaled-out data processing.ADF handles all the code translation,path optimization and execution of data flow jobs.
# MAGIC - Data Flow which we create via **graphical UI** gets converted into **spark source code** and the **source datasets** are converted into **dataframes**. Transformations we apply through datflow performed using **spark operation**.
# MAGIC
# MAGIC - Data Flow must have atleast one source and sink. It can have multiple source.
# MAGIC
# MAGIC
# MAGIC ##### Multiple inputs/outputs:
# MAGIC   - Join | Conditional Split | Exists |Union | Lookup
# MAGIC ##### Schema Modifier:
# MAGIC   - Derived Column | Select | Aggregate | Surrogate Key| Pivot | Unpivot | Window | Rank | External Call | Cast
# MAGIC ##### Formatters:
# MAGIC   - Flatten | Parse | Stringify
# MAGIC ##### Row Modifier:
# MAGIC   - Filter | Sort | Alter Row | Assert
# MAGIC ##### Destination:
# MAGIC   - Sink
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Source types in Data Flow:
# MAGIC Difference b/w Dataset and Inline: Inline let us access data without creating dataset. However they should only be used for testing purposes as they are not reusable. Also inlie is not recommended for laarge amount of data.
# MAGIC
# MAGIC Options:
# MAGIC - Allow Schema Drift
# MAGIC - Infer Drifted column types
# MAGIC - Validate Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Source and Sink Partitioning:
# MAGIC
# MAGIC Partition Option: 1. Use Current partitioning 2. Single Partition 3. Set Partitioning(Round-Robin,hash,Dynamic Range,Fixed Range,Key)
# MAGIC
# MAGIC Partitioning for the source is for paerformance of the dataflow. How data is partitioned and gets processed during the execution.
# MAGIC
# MAGIC Partitioning for sink determines how data is organized and structured at the destination.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select Transformation: 
# MAGIC Use the select transformation to rename,drop or reorder columns. This transformation doesn't alter row data, but chooses which columns are propagated downstream.
# MAGIC
# MAGIC There are two types of mapping available in Select transformation: 1) Fixed Mapping 2) Rules based Maping

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cast Transformation:
# MAGIC Use this transformation for to easily modify the data types of individual column. It also enables an easy way to validate casting errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Derived Column Transformation:
# MAGIC Use this transformation to generate new columns in your data flow or to modify existing fields
# MAGIC ("dd-MMM-yy HH.mm.ss.SS") : 06-JAN-22 09.35.32.00
# MAGIC
# MAGIC toDate()
# MAGIC
# MAGIC toTimestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Filter and Sort Transformation:

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Aggregate Transformation:

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join Transformation:

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Conditional Split and Union Transformation:

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join Transformation:
