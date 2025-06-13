# Databricks notebook source
spark.sparkContext.getConf()

# COMMAND ----------

# if __name__ == "__main__":
#     spark = SparkSession.builder
#     .appName("Hello Spark")
#     .master("local[3]")        
#     .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark is a distributed data processing framework.

# COMMAND ----------

sample_df = spark.read.options(header=True,inferschema=True).csv("/FileStore/SparkCourse/sample.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### DataFrame:
# MAGIC - A 2-Dimensional,table like data structure inspired by pandas dataframe. They are distributed table with named columns and well defined schema.
# MAGIC
# MAGIC - DataFrame is an internally distributed data structure and it is composed of a bunch of partitions. However we can visualize as plain data structure.
# MAGIC
# MAGIC - Spark DataFrame is an immutable data structure

# COMMAND ----------

# MAGIC %md
# MAGIC Spark data processing is all about creating **DAG** of activities.
# MAGIC
# MAGIC 1. **Transformations** : Transform one DF to another without modyfing the original DF
# MAGIC     - **Narrow Dependency Transformation** : A Transformation performed independently on a single partition and still valid results can be produced. Examples: Where
# MAGIC     - **Wide Dependency Transformation**: A transformation that requires data from other partitions in order to produce valid results. Example: groupBy
# MAGIC 2. **Actions**: Operations which requires us to read/write/collect and show data is called Action. Example: Read,Write,Collect,Show,Count

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Shuffle/sort Exchange

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lazy Evalutions:
# MAGIC - This is a Functional programming technique. Spark program doesn't get execute line by line. As because it uses builder pattern to create a DAG of operations. All of the statements go to spark driver and the driver will look at these transfomation, rearrange them to optimize certain activity and finally creates a Execution Plan which will be executed by the executors.
# MAGIC
# MAGIC - In spark statements don't get executed individually. But they are converted into **optimized Execution Plan** which is triggered by an **Action**.
# MAGIC
# MAGIC - Therefore Action will terminate the transformation DAG and triggers the execution.
# MAGIC
# MAGIC - Hence we say transformations are lazy. However Actions are evaluted immidialtely.

# COMMAND ----------

partitioned_df = sample_df.repartition(2)

# COMMAND ----------

#Shiffle/sort caused by groupBy operation will result in 2 partitions
spark.conf.set("spark.sql.shuffle.partition",2)

# COMMAND ----------

filtered_df = partitioned_df.filter("Gender='Male'")
selected_df = filtered_df.select('Country','Gender','Age')
grouped_df = selected_df.groupBy("Country","Gender").avg("Age")
grouped_df.collect()

# COMMAND ----------

grouped_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark will create a DAG for each Job and break it into stages separated by a shuffle operation.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary:
# MAGIC - Each Action will result in a Job
# MAGIC - Each Wide Transformation will result in separate stage.
# MAGIC - Every stage can have one or more tasks based on the partitions. tasks can be executed parallelly

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spark APIs
# MAGIC **[Spark SQL | DataFrame API | Dataset API]** --> **[Catalyst Optimizer]** --> **[RDD APIs]**

# COMMAND ----------

# MAGIC %md
# MAGIC Dataset APIs are language native API in scala and java

# COMMAND ----------

# MAGIC %md
# MAGIC ##### RDD(Resilient Distributed Dataset):
# MAGIC - Similar to DataFrame. As DF is built on top of RDD. However unlike DF, RDD records are language native objects and they don't have row-column structure and schema.
# MAGIC
# MAGIC - RDDs are resilient. That means they are fault-tolerant.As they also store information about how they are created. 
# MAGIC
# MAGIC Lets assume a RDD core has been assigned to executor node for processing. In sometime executor node crashes, then driver will assign that RDD core to another executor node for processing.
# MAGIC
# MAGIC Therefore RDD core can be recreated and reprocessed.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Analysis:** In this phase Spark SQL engine will read your code and generate an abstract syntax tree for sql/df code. In this phase your code is analyzed and column names/tables/sql functions are resolved. You might get runtime error if your names don't get resolved.
# MAGIC
# MAGIC 2. **Logical Optimization:** In this phase SQL engine will apply **rule based optimization** and consturct set of multiple execution plans. Then catalyst optimzer will use cost based optimization to assign cost to each plans.
# MAGIC 3. **Physical Planning :** In this phase SQL engine will pick most effective logical plan and generate a physical plan. Physical plan is basically a set of RDD operations which determine how the plan will get executed on the Spark cluster.
# MAGIC 4. **Code Generation**: Generating efficient java byte code based on physical plan

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How to register UDF:
# MAGIC - For DF: 
# MAGIC `from pyspark.sql.functions import udf`
# MAGIC `function_name_udf = udf(function_name,StringType())` udf() does not register the function in spark catlog. It will not be available in `spark.catalog.listFunctions()`
# MAGIC - For SQL: `spark.udf.register("function_name_udf","function_name",Stringtype())`. Through this way function will be registered in spark catlog. It will be available in `spark.catalog.listFunctions()`

# COMMAND ----------

for each_function in spark.catalog.listFunctions():
    display(each_function.name)

# COMMAND ----------


