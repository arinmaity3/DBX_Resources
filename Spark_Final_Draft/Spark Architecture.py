# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Spark cluster and Runtime Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC - Spark is a distributed computing platform
# MAGIC - Spark Application itself is a distributed application
# MAGIC - Spark Application needs a cluster
# MAGIC - Cluster technologies for Apache Spark:
# MAGIC 1. Hadoop Yarn (Most Used)
# MAGIC 2. Kubernates  (Most Used)
# MAGIC 3. Apache Mesos
# MAGIC 4. Spark Standalone

# COMMAND ----------

# MAGIC %md
# MAGIC What is a cluster?
# MAGIC - A pool of computers working together but viewed as a single system
# MAGIC
# MAGIC Example of cluster configuration
# MAGIC - Worker Node Capacity:
# MAGIC 16 CPU cores and 64 GB RAM
# MAGIC - Cluster Capacity(Total for 10 worker nodes):
# MAGIC 160 CPU cores and 640GB RAM
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Using "spark-submit" command we submit spark application to the cluster. Then the request will go to "YARN Resource Manager". The YRM will create an "Application Master Container" on a worker node and start my application's main() method in the container.
# MAGIC - Container is an isolated virtual environement, it comes with some CPU and Memory allocation. 
# MAGIC - Resource Manager might not give entire worker memory and cpu cores to the AM container.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Inside Application Master Node:
# MAGIC - Application Master Container is running the main() method of the application. And we have 2 possibilities here:
# MAGIC 1. Pyspark Application
# MAGIC 2. Scala Application
# MAGIC
# MAGIC main() method could be pyspark/scala application. Lets assueme our application is a pyspark application. But spark is written in scala and it runs in java virtual machine.
# MAGIC
# MAGIC - java wrapper is there on top of Spark Core, Python wrapper is there on top of java wrapper. This Python wrapper known as PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC So we have python code in main(). It is desigened to start a java main method internally. Once we have a JVM application, PySpark wrapper will call the Java wrapper using the Py4J connection. 
# MAGIC - Py4J allow a python applcition to call a java application.
# MAGIC - Pyspark main() is called PySpark Driver
# MAGIC - JVM applcation is called Applcition Driver
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Application Driver distributes works to others. This driver does not perform any data processing work. Instead it will create some executors and get the work done from them. After starting it will go to Resource manager and ask for some container. then The resource manger will create some more container worker node and give them to the driver. Now driver will run spark executors in these containers. Each container will run one spark executor. Executor is also JVM application.
# MAGIC
# MAGIC - These executors are resposible for all data processing work.
# MAGIC - The driver will assign work to these executors and manage/monitor them.
# MAGIC - The AM container as well as Executor containers will run on worker nodes 
# MAGIC %md
# MAGIC - If you are using spark dataframe API is scala or jave, runtime architecture will have one jvm driver and one or more jvm executors.
# MAGIC - If you are using pyspark dataframe api, Your runtime architecture will be like:
# MAGIC pyspark driver and application driver(jvm) and jvm executors
# MAGIC -  But if you are using any additional python libraries that are not part of pyspark or you are using udf, then run time architecture:
# MAGIC pyspark driver and application driver(jvm) | jvm executors and python worker
# MAGIC
# MAGIC %md 
# MAGIC Python worker is a python runtime environment. We need them to execute python code/libraries.
