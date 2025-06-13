# Databricks notebook source
# MAGIC %md
# MAGIC #### 1.Broadcast Variable

# COMMAND ----------

# MAGIC %md
# MAGIC - A broadcast variable in Spark is a **read-only** variable that is cached on all the nodes in a cluster. This means that all the tasks on the cluster can access the broadcast variable without having to send it over the network.
# MAGIC
# MAGIC - Broadcast variables are useful for storing small to medium-sized data that needs to be accessed by all the tasks in a cluster. For example, you could use a broadcast variable to store a lookup table or a list of parameters.
# MAGIC
# MAGIC - To create a broadcast variable, you use the broadcast() method of the SparkContext class. This method takes the variable that you want to broadcast as an argument and returns a Broadcast object.
# MAGIC
# MAGIC - Once you have created a broadcast variable, you can access it from any task in your Spark application using the value property of the Broadcast object.
# MAGIC
# MAGIC - Broadcast variables can be a great way to improve the performance of your Spark applications by reducing the amount of data that needs to be sent over the network.
# MAGIC

# COMMAND ----------

from pyspark import SparkContext

sc = SparkContext()

# Create a broadcast variable
lookup_table = {"CA": "California", "NY": "New York", "TX": "Texas"}
broadcast_lookup_table = sc.broadcast(lookup_table)

# Access the broadcast variable from a task
def map_fn(partition):
  for record in partition:
    state = broadcast_lookup_table.value[record["state"]]
    yield (record["name"], state)

# Apply the map function to the RDD
rdd = sc.parallelize([{"name": "Alice", "state": "CA"}, {"name": "Bob", "state": "NY"}, {"name": "Carol", "state": "TX"}])
mapped_rdd = rdd.map(map_fn)

# Collect the results
results = mapped_rdd.collect()

# Print the results
for result in results:
  print(result)


# COMMAND ----------

# MAGIC %md
# MAGIC Output= ('Alice', 'California')
# MAGIC ('Bob', 'New York')
# MAGIC ('Carol', 'Texas')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Accumulator

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - An accumulator in Spark is a variable that can only be added to through an associative and commutative operation. This means that the order in which values are added to the accumulator does not matter.
# MAGIC
# MAGIC - Accumulators are useful for aggregating data across all the tasks in a Spark application. For example, you could use an accumulator to count the number of records in an RDD, or to calculate the sum or average of a column in an RDD.
# MAGIC
# MAGIC - To create an accumulator, you use the accumulator() method of the SparkContext class. This method takes the initial value of the accumulator and a function that implements the associative and commutative operation as arguments.
# MAGIC
# MAGIC - Once you have created an accumulator, you can add values to it from any task in your Spark application using the += operator.
# MAGIC
# MAGIC - To get the value of the accumulator, you need to call the value property of the accumulator object. However, you can only call the value property from the driver program.
# MAGIC
# MAGIC - Accumulators can be a great way to aggregate data across all the tasks in a Spark application without having to send the data over the network to the driver program. This can significantly improve the performance of your Spark applications.

# COMMAND ----------

from pyspark import SparkContext

sc = SparkContext()

# Create an accumulator to count the number of records in an RDD
count_accumulator = sc.accumulator(0)

# Define a function to add a value to the accumulator
def add_to_count_accumulator(value):
  count_accumulator += 1

# Apply the function to the RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd.foreach(add_to_count_accumulator)

# Get the value of the accumulator
count = count_accumulator.value

# Print the count
print(count)

# COMMAND ----------

# MAGIC %md
# MAGIC Output:
# MAGIC
# MAGIC 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Persistence in Spark

# COMMAND ----------

# MAGIC %md
# MAGIC Persistence in Spark is the process of storing RDDs, DataFrames, and Datasets in memory or on disk. This allows Spark to reuse the data across multiple operations, which can significantly improve performance.
# MAGIC
# MAGIC To persist an RDD, DataFrame, or Dataset, you use the persist() method. This method takes a storage level as an argument, which specifies how and where to store the data.
# MAGIC
# MAGIC There are two types of storage levels in Spark:
# MAGIC
# MAGIC Memory only: This stores the data in memory, which is the fastest storage level, but it can also be the most memory-intensive.
# MAGIC Memory and disk: This stores the data in memory and on disk. This storage level is a good compromise between speed and memory usage.
# MAGIC You can also specify a replication factor for persisted data. This specifies how many copies of each partition of the data should be stored.
# MAGIC
# MAGIC Once you have persisted data, you can access it from any operation in your Spark application. Spark will automatically load the data from memory or disk as needed.
# MAGIC
# MAGIC
# MAGIC Persistence can be a very useful technique for improving the performance of Spark applications. However, it is important to use it wisely, as it can also lead to memory problems if not used correctly.
# MAGIC
# MAGIC Here are some tips for using persistence effectively:
# MAGIC
# MAGIC Only persist data that you need to reuse across multiple operations.
# MAGIC Use the appropriate storage level for your needs.
# MAGIC Unpersist data when you are finished with it.
# MAGIC By following these tips, you can use persistence to improve the performance of your Spark applications without sacrificing memory efficiency.
