# Databricks notebook source
# MAGIC %md
# MAGIC **Serialization** is the process of converting the state of an object to a byte stream. **Deserialization** is the reverse of serialization and converts the byte stream back to the original object.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Broadcast Variable: 
# MAGIC A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner.
# MAGIC - Shared and immutable dataset
# MAGIC - Serialized only once per worker node
# MAGIC - Lazy Serialization
# MAGIC - Must fit in the task memory

# COMMAND ----------

transaction = [
    (1,'cosmetic',200),(2,'shoe',2000),(3,'shirt',1000),(4,'Cookies',50)
]

# COMMAND ----------

schema = "store_id Integer,item String, price Integer"

# COMMAND ----------

transaction_df = spark.createDataFrame(transaction,schema)

# COMMAND ----------

transaction_df.display()

# COMMAND ----------

#Create Dimension Table
store = [
    (1,'store_paris'),
    (2,'store_italy'),
    (3,'store_london'),
    (4,'store_NewYork'),
]

store_schema = "store_id Integer,store_name String"

# COMMAND ----------

store_df = spark.createDataFrame(store,store_schema)

# COMMAND ----------

store_df.display()

# COMMAND ----------

from pyspark.sql.functions import broadcast 

# COMMAND ----------

joined_df = transaction_df.join(store_df,transaction_df['store_id']==store_df['store_id'])

# COMMAND ----------

joined_df.explain()

# COMMAND ----------

broadcast_joined_df = transaction_df.join(broadcast(store_df),transaction_df['store_id']==store_df['store_id'])

# COMMAND ----------

broadcast_joined_df.explain()

# COMMAND ----------

store_dict = {1:'store_paris',2:'store_italy',3:'store_london',4:'store_NewYork'}

# COMMAND ----------

bd_data = sc.broadcast(store_dict)

# COMMAND ----------

bd_data.value.get(1)

# COMMAND ----------

def get_store_name(code:str) -> str:
    return bd_data.value.get(code)
    

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import expr

# COMMAND ----------

spark.udf.register("udf_func",get_store_name,StringType())

# COMMAND ----------

udf_func = udf(get_store_name,StringType())

# COMMAND ----------

#new_df= transaction_df.withColumn("store_name",expr("udf_func(store_id)"))
new_df= transaction_df.withColumn("store_name",udf_func(transaction_df['store_id']))

# COMMAND ----------

new_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Accumulator:
# MAGIC - Global mutable variable
# MAGIC - Can update them row basis
# MAGIC - can be used for counters and sum

# COMMAND ----------

shipment_data =[
    ('India','China',5),('India','Japan',7),('USA','India','nine'),('China','India','one')
]

# COMMAND ----------

shipment_df= spark.createDataFrame(shipment_data,("source","destination","id"))

# COMMAND ----------

shipment_df.show()

# COMMAND ----------

bad_rec_accumulator = sc.accumulator(0)

# COMMAND ----------

def handle_bad_rec(id):
    try:
        id = int(id)
    except ValueError:
        bad_rec_accumulator.add(1)
    return id

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

handle_bad_rec_udf = udf(handle_bad_rec,IntegerType())


# COMMAND ----------

processed_df = shipment_df.withColumn("shipment_int_id",handle_bad_rec_udf(shipment_df['id']))

# COMMAND ----------

processed_df.display()

# COMMAND ----------

#processed_df.filter(expr("shipment_int_id is null")).count()

# COMMAND ----------

bad_rec_accumulator

# COMMAND ----------

# MAGIC %md
# MAGIC - Accumulator variable maintained at the driver, therefore we don't have to collect from worker nodes
# MAGIC - We can increment accumulators from 1)Transformation as well as 2)Actions
# MAGIC However it is recommended use accumulator inside the Action as spark guraantees Accuracy in this case.

# COMMAND ----------

shipment_df.Seqno

# COMMAND ----------

shipment_df.forEach(bad_rec_)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Speculative Execution

# COMMAND ----------

#Default value is false
spark.conf.get("spark.speculation")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Dataframe Hints:
# MAGIC 1)  Partioning Hints
# MAGIC     - COALESCE
# MAGIC     - REPARTITION
# MAGIC     - REPARTITION_BY_RANGE
# MAGIC     - REBALANCE
# MAGIC 2) Joining Hints
# MAGIC     - BROADCAST aliaS BROADCASTJOIN and MAPJOIN
# MAGIC     - MERGE alias SHUFFLE_MERGE and MERGEJOIN
# MAGIC     - SHUFFLE_HASH
# MAGIC     - SHUFFLE_REPLICATE_NL

# COMMAND ----------

# MAGIC %md
# MAGIC spark.sql.optimizer.dynamicPartitionPruning.enabled
# MAGIC - Default value is true

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Predicate Pushdown
# MAGIC 2. Partition Pruning

# COMMAND ----------


