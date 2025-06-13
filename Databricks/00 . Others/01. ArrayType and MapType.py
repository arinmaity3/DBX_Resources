# Databricks notebook source
# MAGIC %md
# MAGIC #### Agenda:
# MAGIC 1. ArrayType
# MAGIC 2. Array
# MAGIC 3. array_contains
# MAGIC - Array: Its used to create a new column by merging the data from multiple columns. All input columns must have the same data type.
# MAGIC - array_contains() : SQL function is used to check if array column contains a value. Returns null if the array contains the value, and false otherwise

# COMMAND ----------

from pyspark.sql.types import StringType,ArrayType,StructType,StructField

# COMMAND ----------

data = [
    ("James,Smith",["ADF","Scala","PySpark"],["PySpark","ADF"],"OH","CA"),
    ("Michael,Rose",["ADF","SQL","PySpark"],["SQL","ADF"],"NY","NJ"),
    ("Robert,Willliam",["SQL","Scala"],["SQL","Scala"],"UT","NV"),    
]

schema = StructType(
    [
        StructField("Name",StringType(),True),
        StructField("Skills",ArrayType(StringType()),True),
        StructField("Work_Profile",ArrayType(StringType()),True),
        StructField("current_state",StringType(),True),
        StructField("prev_state",StringType(),True)
    ]
)

# COMMAND ----------

df = spark.createDataFrame(data,schema)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import array,array_contains,when

# COMMAND ----------

df1 = df.withColumn("States",array(df['current_state'],df['prev_state']))

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.withColumn("contains_ADF",when(array_contains(df['skills'],'ADF'),"Eligible").otherwise("Not Eligible")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Agenda
# MAGIC 1. mapType
# MAGIC 2. map_keys
# MAGIC 3. map_values
# MAGIC 4. explode
# MAGIC
# MAGIC Explode: It is used in pyspark data model to explode an array or map-related columns to row.

# COMMAND ----------

from pyspark.sql.types import MapType

# COMMAND ----------

data = [
    ('India',{"UP":"Lucknow","Bihar":"Patna","MP":"Bhopal","WB":"Kolkata"}),
    ('USA',{"Texas":"Austin","Ohio":"Columbus","NJ":"Trenton"})
]

schema = StructType([
    StructField("Country",StringType(),True),
    StructField("State_Capital",MapType(StringType(),StringType()),True)
])

# COMMAND ----------

country_df = spark.createDataFrame(data,schema)

# COMMAND ----------

country_df.display()

# COMMAND ----------

country_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode,map_keys,map_values

# COMMAND ----------

country_df.select('Country','State_Capital',explode('State_Capital')).display()

# COMMAND ----------

country_df.select('Country','State_Capital',map_keys('State_Capital').alias("keys")).display()

# COMMAND ----------

country_df.select('Country','State_Capital',map_values('State_Capital').alias("values")).display()

# COMMAND ----------

country_df.select('Country','State_Capital',explode(map_values('State_Capital')).alias("exploded_values")).display()

# COMMAND ----------

country_df.select('Country','State_Capital',explode(map_keys('State_Capital')).alias("exploded_keys")).display()

# COMMAND ----------


