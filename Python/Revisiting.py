# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Dictionary

# COMMAND ----------

my_dict = {'banana': 3, 'apple': 1, 'orange': 2, 'grape': 4}

# COMMAND ----------

#Sorting Dictionary based on keys
my_dict.items()

# COMMAND ----------

sorted(my_dict.items())

# COMMAND ----------

dict(sorted(my_dict.items()))

# COMMAND ----------

#based on Values
dict(sorted(my_dict.items(),key=lambda d:d[1]))

# COMMAND ----------

#Merging two dictionaries
dict1 = {1:10,2:20,3:30}
dict2 = {2:200,4:200,5:500}

# COMMAND ----------

dict1.update(dict2)

# COMMAND ----------

dict1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. lamda

# COMMAND ----------

a = ["a","b","c","d"]

# COMMAND ----------

list(map(lambda i:i+"1",a))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Counter

# COMMAND ----------

a = [1,2,2,3,1,1,4,5]

# COMMAND ----------

from collections import Counter

# COMMAND ----------

Counter(a)

# COMMAND ----------

b = Counter(a)

# COMMAND ----------

b.most_common(2)

# COMMAND ----------

dict(b)
