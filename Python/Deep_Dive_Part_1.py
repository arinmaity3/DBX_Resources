# Databricks notebook source
a = [1,2,3]

# COMMAND ----------

a = [1,
     2,
     3,4]

# COMMAND ----------

a = [1 #item1
     ,2]

# COMMAND ----------

a,b,c = 1,2,3

# COMMAND ----------

if a < b 
and b < c:
    print("True")

# COMMAND ----------

if a < b \
and \
b < c:
    print("True")

# COMMAND ----------

a  ="Random Multiline
Setence"

# COMMAND ----------

a  ='''Random Multiline
Sentence'''

# COMMAND ----------

print(a)

# COMMAND ----------

a

# COMMAND ----------

x=5
y =6

# COMMAND ----------

x.__lt__(y)

# COMMAND ----------

x.__gt__(y)

# COMMAND ----------

x =100

# COMMAND ----------

if x ==100:
    print("Hundred")
else:
    print("Not Hundred")

# COMMAND ----------

x=101

# COMMAND ----------

output = "Hundred" if x==100 else "Not Hundred"
print(output)

# COMMAND ----------

a=[1,2,3]

# COMMAND ----------

type(a.sort)

# COMMAND ----------

type(list)

# COMMAND ----------

list.__dict__

# COMMAND ----------

type.__dict__

# COMMAND ----------

type(type)

# COMMAND ----------

a= [1,2,3]

# COMMAND ----------

len(a)

# COMMAND ----------

type(len)

# COMMAND ----------

from math import sqrt

# COMMAND ----------

sqrt(4)

# COMMAND ----------

4**0.5

# COMMAND ----------

def func1():
    print("something")

# COMMAND ----------

hex(id(func1))

# COMMAND ----------

type(func1)

# COMMAND ----------

func1()

# COMMAND ----------

def func2(a:int,b:int)->int:
    return a+b

# COMMAND ----------

func2("a","b")

# COMMAND ----------

func2(100,200)

# COMMAND ----------

def new_func1():
    new_func2()

# COMMAND ----------

def new_func2():
    print("running new_func2")

# COMMAND ----------

new_func1()

# COMMAND ----------

power = lambda x,y : x**y

# COMMAND ----------

power(100,3)

# COMMAND ----------

type(power)

# COMMAND ----------

power

# COMMAND ----------

i =100
while i >0:
    print(i)
    i -= 10

# COMMAND ----------

#similar approach as "do-while"
i =100
while True:
    print(i)
    if i >=100:
        break
    i += 10

# COMMAND ----------

help("a".isprintable)

# COMMAND ----------

name = input("Please enter your name: ")
while not(name.isprintable() and len(name)>2 and name.isalpha()):
    name = input("Please enter your name: ")

print(f"Your name is {name}")


# COMMAND ----------

while True:
    name = input("Please enter your name: ")
    if (name.isprintable() and len(name)>2 and name.isalpha()):
        break
print(f"Your name is {name}")


# COMMAND ----------

for i in range(30):
    if i %5 ==0 or i%2==0:
        continue
    print(i)

# COMMAND ----------

a = [1,2,3,4,5]
b =5
idx = 0
while idx <len(a):
    if a[idx] == b:
        break
    idx += 1
else:
    a.append(b)

print(a)

# COMMAND ----------

a=100
b =0
while True:
    try:
        a/b
    except ZeroDivisionError as e:
        print(e)
    finally:
        print("Always gets executed")
    print("main loop")
    break

# COMMAND ----------

a ="Arin Maity"

# COMMAND ----------

b =enumerate(a)

# COMMAND ----------

for each in b:
    print(each)

# COMMAND ----------

for i,ch in enumerate(a):
    print(i,ch)

# COMMAND ----------

x= [1,2,3]

# COMMAND ----------

list(map(lambda x : x*x, x))

# COMMAND ----------

a_list = [1,2,3,4,5]

# COMMAND ----------

b_list = [10,20,30]

# COMMAND ----------

list(map(lambda x,y:x+y,a_list,b_list))

# COMMAND ----------

l = [1,2,3,0,4]

# COMMAND ----------

list(filter(None,l))

# COMMAND ----------

list(filter(lambda x:x %2 == 0,l))

# COMMAND ----------

list(zip(a_list,b_list))

# COMMAND ----------

l = [2,3,4]

# COMMAND ----------

[x*x for x in l]

# COMMAND ----------

[x+y for x,y in zip(a_list,b_list)]

# COMMAND ----------

a = list(range(50))

# COMMAND ----------

[x for x in a if x%5==0]

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

schema = StructType([
    StructField("Name",StringType(),True),
    StructField("Location",StringType(),True),
    StructField("MLevel",IntegerType(),True)
]
)

# COMMAND ----------

data = [("Arin","Kolkata",10),("Deepak","Delhi",7),("Veena","Chennai",8)]

# COMMAND ----------

acc_df = spark.createDataFrame(data,schema)

# COMMAND ----------

acc_df.show()

# COMMAND ----------

acc_df.dtypes

# COMMAND ----------

type(acc_df.dtypes)

# COMMAND ----------

dict(acc_df.dtypes)

# COMMAND ----------

tgtDataTypeMap = {k.lower():v for k,v in dict(acc_df.dtypes).items()}

# COMMAND ----------

tgtDataTypeMap

# COMMAND ----------

print("som.{0}..{2}..{1}".format(100,200,300))

# COMMAND ----------

acc_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

acc_df.select(col("MLevel").cast(StringType())).show()

# COMMAND ----------

dbutils.widgets.text("input","")

# COMMAND ----------

i = dbutils.widgets.get("input")

# COMMAND ----------

i

# COMMAND ----------

j = i.strip().replace("\\n","")

# COMMAND ----------

j

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark_session = SparkSession.getActiveSession()

# COMMAND ----------

spark.conf.getAll()

# COMMAND ----------

from databricks import get_runtime

# COMMAND ----------

from pyspark.sql import Row

# Sample data for the first DataFrame
data_1 = [
    Row(id=1, name='Alice', age=25),
    Row(id=2, name='Bob', age=30),
    Row(id=3, name='Charlie', age=35),
    Row(id=4, name='David', age=27),
    Row(id=5, name='Eva', age=290)
]

# Create a DataFrame from the sample data for the first DataFrame
df_1 = spark.createDataFrame(data_1)

# Sample data for the second DataFrame
data_2 = [
    Row(id=1, name='Alice', age=25),
    Row(id=3, name='Charlie', age=35),
    Row(id=4, name='David', age=27),
    Row(id=5, name='Eva', age=29),
    Row(id=6, name='Evan', age=65)
]

# Create a DataFrame from the sample data for the second DataFrame
df_2 = spark.createDataFrame(data_2)



# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df_2_minus_df_1 = df_2.exceptAll(df_1).withColumn("table",lit("current"))

# COMMAND ----------

df_1_minus_df_2 = df_1.exceptAll(df_2).withColumn("table",lit("history"))

# COMMAND ----------

df_1_minus_df_2.union(df_2_minus_df_1).show()

# COMMAND ----------


