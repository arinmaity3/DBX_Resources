# Databricks notebook source
# MAGIC %md
# MAGIC https://www.machinelearningplus.com/pyspark/pyspark-exercises-101-pyspark-exercises-for-data-analysis/

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. How to import PySpark and check the version?

# COMMAND ----------

spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. How to convert the index of a PySpark DataFrame into a column?

# COMMAND ----------

df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

# COMMAND ----------

df.show()

# COMMAND ----------

display(df.collect())

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql.functions import row_number

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window_spec = Window.partitionBy().orderBy("value")

# COMMAND ----------

df = df.withColumn("row",row_number().over(window_spec))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. How to combine many lists to form a PySpark DataFrame?
# MAGIC

# COMMAND ----------

list1 = ["a", "b", "c", "d"]

# COMMAND ----------

list2 = [1, 2, 3, 4]

# COMMAND ----------

rdd = spark.sparkContext.parallelize(list(zip(list1, list2)))

# COMMAND ----------

rdd.top(5)

# COMMAND ----------

df = rdd.toDF(["Column1", "Column2"])

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. How to get the items of list A not present in list B?

# COMMAND ----------

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]

# COMMAND ----------

rdd_A = spark.sparkContext.parallelize(list_A)

# COMMAND ----------

rdd_B = spark.sparkContext.parallelize(list_B)

# COMMAND ----------

result_rdd = rdd_A.subtract(rdd_B)

# COMMAND ----------

result_list = result_rdd.collect()

# COMMAND ----------

result_list

# COMMAND ----------

# MAGIC %md
# MAGIC 5. How to get the items not common to both list A and list B?

# COMMAND ----------

rdd_A.take(5)

# COMMAND ----------

rdd_B.take(5)

# COMMAND ----------

result_rdd_A = rdd_A.subtract(rdd_B)

# COMMAND ----------

result_rdd_B = rdd_B.subtract(rdd_A)

# COMMAND ----------

rdd_union_result = result_rdd_A.union(result_rdd_B)

# COMMAND ----------

rdd_union_result.top(5)

# COMMAND ----------

rdd_intersection = rdd_A.intersection(rdd_B)

# COMMAND ----------

rdd_intersection.top(5)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. How to get the minimum, 25th percentile, median, 75th, and max of a numeric column?

# COMMAND ----------

data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()()

# COMMAND ----------

from pyspark.sql.functions import max,min

# COMMAND ----------

df.select(max(df['Age'])).show()

# COMMAND ----------

df.agg(max('Age'),min('Age')).show()

# COMMAND ----------

df.approxQuantile("Age", [0.0, 0.25, 0.5, 0.75, 1.0],0.1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. How to get frequency counts of unique items of a column?

# COMMAND ----------

from pyspark.sql import Row

# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

# create DataFrame
df = spark.createDataFrame(data)

# show DataFrame
df.show()

# COMMAND ----------

df.groupBy('job').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC 8. How to keep only top 2 most frequent values as it is and replace everything else as ‘Other’?

# COMMAND ----------

from pyspark.sql.functions import desc,col


# COMMAND ----------

top_2_jobs = df.groupBy('job').count().sort(col('count').desc()).limit(2).select('job').collect()


# COMMAND ----------

display(top_2_jobs)

# COMMAND ----------

print([r.job for r in top_2_jobs])

# COMMAND ----------

from pyspark.sql.functions import when


# COMMAND ----------

df.withColumn("job",when(df['job'].isin([r.job for r in top_2_jobs]),df['job']).otherwise("other")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9. How to Drop rows with NA values specific to a particular column?

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

data = [
    Row(name='A',value=1,id=None),
    Row(name='B',value=None,id=123),
    Row(name='B',value=3,id=456),
    Row(name='D',value=None,id=None),
]

# COMMAND ----------

df1 = spark.createDataFrame(data)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.dropna().display()

# COMMAND ----------

df1.dropna(subset=['id','value']).display()

# COMMAND ----------

df1.dropna(subset=['id']).display()

# COMMAND ----------

df1.dropna(subset=['value']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 10. How to rename columns of a PySpark DataFrame using two lists – one containing the old column names and the other containing the new column names?
# MAGIC

# COMMAND ----------

df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["col1", "col2", "col3"])
old_names = ["col1", "col2", "col3"]
new_names = ["new_col1", "new_col2", "new_col3"]

# COMMAND ----------

df.columns

# COMMAND ----------

for old,new in zip(df.columns,new_names):
    df=df.withColumnRenamed(old,new)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 11. How to bin a numeric list to 10 groups of equal size?

# COMMAND ----------

data = [("A", "X"), ("A", "Y"), ("A", "X"), ("B", "Y"), ("B", "X"), ("C", "X"), ("C", "X"), ("C", "Y")]
df = spark.createDataFrame(data, ["category1", "category2"])

df.show()

# COMMAND ----------

df.cube('category1').count().show()

# COMMAND ----------

df.cube()

# COMMAND ----------

df.crosstab('category1', 'category2').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 13. How to find the numbers that are multiples of 3 from a column?

# COMMAND ----------

from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)
# # Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

# # Show the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

df.filter(df['random'] % 3 ==0).display()

# COMMAND ----------

df.filter(expr('random % 3 = 0')).display()

# COMMAND ----------

df.withColumn("is_multiple_3",expr("case when random % 3 = 0 then 1 else 0 end")).display()

# COMMAND ----------

df.withColumn("is_multiple_3",when(df['random'] % 3 == 0,1).otherwise(0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 14. How to extract items at given positions from a column?

# COMMAND ----------

from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)

# Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

# Show the DataFrame
df.show()

pos = [0, 4, 8, 5]

# COMMAND ----------

df.filter(df['id'].isin(pos)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 15. How to stack two DataFrames vertically ?

# COMMAND ----------

# Create DataFrame for region A
df_A = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 10), ("orange", 2, 8)], ["Name", "Col_1", "Col_2"])
df_A.show()

# Create DataFrame for region B
df_B = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 15), ("grape", 4, 6)], ["Name", "Col_1", "Col_3"])
df_B.show()

# COMMAND ----------

df_A.union(df_B).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 16. How to compute the mean squared error on a truth and predicted columns?

# COMMAND ----------

# MAGIC %md
# MAGIC 17. How to convert the first character of each element in a series to uppercase?

# COMMAND ----------

# Suppose you have the following DataFrame
data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import initcap

# COMMAND ----------

df = df.withColumn("name",initcap(df['name']))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 18. How to compute summary statistics for all columns in a dataframe

# COMMAND ----------

# For the sake of example, we'll create a sample DataFrame
data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]

df = spark.createDataFrame(data, ["name", "age" , "salary"])

df.show()

# COMMAND ----------

df.summary().show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC 19. How to calculate the number of characters in each word in a column?

# COMMAND ----------

data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import length

# COMMAND ----------

df.withColumn("name_length", length(df['name'])).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 20 How to compute difference of differences between consecutive numbers of a column?

# COMMAND ----------

data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]

df = spark.createDataFrame(data, ["name", "age" , "salary"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df = df.withColumn("idx", monotonically_increasing_id())

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import row_number,asc

# COMMAND ----------

win_spec = Window.partitionBy().orderBy(df['idx'].asc())

# COMMAND ----------

df = df.withColumn("id",row_number().over(win_spec))

# COMMAND ----------

from pyspark.sql.functions import lead

# COMMAND ----------

df = df.withColumn("salary_lead",lead('salary').over(win_spec))

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.dropna(subset=['salary_lead'])

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import abs

# COMMAND ----------

df.withColumn("differ",abs(df['salary'] - df['salary_lead'])).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 21. How to get the day of month, week number, day of year and day of week from a date strings?

# COMMAND ----------

data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import dayofmonth,dayofweek,dayofyear,year,month

# COMMAND ----------

df.withColumn("d_o_month", dayofmonth(df['date_str_1']))\
    .withColumn("d_o_week", dayofweek(df['date_str_1']))\
        .withColumn("d_o_year", dayofyear(df['date_str_1']))\
            .withColumn("year", year(df['date_str_1']))\
                .withColumn("month", month(df['date_str_1']))\
    .show()

# COMMAND ----------

from pyspark.sql.functions import to_date

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("date_1",to_date(df['date_str_1'],'yyyy-MM-dd'))\
    .withColumn("date_2",to_date(df['date_str_2'],'dd MMM yyyy'))

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 22. How to convert year-month string to dates corresponding to the 4th day of the month?

# COMMAND ----------

df = spark.createDataFrame([('Jan 2010',), ('Feb 2011',), ('Mar 2012',)], ['MonthYear'])

df.show()

# COMMAND ----------

df.withColumn("in_date_format",to_date(df['MonthYear'],'MMM yyyy')).show()

# COMMAND ----------

df = df.withColumn("in_date_format",to_date(df['MonthYear'],'MMM yyyy'))

# COMMAND ----------

from pyspark.sql.functions import date_add

# COMMAND ----------

df.withColumn("4th_day_of_month",date_add(df['in_date_format'],3)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 23 How to filter words that contain atleast 2 vowels from a series?

# COMMAND ----------

# example dataframe
df = spark.createDataFrame([('Apple',), ('Orange',), ('Plan',) , ('Python',) , ('Money',)], ['Word'])

df.show()

# COMMAND ----------

from pyspark.sql.functions import translate,length

# COMMAND ----------

df=df.withColumn("without_vowel",translate(df['word'],'AEIOUaeiou',''))

# COMMAND ----------

df.filter(length(df['word']) - length(df['without_vowel']) >=2 ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 24. How to filter valid emails from a list?

# COMMAND ----------

data = ['buying books at amazom.com', 'rameses@egypt.com', 'matt@t.co', 'narendra@modi.com']

# Convert the list to DataFrame
df = spark.createDataFrame(data, "string")
df.show(truncate =False)

# COMMAND ----------

# MAGIC %md
# MAGIC Not Solved... Need to check

# COMMAND ----------

# MAGIC %md
# MAGIC 25. How to Pivot PySpark DataFrame?

# COMMAND ----------

# MAGIC %md
# MAGIC 26. How to get the mean of a variable grouped by another variable?

# COMMAND ----------

data = [("1001", "Laptop", 1000),
("1002", "Mouse", 50),
("1003", "Laptop", 1200),
("1004", "Mouse", 30),
("1005", "Smartphone", 700)]

# Create DataFrame
columns = ["OrderID", "Product", "Price"]
df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

from pyspark.sql.functions import avg

# COMMAND ----------

df.groupBy('Product').avg('Price').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 27. How to compute the euclidean distance between two columns?

# COMMAND ----------

data = [(1, 10), (2, 9), (3, 8), (4, 7), (5, 6), (6, 5), (7, 4), (8, 3), (9, 2), (10, 1)]

# Convert list to DataFrame
df = spark.createDataFrame(data, ["series1", "series2"])

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 28. How to replace missing spaces in a string with the least frequent character?

# COMMAND ----------

df = spark.createDataFrame([('dbc deb abed gade',),], ["string"])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 29. How to create a TimeSeries starting ‘2000-01-01’ and 10 weekends (saturdays) after that having random numbers as values?

# COMMAND ----------

# MAGIC %md
# MAGIC 30. How to get the nrows, ncolumns, datatype of a dataframe?

# COMMAND ----------

# For number of rows
nrows = df.count()
print("Number of Rows: ", nrows)

# For number of columns
ncols = len(df.columns)
print("Number of Columns: ", ncols)

# For data types of each column
datatypes = df.dtypes
print("Data types: ", datatypes)

# COMMAND ----------

# MAGIC %md
# MAGIC 31. How to rename a specific columns in a dataframe?

# COMMAND ----------

# Suppose you have the following DataFrame
df = spark.createDataFrame([('Alice', 1, 30),('Bob', 2, 35)], ["name", "age", "qty"])

df.show()

# Rename lists for specific columns
old_names = ["qty", "age"]
new_names = ["user_qty", "user_age"]

# COMMAND ----------

for old,new in zip(old_names,new_names):
    df = df.withColumnRenamed(old,new)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 32. How to check if a dataframe has any missing values and count of missing values in each column?

# COMMAND ----------

df = spark.createDataFrame([
("A", 1, None),
("B", None, "123" ),
("B", 3, "456"),
("D", None, None),
], ["Name", "Value", "id"])

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 33 How to replace missing values of multiple numeric columns with the mean?

# COMMAND ----------

# MAGIC %md
# MAGIC 34. How to change the order of columns of a dataframe?

# COMMAND ----------

# Sample data
data = [("John", "Doe", 30), ("Jane", "Doe", 25), ("Alice", "Smith", 22)]

# Create DataFrame from the data
df = spark.createDataFrame(data, ["First_Name", "Last_Name", "Age"])

# Show the DataFrame
df.show()

# COMMAND ----------

new_order = ["Age", "First_Name", "Last_Name"]

# COMMAND ----------

df.select(*new_order).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 35. How to format or suppress scientific notations in a PySpark DataFrame?

# COMMAND ----------

# MAGIC %md
# MAGIC #### 36. How to format all the values in a dataframe as percentages?

# COMMAND ----------

# Sample data
data = [(0.1, .08), (0.2, .06), (0.33, .02)]
df = spark.createDataFrame(data, ["numbers_1", "numbers_2"])

df.show()

# COMMAND ----------

#import pyspark.sql.functions as f
#dir(f)

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat0

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(concat((col('numbers_1')*100).cast('decimal(10,2)'),lit('%')).alias('number_1_percent'),concat((col('numbers_2')*100).cast('decimal(10,2)'),lit('%')).alias('number_2_percent')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 37. How to filter every nth row in a dataframe?

# COMMAND ----------

# Sample data
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("Dave", 4), ("Eve", 5),
("Frank", 6), ("Grace", 7), ("Hannah", 8), ("Igor", 9), ("Jack", 10)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Number"])

df.show()

# COMMAND ----------

df.filter('number=1').show()

# COMMAND ----------

df.filter(df['number']==1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 38 How to get the row number of the nth largest value in a column?

# COMMAND ----------

from pyspark.sql import Row

# Sample Data
data = [
Row(id=1, column1=5),
Row(id=2, column1=8),
Row(id=3, column1=12),
Row(id=4, column1=1),
Row(id=5, column1=15),
Row(id=6, column1=7),
]

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import rank

# COMMAND ----------

win_spec = Window.orderBy(col('column1').desc())

# COMMAND ----------

df = df.withColumn("rank",rank().over(win_spec))

# COMMAND ----------

df.filter('rank=4').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 39. How to get the last n rows of a dataframe with row sum > 100?

# COMMAND ----------

# Sample data
data = [(10, 25, 70),
(40, 5, 20),
(70, 80, 100),
(10, 2, 60),
(40, 50, 20)]

# Create DataFrame
df = spark.createDataFrame(data, ["col1", "col2", "col3"])

# Display original DataFrame
df.show()

# COMMAND ----------

display(df.filter((df['col1']+df['col2']+df['col3'])>100).tail(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 40. How to create a column containing the minimum by maximum of each row?

# COMMAND ----------

# Sample Data
data = [(1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12)]

# Create DataFrame
df = spark.createDataFrame(data, ["col1", "col2", "col3"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import col,least,greatest

# COMMAND ----------

df.withColumn("min_value",least(col("col1"), col("col2"), col("col3")))\
    .withColumn("max_value",greatest(col("col1"), col("col2"), col("col3")))\
        .withColumn("min_by_max",(col('min_value')/col('max_value')).cast('decimal(10,2)'))\
        .show()

# COMMAND ----------

from pyspark.sql.functions import udf, array
from pyspark.sql.types import FloatType

# Define UDF
def min_max_ratio(row):
    return float(min(row)) / max(row)

min_max_ratio_udf = udf(min_max_ratio, FloatType())

# Apply UDF to create new column
df = df.withColumn('min_by_max', min_max_ratio_udf(array(df.columns)))

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 41. How to create a column that contains the penultimate value in each row?

# COMMAND ----------

data = [(10, 20, 30),
(40, 60, 50),
(80, 70, 90)]

df = spark.createDataFrame(data, ["Column1", "Column2", "Column3"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import udf,array

# COMMAND ----------

from pyspark.sql.types import ArrayType,IntegerType

# COMMAND ----------

# Define UDF to sort array in descending order
sort_array_desc = udf(lambda arr: sorted(arr), ArrayType(IntegerType()))


# COMMAND ----------

df.withColumn("row_as_array", sort_array_desc(array(df.columns))).show()

# COMMAND ----------

df = df.withColumn("row_as_array", sort_array_desc(array(df.columns)))

# COMMAND ----------

df.withColumn("Penultimate",df['row_as_array'][1]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 42. How to normalize all columns in a dataframe?
# MAGIC - Normalize all columns of df by subtracting the column mean and divide by standard deviation.
# MAGIC
# MAGIC Range all columns of df such that the minimum value in each column is 0 and max is 1.

# COMMAND ----------

# create a sample dataframe
data = [(1, 2, 3),
(2, 3, 4),
(3, 4, 5),
(4, 5, 6)]

df = spark.createDataFrame(data, ["Col1", "Col2", "Col3"])
df.show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC 43. How to get the positions where values of two columns match?

# COMMAND ----------

# Create sample DataFrame
data = [("John", "John"), ("Lily", "Lucy"), ("Sam", "Sam"), ("Lucy", "Lily")]
df = spark.createDataFrame(data, ["Name1", "Name2"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id,row_number

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn("idx",monotonically_increasing_id()).show()

# COMMAND ----------

win_spec = Window.orderBy('idx')

# COMMAND ----------

df = df.withColumn("idx",monotonically_increasing_id()).withColumn("id",row_number().over(win_spec))

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter('name1=name2').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 44. How to create lags and leads of a column by group in a dataframe?

# COMMAND ----------

# Create a sample DataFrame
data = [("2023-01-01", "Store1", 100),
("2023-01-02", "Store1", 150),
("2023-01-03", "Store1", 200),
("2023-01-04", "Store1", 250),
("2023-01-05", "Store1", 300),
("2023-01-01", "Store2", 50),
("2023-01-02", "Store2", 60),
("2023-01-03", "Store2", 80),
("2023-01-04", "Store2", 90),
("2023-01-05", "Store2", 120)]

df = spark.createDataFrame(data, ["Date", "Store", "Sales"])

df.show()

# COMMAND ----------

from pyspark.sql.functions import col,lead,lag

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

winspec = Window.partitionBy('store').orderBy('Date')

# COMMAND ----------

df.withColumn("lead_price",lead('Sales').over(winspec)).show()

# COMMAND ----------

df.withColumn("lead_price",lag('sales').over(winspec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 45. How to get the frequency of unique values in the entire dataframe?

# COMMAND ----------

# Create a numeric DataFrame
data = [(1, 2, 3),
(2, 3, 4),
(1, 2, 3),
(4, 5, 6),
(2, 3, 4)]
df = spark.createDataFrame(data, ["Column1", "Column2", "Column3"])

# Print DataFrame
df.show()

# COMMAND ----------

from pyspark.sql.functions import concat_ws

# COMMAND ----------

concatenated_df = df.select(concat_ws(",", *df.columns).alias("Values")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 46. How to replace both the diagonals of dataframe with 0?

# COMMAND ----------

# Create a numeric DataFrame
data = [(1, 2, 3, 4),
(2, 3, 4, 5),
(1, 2, 3, 4),
(4, 5, 6, 7)]

df = spark.createDataFrame(data, ["col_1", "col_2", "col_3", "col_4"])

# Print DataFrame
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 47. How to reverse the rows of a dataframe?

# COMMAND ----------

# Create a numeric DataFrame
data = [(1, 2, 3, 4),
(2, 3, 4, 5),
(3, 4, 5, 6),
(4, 5, 6, 7)]

df = spark.createDataFrame(data, ["col_1", "col_2", "col_3", "col_4"])

# Print DataFrame
df.show()

# COMMAND ----------

df.select(*df.columns[::-1]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 51. How to impute missing values with Zero?

# COMMAND ----------

# Suppose df is your DataFrame
df = spark.createDataFrame([(1, None), (None, 2), (3, 4), (5, None)], ["a", "b"])

df.show()

# COMMAND ----------

df.fillna(0,subset=['a','b']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 70. How to get the names of DataFrame objects that have been created in an environment?

# COMMAND ----------

# MAGIC %md
# MAGIC #### 58. How to View PySpark Cluster Configuration Details?

# COMMAND ----------

for k,v in spark.sparkContext.getConf().getAll():
    print(f"{k} : {v}")

# COMMAND ----------

[name for name, obj in globals().items() if isinstance(obj, dataframe)]
