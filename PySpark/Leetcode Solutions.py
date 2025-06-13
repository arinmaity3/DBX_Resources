# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. 1757. Recyclable and Low Fat Products
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to find the ids of products that are both low fat and recyclable.
# MAGIC - https://leetcode.com/problems/recyclable-and-low-fat-products/?envType=study-plan-v2&envId=top-sql-50

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,BooleanType

# COMMAND ----------

schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("low_fats",BooleanType(),True),
    StructField("recyclable",BooleanType(),True)
]
)

# COMMAND ----------

data = [(0,True,False),(1,True,True),(2,False,True),(3,True,True),(4,False,False)]

# COMMAND ----------

df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter((df['low_fats'] == True) & (df['recyclable'] == True)).select('product_id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. 584. Find Customer Referee
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Find the names of the customer that are not referred by the customer with id = 2.
# MAGIC Return the result table in any order.
# MAGIC - https://leetcode.com/problems/find-customer-referee/?envType=study-plan-v2&envId=top-sql-50

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

data = [Row(id=1,name="Will",referee_id=None),
        Row(id=2,name="Jane",referee_id=None),
        Row(id=3,name="Alex",referee_id=2),
        Row(id=4,name="Bill",referee_id=None),
        Row(id=5,name="Zack",referee_id=1),
        Row(id=6,name="mark",referee_id=2)]

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter((df['referee_id'] != 2) | df['referee_id'].isNull()).select('name').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Note: When you use the != operator to compare a column to a specific value, it doesn't include null values because null represents an unknown or missing value, and comparisons involving null typically result in null or false.
# MAGIC
# MAGIC To include both rows where 'referee_id' is not equal to 2 and rows where 'referee_id' is null, you should indeed use the df['referee_id'].isNull() condition explicitly, 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. 595. Big Countries
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: A country is big if:
# MAGIC
# MAGIC it has an area of at least three million (i.e., 3000000 km2), or
# MAGIC it has a population of at least twenty-five million (i.e., 25000000).
# MAGIC Write a solution to find the name, population, and area of the big countries.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

#from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Create a Spark session
#spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("area", LongType(), True),
    StructField("population", LongType(), True),
    StructField("gdp", LongType(), True)
])

# Create the DataFrame with the provided data
data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000000),
    ("Albania", "Europe", 28748, 2831741, 12960000000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000000),
    ("Andorra", "Europe", 468, 78115, 3712000000),
    ("Angola", "Africa", 1246700, 20609294, 100990000000)
]

df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.filter((col('area')>=3000000) | (col('population')>=25000000)).select("name","area","population").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. 1148. Article Views I

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement:Write a solution to find all the authors that viewed at least one of their own articles.
# MAGIC
# MAGIC Return the result table sorted by id in ascending order.

# COMMAND ----------

data = [(1,3,5,"2019-08-01"),
        (1,3,6,"2019-08-02"),
        (2,7,7,"2019-08-01"),
        (2,7,6,"2019-08-02"),
        (4,7,1,"2019-07-22"),
        (3,4,4,"2019-07-21"),
        (3,4,4,"2019-02-21")]

# COMMAND ----------

df = spark.createDataFrame(data,["article_id","author_id","viewer_id","view_date"])

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("view_date",df['view_date'].cast("Date"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.filter(df['author_id'] == df['viewer_id']).display()

# COMMAND ----------

df.filter(df['author_id'] == df['viewer_id']).select('viewer_id').distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. 1683. Invalid Tweets
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statemaent:Write a solution to find the IDs of the invalid tweets. The tweet is invalid if the number of characters used in the content of the tweet is strictly greater than 15.

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

data = [Row(tweet_id = 1,content ="Vote for Biden "),
        Row(tweet_id = 2,content ="Let us make America great again!")
        ]

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import length,col

# COMMAND ----------

df.withColumn("tweet_length",length(col("content"))).display()

# COMMAND ----------

df.filter(length(col("content")) > 15).select("tweet_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. 1378. Replace Employee ID With The Unique Identifier
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to show the unique ID of each user, If a user does not have a unique ID replace just show null.

# COMMAND ----------

emp_data =[Row(id = 1,name = "Alice" ),
           Row(id = 7,name = "bob"),
           Row(id = 11,name = "meir"),
           Row(id = 90,name = "Winston"),
           Row(id = 3,name = "jonathon")
           ]

# COMMAND ----------

uni_data = [Row(id = 3, unique_id = 1),
            Row(id = 11, unique_id = 2),
            Row(id = 90, unique_id = 3)]

# COMMAND ----------

emp_df = spark.createDataFrame(emp_data)
uni_df = spark.createDataFrame(uni_data)
emp_df.show()
uni_df.show()

# COMMAND ----------

emp_df.join(uni_df,["id"],"left").select("name","unique_id").display()

# COMMAND ----------

emp_df.join(uni_df,["id"],"anti").display()

# COMMAND ----------

emp_df.join(uni_df,["id"],"semi").display()

# COMMAND ----------

emp_df.join(uni_df,["id"],"right").display()

# COMMAND ----------

emp_df.join(uni_df,["id"],"inner").display()

# COMMAND ----------

emp_df.join(uni_df,["id"],"full").display()

# COMMAND ----------

emp_df.join(uni_df,["id"],"cross").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. 1068. Product Sales Analysis I

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to report the product_name, year, and price for each sale_id in the Sales table.

# COMMAND ----------

sales_data = [(1,100,"2008",10,5000),
              (2,100,"2009",12,5000),
              (7,200,"2011",15,9000)]

# COMMAND ----------

sales_df = spark.createDataFrame(sales_data,["sale_id" , "product_id" , "year" , "quantity" , "price"])

# COMMAND ----------

sales_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

sales_df.display()

# COMMAND ----------

data=[Row(product_id = 100,product_name ="Nokia" ),Row(product_id = 200,product_name = "Apple"),Row(product_id = 300,product_name ="Samsung" )]

# COMMAND ----------

product_df = spark.createDataFrame(data)

# COMMAND ----------

product_df.display()

# COMMAND ----------

sales_df.join(product_df,"product_id","inner").select("product_name","year","price").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. 1581. Customer Who Visited but Did Not Make Any Transactions

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to find the IDs of the users who visited without making any transactions and the number of times they made these types of visits.

# COMMAND ----------


# Create DataFrames for Visits and Transactions
visits_data = [(1, 23), (2, 9), (4, 30), (5, 54), (6, 96), (7, 54), (8, 54)]
transactions_data = [(2, 5, 310), (3, 5, 300), (9, 5, 200), (12, 1, 910), (13, 2, 970), (13, 8, 970)]

visits_df = spark.createDataFrame(visits_data, ["visit_id", "customer_id"])
transactions_df = spark.createDataFrame(transactions_data, ["transaction_id", "visit_id", "amount"])

# Show the DataFrames
visits_df.show()
transactions_df.show()


# COMMAND ----------

visits_df.join(transactions_df,"visit_id","anti").display()

# COMMAND ----------

visited_customers= visits_df.join(transactions_df,"visit_id","semi").select("customer_id").distinct().collect()

# COMMAND ----------

print(visited_customers)

# COMMAND ----------

visited_customer_list = [row.customer_id for row in visited_customers]

# COMMAND ----------

visited_customer_list

# COMMAND ----------

visits_df.filter(~col("customer_id").isin(visited_customer_list)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9. 197. Rising Temperature
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Find all dates' Id with higher temperatures compared to its previous dates (yesterday).

# COMMAND ----------


from pyspark.sql.functions import to_date, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType



# Define the schema for the DataFrame
# schema = StructType([
#     StructField("id", IntegerType(), False),
#     StructField("recordDate", DateType(), False),
#     StructField("temperature", IntegerType(), False)
# ])

# Sample data for the Weather table
data = [
    (1, "2015-01-01", 10),
    (2, "2015-01-02", 25),
    (3, "2015-01-03", 20),
    (4, "2015-01-04", 30)
]

# Create the DataFrame
weather_df = spark.createDataFrame(data, ["id","recordDate","temparature"])

# Show the DataFrame
weather_df.show()


# COMMAND ----------

from pyspark.sql.functions import date_add,col,expr

# COMMAND ----------

weather_df = weather_df.withColumn("recordDate",col('recordDate').cast("Date"))

# COMMAND ----------

weather_df.alias("w1").join(weather_df.alias("w2"),expr("w1.recordDate = date_add(w2.recordDate,1)"),"inner")\
    .select(expr("w1.id"),expr("w1.recordDate"),expr("w1.temparature"),expr("w2.recordDate AS PrevDate"),expr("w2.temparature")).display()

# COMMAND ----------

weather_df.alias("w1").join(weather_df.alias("w2"),expr("w1.recordDate = date_add(w2.recordDate,1) and w1.temparature > w2.temparature"),"inner")\
    .select(expr("w1.id"),expr("w1.recordDate"),expr("w1.temparature"),expr("w2.recordDate AS PrevDate"),expr("w2.temparature")).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("WeatherTable").getOrCreate()

# Sample data for the Weather table
data = [
    (1, "2015-01-01", 10),
    (2, "2015-01-02", 25),
    (3, "2015-01-03", 20),
    (4, "2015-01-04", 30)
]

# Create the DataFrame with corrected column names
weather_df = spark.createDataFrame(data, ["id", "recordDate", "temperature"])

# Convert the 'recordDate' column to DateType
weather_df = weather_df.withColumn("recordDate", col('recordDate').cast("Date"))

# Use the lag function to get the previous date and temperature
window_spec = Window.orderBy("recordDate")
result_df = weather_df.withColumn("PrevDate", lag("recordDate").over(window_spec)) \
                     .withColumn("PrevTemperature", lag("temperature").over(window_spec))

# Show the result DataFrame
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 10. 1661. Average Time of Process per Machine

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement:
# MAGIC There is a factory website that has several machines each running the same number of processes. Write a solution to find the average time each machine takes to complete a process.
# MAGIC
# MAGIC The time to complete a process is the 'end' timestamp minus the 'start' timestamp. The average time is calculated by the total time to complete every process on the machine divided by the number of processes that were run.
# MAGIC
# MAGIC The resulting table should have the machine_id along with the average time as processing_time, which should be rounded to 3 decimal places.

# COMMAND ----------


# Sample data for the Activity table
data = [
    (0, 0, "start", 0.712),
    (0, 0, "end", 1.520),
    (0, 1, "start", 3.140),
    (0, 1, "end", 4.120),
    (1, 0, "start", 0.550),
    (1, 0, "end", 1.550),
    (1, 1, "start", 0.430),
    (1, 1, "end", 1.420),
    (2, 0, "start", 4.100),
    (2, 0, "end", 4.512),
    (2, 1, "start", 2.500),
    (2, 1, "end", 5.000)
]

# Create the DataFrame
activity_df = spark.createDataFrame(data, ["machine_id", "process_id", "activity_type", "timestamp"])

# Show the DataFrame
activity_df.show()


# COMMAND ----------

from pyspark.sql.functions import sum,when

# COMMAND ----------

process_df = activity_df.groupBy("machine_id","process_id")\
    .agg(sum(when(col('activity_type') == "start",-col('timestamp')).otherwise(col('timestamp'))).alias("process_time"))

# COMMAND ----------

process_df.show()

# COMMAND ----------

from pyspark.sql.functions import avg,round

# COMMAND ----------

process_df.groupBy("machine_id").agg(round(avg(col("process_time")),2)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 11. 577. Employee Bonus
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to report the name and bonus amount of each employee with a bonus less than 1000.

# COMMAND ----------

data = [(3 ,"Brad",None,4000),
        (1 ,"John",3,1000),
        (2 ,"Dan",3,2000),
        (4 ,"Thomas",3,4000)]

# COMMAND ----------

emp_df = spark.createDataFrame(data,["empId","name","supervisor","salary"])

# COMMAND ----------

emp_df.show()

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

bonus_df = spark.createDataFrame(data=[Row(empId = 2, bonus = 500),Row(empId = 4, bonus = 2000)])
bonus_df.show()

# COMMAND ----------

result_df = emp_df.join(bonus_df,"empId","Left")\
                    .filter("bonus < 1000 or bonus is null")

# COMMAND ----------

result_df.show()

# COMMAND ----------

emp_df.join(bonus_df,"empId","Left")\
                    .filter((col('bonus') < 1000)| (col('bonus').isNull() )).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 12. 1280. Students and Examinations
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to find the number of times each student attended each exam.
# MAGIC
# MAGIC Return the result table ordered by student_id and subject_name.

# COMMAND ----------

#from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row

# Create a Spark session
#spark = SparkSession.builder.appName("StudentSubjectExams").getOrCreate()

# Sample data for the Students table
students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]

# Sample data for the Subjects table
subjects_data = [Row(subject_name =  "Math"),
                 Row(subject_name = "Physics"),
                 Row(subject_name = "Programming")  
]

# Sample data for the Examinations table
examinations_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]

# Define schemas for Students and Examinations tables
students_schema = StructType([
    StructField("student_id", IntegerType(), False),
    StructField("student_name", StringType(), False)
])

examinations_schema = StructType([
    StructField("student_id", IntegerType(), False),
    StructField("subject_name", StringType(), False)
])

# Create DataFrames with defined schemas
students_df = spark.createDataFrame(students_data, schema=students_schema)
subjects_df = spark.createDataFrame(subjects_data)
examinations_df = spark.createDataFrame(examinations_data, schema=examinations_schema)

# Show the DataFrames
students_df.show()
subjects_df.show()
examinations_df.show()


# COMMAND ----------

students_df.crossJoin(subjects_df).display()

# COMMAND ----------

stedents_with_each_subject_df = students_df.crossJoin(subjects_df)

# COMMAND ----------

examinations_df.columns

# COMMAND ----------

examinations_df.groupBy("student_id","subject_name").count().display()

# COMMAND ----------

grouped_exam_df = examinations_df.groupBy("student_id","subject_name").count()

# COMMAND ----------

stedents_with_each_subject_df.join(grouped_exam_df,["student_id","subject_name"],"left").show()

# COMMAND ----------

stedents_with_each_subject_df.join(grouped_exam_df,["student_id","subject_name"],"left")\
                            .fillna(0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 13. 570. Managers with at Least 5 Direct Reports
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement:
# MAGIC Write a solution to find managers with at least five direct reports.

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType



# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("managerId", StringType(), True)  # Use StringType to handle "None" values
])

# Create a list of rows based on the provided data
data = [
    (101, 'John', 'A', 'None'),
    (102, 'Dan', 'A', '101'),
    (103, 'James', 'A', '101'),
    (104, 'Amy', 'A', '101'),
    (105, 'Anne', 'A', '101'),
    (106, 'Ron', 'B', '101')
]

# Create a DataFrame using the schema and data
employee_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
employee_df.show()




# COMMAND ----------

employee_df.groupBy("managerId").count().filter("count >=5").show()

# COMMAND ----------

manager_df = employee_df.groupBy("managerId").count().filter("count >=5")

# COMMAND ----------

# Join the filtered manager DataFrame with the original employee DataFrame
result_df = employee_df.join(manager_df, employee_df['id'] == manager_df['managerId'], "inner").select(col('name'))

# Show the result
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 14. 1934. Confirmation Rate
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: The confirmation rate of a user is the number of 'confirmed' messages divided by the total number of requested confirmation messages. The confirmation rate of a user that did not request any confirmation messages is 0. Round the confirmation rate to two decimal places.
# MAGIC
# MAGIC Write an SQL query to find the confirmation rate of each user.

# COMMAND ----------

#from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp

# Create a SparkSession
#spark = SparkSession.builder.appName("SignUpConfirmation").getOrCreate()

# Define the data for the Signups table
signups_data = [
    (3, "2020-03-21 10:16:13"),
    (7, "2020-01-04 13:57:59"),
    (2, "2020-07-29 23:09:44"),
    (6, "2020-12-09 10:39:37"),
]

# Define the data for the Confirmations table
confirmations_data = [
    (3, "2021-01-06 03:30:46", "timeout"),
    (3, "2021-07-14 14:00:00", "timeout"),
    (7, "2021-06-12 11:57:29", "confirmed"),
    (7, "2021-06-13 12:58:28", "confirmed"),
    (7, "2021-06-14 13:59:27", "confirmed"),
    (2, "2021-01-22 00:00:00", "confirmed"),
    (2, "2021-02-28 23:59:59", "timeout"),
]


# Define the schema for the Signups table
signups_schema = ["user_id", "time_stamp"]

# Define the schema for the Confirmations table
confirmations_schema = ["user_id", "time_stamp", "action"]


# Create PySpark DataFrames
signups_df = spark.createDataFrame(signups_data, signups_schema)
confirmations_df = spark.createDataFrame(confirmations_data, confirmations_schema)

# Convert the 'time_stamp' column to a timestamp type
signups_df = signups_df.withColumn("time_stamp", to_timestamp("time_stamp"))
confirmations_df = confirmations_df.withColumn("time_stamp", to_timestamp("time_stamp"))

# Show the DataFrames
signups_df.show()
confirmations_df.show()



# COMMAND ----------

from pyspark.sql.functions import count,sum,when,col

# COMMAND ----------

confirmations_df.groupBy("user_id").agg((count(when(col("action")=="confirmed",1))/count(col("action"))).alias("confirmation_rate")).show()

# COMMAND ----------

confirmation_rate_df = confirmations_df.groupBy("user_id")\
                                    .agg((count(when(col("action")=="confirmed",1))/count(col("action"))).alias("confirmation_rate"))

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------

signups_df.join(confirmation_rate_df,"user_id","left")\
            .select("user_id",col("confirmation_rate").cast("decimal(20,2)"))\
            .fillna(0)\
            .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 15. 620. Not Boring Movies
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to report the movies with an odd-numbered ID and a description that is not "boring".
# MAGIC
# MAGIC Return the result table ordered by rating in descending order.

# COMMAND ----------

data = [(1,"War","great 3D",8.9 ),
        (2,"Science","fiction",8.5),
        (3,"irish","boring",6.2),
        (4,"Ice song","Fantacy",8.6),
        (5,"House card","Interesting",9.1 )]

# COMMAND ----------

df = spark.createDataFrame(data,schema=["id","movie","description","rating"])

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter((df['id'] %2 != 0 ) & (df['description'] != "boring")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 16. 1251. Average Selling Price

# COMMAND ----------

# Import necessary libraries

from pyspark.sql.functions import col

# Define the data for the Prices and UnitsSold tables
prices_data = [
    (1, "2019-02-17", "2019-02-28", 5),
    (1, "2019-03-01", "2019-03-22", 20),
    (2, "2019-02-01", "2019-02-20", 15),
    (2, "2019-02-21", "2019-03-31", 30),
]

units_sold_data = [
    (1, "2019-02-25", 100),
    (1, "2019-03-01", 15),
    (2, "2019-02-10", 200),
    (2, "2019-03-22", 30),
]

# Create DataFrames for Prices and UnitsSold
prices_df = spark.createDataFrame(prices_data, ["product_id", "start_date", "end_date", "price"])
units_sold_df = spark.createDataFrame(units_sold_data, ["product_id", "purchase_date", "units"])



# Show the result DataFrame
prices_df.show()
units_sold_df.show()


# COMMAND ----------

units_sold_df\
    .join(prices_df,(units_sold_df['product_id'] == prices_df["product_id"])&\
        ((units_sold_df['purchase_date']<=prices_df['end_date'])&(units_sold_df['purchase_date']>=prices_df['start_date'])),"inner").show()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

result_df = units_sold_df \
    .join(prices_df, (units_sold_df['product_id'] == prices_df["product_id"]) &
          ((units_sold_df['purchase_date'] >= prices_df['start_date']) &
           (units_sold_df['purchase_date'] <= prices_df['end_date'])),
          "inner") \
    .select(units_sold_df['product_id'], "units", "price")

# COMMAND ----------

result_df.show()

# COMMAND ----------

from pyspark.sql.functions import sum,round

# COMMAND ----------

result_df.groupBy("product_id")\
    .agg(round(sum(col('units')*col('price'))/sum(col("units")),2).alias("average_price")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17. 1075. Project Employees I
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write an SQL query that reports the average experience years of all the employees for each project, rounded to 2 digits.

# COMMAND ----------


# Define the data for the Project and Employee tables
project_data = [
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 1),
    (2, 4),
]

employee_data = [
    (1, "Khaled", 3),
    (2, "Ali", 2),
    (3, "John", 1),
    (4, "Doe", 2),
]

# Create DataFrames for Project and Employee
project_df = spark.createDataFrame(project_data, ["project_id", "employee_id"])
employee_df = spark.createDataFrame(employee_data, ["employee_id", "name", "experience_years"])

# Show the DataFrames
project_df.show()
employee_df.show()


# COMMAND ----------

from pyspark.sql.functions import avg

# COMMAND ----------

project_df.join(employee_df,"employee_id","inner")\
    .groupBy("project_id").agg(avg("experience_years").cast("decimal(20,2)").alias("average_years ")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 18. 1633. Percentage of Users Attended a Contest
# MAGIC

# COMMAND ----------


# Define the data for the Users and Register tables
users_data = [
    (6, "Alice"),
    (2, "Bob"),
    (7, "Alex"),
]

register_data = [
    (215, 6),
    (209, 2),
    (208, 2),
    (210, 6),
    (208, 6),
    (209, 7),
    (209, 6),
    (215, 7),
    (208, 7),
    (210, 2),
    (207, 2),
    (210, 7),
]

# Create DataFrames for Users and Register
users_df = spark.createDataFrame(users_data, ["user_id", "user_name"])
register_df = spark.createDataFrame(register_data, ["contest_id", "user_id"])

# Show the DataFrames
users_df.show()
register_df.show()


# COMMAND ----------

users_count = users_df.distinct().count()

# COMMAND ----------

users_count

# COMMAND ----------

from pyspark.sql.functions import countDistinct

# COMMAND ----------

register_df.groupBy('contest_id')\
    .agg((countDistinct("user_id")*100/users_count).cast("decimal(30,2)").alias("percentage")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 19. 1211. Queries Quality and Percentage
# MAGIC

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Define the schema for the DataFrame
schema = StructType([
    StructField("query_name", StringType(), True),
    StructField("result", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("rating", IntegerType(), True)
])

# Define the data for the Queries table
data = [
    ("Dog", "Golden Retriever", 1, 5),
    ("Dog", "German Shepherd", 2, 5),
    ("Dog", "Mule", 200, 1),
    ("Cat", "Shirazi", 5, 2),
    ("Cat", "Siamese", 3, 3),
    ("Cat", "Sphynx", 7, 4),
]

# Create a DataFrame from the data and schema
queries_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
queries_df.show()




# COMMAND ----------

from pyspark.sql.functions import avg,col,count,when

# COMMAND ----------

queries_df.groupBy("query_name")\
    .agg(avg(col('rating')/col('position'))\
            .cast("decimal(30,2)").alias("quality"),\
        (count(when(col('rating')<=3,col('rating')))*100/count('rating'))\
            .cast("decimal(30,2)").alias("poor_query_percentage "))\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 20. 1193. Monthly Transactions I
# MAGIC

# COMMAND ----------

# from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import to_date

# # Create a SparkSession
# spark = SparkSession.builder.appName("TransactionsDataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("trans_date", StringType(), True)
])

# Define the data for the Transactions table
data = [
    (121, "US", "approved", 1000, "2018-12-18"),
    (122, "US", "declined", 2000, "2018-12-19"),
    (123, "US", "approved", 2000, "2019-01-01"),
    (124, "DE", "approved", 2000, "2019-01-07")
]



# Create a DataFrame from the data and schema
transactions_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
transactions_df.show()

# # Stop the SparkSession
# spark.stop()


# COMMAND ----------

from pyspark.sql.functions import to_date,col

# COMMAND ----------

transactions_df = transactions_df.withColumn("trans_date",to_date(col("trans_date"),"yyyy-MM-dd"))

# COMMAND ----------

transactions_df.show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

transactions_df.withColumn("trans_date",date_format(col("trans_date"),"yyyy-MM")).show()

# COMMAND ----------

transactions_df = transactions_df.withColumn("trans_date",date_format(col("trans_date"),"yyyy-MM"))

# COMMAND ----------

from pyspark.sql.functions import count,sum,when

# COMMAND ----------

transactions_df.groupBy("trans_date","country")\
    .agg(count("state").alias("trans_count"),\
            count(when(col('state')=="approved","state")).alias("approved_count"),\
            sum("amount").alias("trans_total_amount"),\
            sum(when(col('state')=="approved",col("amount"))).alias("approved_total_amount")
        ).show()

# COMMAND ----------

transactions_df.show()

# COMMAND ----------

transactions_df.select(date_format(col("trans_date"),"MM:yyyy")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 21. 1174. Immediate Food Delivery II
# MAGIC

# COMMAND ----------

# from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType
from pyspark.sql.functions import to_date

# Create a SparkSession
# spark = SparkSession.builder \
#     .appName("DeliveryDataFrame") \
#     .getOrCreate()

# Define the schema for the Delivery DataFrame
schema = StructType([
    StructField("delivery_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", StringType(), False),
    StructField("customer_pref_delivery_date", StringType(), False)
])

# Define the data for the Delivery DataFrame
data = [
    (1, 1, "2019-08-01", "2019-08-02"),
    (2, 2, "2019-08-02", "2019-08-02"),
    (3, 1, "2019-08-11", "2019-08-12"),
    (4, 3, "2019-08-24", "2019-08-24"),
    (5, 3, "2019-08-21", "2019-08-22"),
    (6, 2, "2019-08-11", "2019-08-13"),
    (7, 4, "2019-08-09", "2019-08-09")
]


# Create the Delivery DataFrame
delivery_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
delivery_df.show()


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

delivery_df = delivery_df.withColumn("order_date",col("order_date").cast(DateType()))\
    .withColumn("customer_pref_delivery_date",col("customer_pref_delivery_date").cast(DateType()))



# COMMAND ----------

from pyspark.sql.functions import row_number,col,desc

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

win_spec = Window.partitionBy("customer_id").orderBy(col("order_date"))

# COMMAND ----------

delivery_df.withColumn("row_number",row_number().over(win_spec)).show()

# COMMAND ----------

delivery_df.withColumn("row_number",row_number().over(win_spec))\
    .filter("row_number=1").show()

# COMMAND ----------

delivery_df = delivery_df.withColumn("row_number",row_number().over(win_spec))\
    .filter("row_number=1")\
    .withColumn("type",when(col("order_date")==col("customer_pref_delivery_date"),"immidiate")\
        .otherwise("scheduled"))

# COMMAND ----------

delivery_df.show()

# COMMAND ----------

delivery_df.filter("type='immidiate'").count()*100/delivery_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 22. 550. Game Play Analysis IV

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType,StringType


# Define the schema for the Activity DataFrame
schema = StructType([
    StructField("player_id", IntegerType(), False),
    StructField("device_id", IntegerType(), False),
    StructField("event_date", StringType(), False),
    StructField("games_played", IntegerType(), False)
])

# Define the data for the Activity DataFrame
data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-03-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]

# Create the Activity DataFrame
activity_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
activity_df.show()


# COMMAND ----------

from pyspark.sql.functions import expr,date_add

# COMMAND ----------

activity_df.alias("a1")\
    .join(activity_df.alias("a2"),expr("a1.player_id = a2.player_id and date_add(a2.event_date,1)= a1.event_date"),"inner")\
        .select(expr("a1.player_id")).show()

# COMMAND ----------

next_day_df = activity_df.alias("a1")\
    .join(activity_df.alias("a2"),expr("a1.player_id = a2.player_id and date_add(a2.event_date,1)= a1.event_date"),"inner")\
        .select(expr("a1.player_id"))

# COMMAND ----------

activity_df.select("player_id").distinct().show()

# COMMAND ----------

round(next_day_df.count()/activity_df.select("player_id").distinct().count(),2)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select ROUND(1.0 * (select count(distinct t1.player_id)
# MAGIC from
# MAGIC (select player_id, event_date, RANK() OVER(PARTITION BY player_id ORDER by event_date) as rnk, LEAD(event_date, 1) OVER (PARTITION BY player_id ORDER by event_date) as next_day
# MAGIC from Activity) t1
# MAGIC where t1.rnk = 1 AND DATEADD( 1, t1.event_date) = t1.next_day ) /
# MAGIC (select count(distinct player_id)
# MAGIC from Activity), 2) as fraction

# COMMAND ----------

# MAGIC %md
# MAGIC #### 23. 2356. Number of Unique Subjects Taught by Each Teacher

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

data = [
    Row(teacher_id= 1, subject_id= 2, dept_id= 3),
    Row(teacher_id= 1, subject_id= 2, dept_id= 4),
    Row(teacher_id= 1, subject_id= 3, dept_id= 3),
    Row(teacher_id= 2, subject_id= 1, dept_id= 1),
    Row(teacher_id= 2, subject_id= 2, dept_id= 1),
    Row(teacher_id= 2, subject_id= 3, dept_id= 1),
    Row(teacher_id= 2, subject_id= 4, dept_id= 1)
]

# COMMAND ----------

teachers_df = spark.createDataFrame(data)

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to calculate the number of unique subjects each teacher teaches in the university.

# COMMAND ----------

teachers_df.show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct

# COMMAND ----------

teachers_df.groupBy("teacher_id").agg(countDistinct("subject_id").alias("cnt")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 24. 1141. User Activity for the Past 30 Days I
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to find the daily active user count for a period of 30 days ending 2019-07-27 inclusively. A user was active on someday if they made at least one activity on that day.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Create a SparkSession
# spark = SparkSession.builder \
#     .appName("ActivityDataFrame") \
#     .getOrCreate()

# Define the schema for the Activity DataFrame
schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("session_id", IntegerType(), False),
    StructField("activity_date", StringType(), False),
    StructField("activity_type", StringType(), False)
])

# Define the data for the Activity DataFrame
data = [
    (1, 1, "2019-07-20", "open_session"),
    (1, 1, "2019-07-20", "scroll_down"),
    (1, 1, "2019-07-20", "end_session"),
    (2, 4, "2019-07-20", "open_session"),
    (2, 4, "2019-07-21", "send_message"),
    (2, 4, "2019-07-21", "end_session"),
    (3, 2, "2019-07-21", "open_session"),
    (3, 2, "2019-07-21", "send_message"),
    (3, 2, "2019-07-21", "end_session"),
    (4, 3, "2019-06-25", "open_session"),
    (4, 3, "2019-06-25", "end_session")
]

# Create the Activity DataFrame
activity_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
activity_df.show()


# COMMAND ----------

activity_df = activity_df.withColumn("activity_date",col("activity_date").cast(DateType()))

# COMMAND ----------

activity_df.show()

# COMMAND ----------

from pyspark.sql.functions import lit,to_date

# COMMAND ----------

comparison_date = to_date(lit("2019-07-27"))

# COMMAND ----------

filtered_df = activity_df.filter((col('activity_date')<= comparison_date)&(col('activity_date')>=date_add(comparison_date,-30)))

# COMMAND ----------

filtered_df.groupBy("activity_date").agg(countDistinct("user_id")).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 25. 1070. Product Sales Analysis III
# MAGIC

# COMMAND ----------

# Import necessary modules

from pyspark.sql.functions import col


# Define the data for the Sales and Product tables
sales_data = [(1, 100, 2008, 10, 5000),
              (2, 100, 2009, 12, 5000),
              (7, 200, 2011, 15, 9000)]

product_data = [(100, "Nokia"),
                (200, "Apple"),
                (300, "Samsung")]

# Create DataFrames from the data
sales_columns = ["sale_id", "product_id", "year", "quantity", "price"]
sales_df = spark.createDataFrame(sales_data, sales_columns)

product_columns = ["product_id", "product_name"]
product_df = spark.createDataFrame(product_data, product_columns)

# Display the DataFrames
sales_df.show()
product_df.show()



# COMMAND ----------

from pyspark.sql.functions import rank,col

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

win_spec = Window.partitionBy("product_id").orderBy(col("year"))

# COMMAND ----------

sales_df.withColumn("year_rank",rank().over(win_spec)).show()

# COMMAND ----------

sales_df.withColumn("year_rank",rank().over(win_spec))\
        .filter("year_rank=1")\
        .drop("sale_id","year_rank")\
        .withColumnRenamed("year","first_year")\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 26. 596. Classes More Than 5 Students
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType

# COMMAND ----------

courses_schema = StructType([
    StructField("student",StringType(),True),
    StructField("class",StringType(),True)
])

# COMMAND ----------

courses_data = [("A","Math"),
                ("B","English"),
                ("C","Math"),
                ("D","Biology"),
                ("E","Math"),
                ("F","Computer"),
                ("G","Math"),
                ("H","Math"),
                ("I","Math")
                ]

# COMMAND ----------

courses_df = spark.createDataFrame(data=courses_data,schema=courses_schema)

# COMMAND ----------

courses_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Problem Statement: Write a solution to find all the classes that have at least five students.

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

courses_df.groupBy("class").agg(count("student").alias("student_count"))\
    .filter("student_count >= 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 27. 1729. Find Followers Count
# MAGIC

# COMMAND ----------

data = [(0,1),(1,0),(2,0),(2,1)]

# COMMAND ----------

followers_df = spark.createDataFrame(data,["user_id","follower_id"])

# COMMAND ----------

followers_df.show()

# COMMAND ----------

from pyspark.sql.functions import count,col,desc

# COMMAND ----------

followers_df.groupBy("user_id")\
    .agg(count("follower_id").alias("followers_count"))\
    .sort(col("user_id").desc())\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 28. 619. Biggest Single Number
# MAGIC

# COMMAND ----------

my_num_df = spark.createDataFrame([(8,), (8,), (3,), (3,), (4,), (5,), (6,)], ["num"])

# COMMAND ----------

my_num_df.show()

# COMMAND ----------

from pyspark.sql.functions import max

# COMMAND ----------

my_num_df.groupBy("num")\
    .agg(count("num").alias("num_count"))\
        .filter("num_count<=1")\
            .select(max("num"))\
                .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 29. [Not Solved] 1045. Customers Who Bought All Products
# MAGIC

# COMMAND ----------

customers_df = spark.createDataFrame([(1,5),
                                      (2,6),
                                      (3,5),
                                      (3,6),
                                      (1,6),
                                      (3,6),], ["customer_id","product_key"])

# COMMAND ----------

customers_df.show()

# COMMAND ----------

product_df = spark.createDataFrame([(5,),(6,)],["product_key"])

# COMMAND ----------

product_df.show()

# COMMAND ----------

from pyspark.sql.functions import collect_list,collect_set

# COMMAND ----------

customers_df.groupBy("customer_id").agg(collect_list("product_key")).show()

# COMMAND ----------

customers_df.groupBy("customer_id").agg(collect_set("product_key")).show()

# COMMAND ----------

type(product_df.rdd)

# COMMAND ----------

product_df.rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

product_list = product_df.rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

new_df  = customers_df.groupBy("customer_id").agg(collect_set("product_key").alias("collected_product"))

# COMMAND ----------

new_df.display()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

from pyspark.sql.functions import array_contains

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

broadcast_product_list = broadcast(product_list)

# COMMAND ----------

new_df.filter(array_contains(col("collected_product"),lit(product_list))).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, broadcast, array_contains

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Define the data for customers_df and product_df
customers_data = [(1, 5), (2, 6), (3, 5), (3, 6), (1, 6), (3, 6)]
customers_columns = ["customer_id", "product_key"]
customers_df = spark.createDataFrame(customers_data, customers_columns)

product_data = [(5,), (6,)]
product_columns = ["product_key"]
product_df = spark.createDataFrame(product_data, product_columns)

# Collect the product values into a list
product_list = [row.product_key for row in product_df.collect()]

# Broadcast the product_list for optimization
broadcast_product_list = spark.sparkContext.broadcast(product_list)

# Group customers_df by customer_id and collect unique product_keys into a set
new_df = customers_df.groupBy("customer_id").agg(collect_set("product_key").alias("collected_product"))

# Filter new_df using array_contains and the broadcasted product_list
filtered_df = new_df.filter(array_contains(col("collected_product"), broadcast_product_list.value))

# Show the filtered DataFrame
filtered_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, broadcast, array_contains

# Create a Spark session
#spark = SparkSession.builder.appName("example").getOrCreate()

# Define the data for customers_df and product_df
customers_data = [(1, 5), (2, 6), (3, 5), (3, 6), (1, 6), (3, 6)]
customers_columns = ["customer_id", "product_key"]
customers_df = spark.createDataFrame(customers_data, customers_columns)

product_data = [(5,), (6,)]
product_columns = ["product_key"]
product_df = spark.createDataFrame(product_data, product_columns)

# Collect the product values into a list
product_list = [row.product_key for row in product_df.collect()]

# Broadcast the product_list for optimization
broadcast_product_list = spark.sparkContext.broadcast(product_list)

# Group customers_df by customer_id and collect unique product_keys into a set
new_df = customers_df.groupBy("customer_id").agg(collect_set("product_key").alias("collected_product"))

# Filter new_df using array_contains and the broadcasted product_list
filtered_df = new_df.filter(array_contains(col("collected_product"), broadcast_product_list.value))

# Show the filtered DataFrame
filtered_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 30. 1731. The Number of Employees Which Report to Each Employee

# COMMAND ----------

data = [(9,"Hercy",None,43),
        (6,"Alice",9,41),
        (4,"Bob",9,36),
        (2,"Winston",None,37),]

# COMMAND ----------

emp_df = spark.createDataFrame(data,["employee_id","name","reports_to","age"])

# COMMAND ----------

emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import count,avg,round

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

emp_df.dropna(subset=["reports_to"])\
    .groupBy(col("reports_to").alias("manager_id"))\
        .agg(count("employee_id").alias("reports_count"),\
        round(avg("age")).cast(IntegerType()).alias("average_age"))\
            .join(emp_df,col("manager_id")==emp_df["employee_id"],"inner")\
                .select("manager_id","name","reports_count","average_age").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 31. 1789. Primary Department for Each Employee
# MAGIC

# COMMAND ----------

employee_id | department_id | primary_flag |
+-------------+---------------+--------------+
| 1           | 1             | N            |
| 2           | 1             | Y            |
| 2           | 2             | N            |
| 3           | 3             | N            |
| 4           | 2             | N            |
| 4           | 3             | Y            |
| 4           | 4             | N   

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

data = [
    Row(emp_id = 1, dept_id = 1, flag = 'N'),
    Row(emp_id = 2, dept_id = 1, flag = 'Y'),
    Row(emp_id = 2, dept_id = 2, flag = 'N'),
    Row(emp_id = 3, dept_id = 3, flag = 'N'),
    Row(emp_id = 4, dept_id = 2, flag = 'N'),
    Row(emp_id = 4, dept_id = 3, flag = 'Y'),
    Row(emp_id = 4, dept_id = 4, flag = 'N'),
]

# COMMAND ----------

emp_df  = spark.createDataFrame(data)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

result_df = emp_df.alias("e2").join(
    emp_df.filter(emp_df["flag"] == 'Y').alias("e1"),
    emp_df["emp_id"] == col("e1.emp_id"),
    "left"
).filter(col("e1.emp_id").isNull() | (col('e2.flag') == 'Y'))\
    .select(col("e2.emp_id"),col("e2.dept_id"))

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 32. 610. Triangle Judgement
# MAGIC

# COMMAND ----------

data = [
    Row(x= 13, y= 15,z = 30),
    Row(x= 10, y= 20,z = 15),
    Row(x= 15, y= 10,z = 20),
    Row(x= 20, y= 10,z = 15),
]

# COMMAND ----------

tri_df = spark.createDataFrame(data)

# COMMAND ----------

tri_df.show()

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

tri_df.withColumn("triangle",when(((col('x') + col('y') > col('z')) & (col('x') + col('z') > col('y')) & (col('z') + col('y') > col('x'))),'Y').otherwise('N')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 33. 180. Consecutive Numbers
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
#spark = SparkSession.builder.appName("CreateLogsDataFrame").getOrCreate()

# Define the data as a list of Row objects
data = [
    Row(id=1, num=1),
    Row(id=2, num=1),
    Row(id=3, num=1),
    Row(id=4, num=2),
    Row(id=5, num=1),
    Row(id=6, num=2),
    Row(id=7, num=2)
]

# Create the DataFrame
logs_df = spark.createDataFrame(data)

# Show the DataFrame
logs_df.show()


# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import lead

# COMMAND ----------

win_spec = Window.orderBy("id")

# COMMAND ----------

logs_df.withColumn("next_num1", lead(col("num"), 1).over(win_spec))\
    .withColumn("next_num2", lead(col("num"), 2).over(win_spec))\
        .filter("num = next_num1 and num = next_num2")\
            .select("num").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 34. 1164. Product Price at a Given Date
# MAGIC

# COMMAND ----------

#from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DateType
from pyspark.sql.functions import col
from datetime import date

# Create a SparkSession
#spark = SparkSession.builder.appName("ProductsDataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("new_price", FloatType(), False),
    StructField("change_date", DateType(), False)
])

# Define the data as a list of tuples
data = [
    (1, 20.0, date(2019, 8, 14)),
    (2, 50.0, date(2019, 8, 14)),
    (1, 30.0, date(2019, 8, 15)),
    (1, 35.0, date(2019, 8, 16)),
    (2, 65.0, date(2019, 8, 17)),
    (3, 20.0, date(2019, 8, 18))
]

# Create the DataFrame
products_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
products_df.show()


# COMMAND ----------

from pyspark.sql.functions import when,lit,expr,col

# COMMAND ----------

products_df.filter("change_date <= '2019-08-16'").show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import rank,col

# COMMAND ----------

win_spec = Window.partitionBy("product_id").orderBy(col("change_date").desc())

# COMMAND ----------

products_df.filter("change_date <= '2019-08-16'")\
    .withColumn("ranking_date",rank().over(win_spec))\
        .show()

# COMMAND ----------

products_df.filter("change_date <= '2019-08-16'")\
    .withColumn("ranking_date",rank().over(win_spec))\
        .filter("ranking_date = 1").show()

# COMMAND ----------

new_product_df = products_df.filter("change_date <= '2019-08-16'")\
    .withColumn("ranking_date",rank().over(win_spec))\
        .filter("ranking_date = 1")\
            .select("product_id","new_price")

# COMMAND ----------

distict_product_df = products_df.select("product_id").distinct()

# COMMAND ----------

distict_product_df.join(new_product_df,"product_id","left")\
    .fillna(10.0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 35. 1204. Last Person to Fit in the Bus

# COMMAND ----------

#from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a SparkSession
#spark = SparkSession.builder.appName("QueueDataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("person_id", IntegerType(), False),
    StructField("person_name", StringType(), False),
    StructField("weight", IntegerType(), False),
    StructField("turn", IntegerType(), False)
])

# Define the data as a list of tuples
data = [
    (5, "Alice", 250, 1),
    (4, "Bob", 175, 5),
    (3, "Alex", 350, 2),
    (6, "John Cena", 400, 3),
    (1, "Winston", 500, 6),
    (2, "Marie", 200, 4)
]

# Create the DataFrame
queue_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
queue_df.show()


# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

win_spec = Window.orderBy("turn")

# COMMAND ----------

queue_df.withColumn("moving_sum",sum("weight").over(win_spec))\
    .filter("moving_sum <= 1000")\
        .tail(1)[0]["person_name"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### 36. 1907. Count Salary Categories
# MAGIC

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

data = [
    Row(id= 3, income = 108939),
    Row(id= 2, income = 12747),
    Row(id= 8, income = 87709),
    Row(id= 6, income = 91796),
]

# COMMAND ----------

salary_df = spark.createDataFrame(data)

# COMMAND ----------

salary_df.show()

# COMMAND ----------

from pyspark.sql.functions import when,count

# COMMAND ----------

salary_df.withColumn(
    "category",when(col("income")< 20000,"Low Salary")
        .when(col("income")> 50000,"High Salary")
        .otherwise("Average Salary")
    )

# COMMAND ----------

df_1 = salary_df.withColumn(
    "category",when(col("income")< 20000,"Low Salary")
        .when(col("income")> 50000,"High Salary")
        .otherwise("Average Salary")
    ).groupBy("category").agg(count("category"))

# COMMAND ----------

category_df = spark.createDataFrame(
    [("High Salary",), ("Low Salary",), ("Average Salary",)], ["category"])

# COMMAND ----------

category_df.show()

# COMMAND ----------

df_1.join(category_df,"category","right").fillna(0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 37. 1978. Employees Whose Manager Left the Company
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Define the schema for the DataFrame
schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("manager_id", IntegerType(), True),  # Allow null values for manager_id
    StructField("salary", IntegerType(), False)
])

# Define the data as a list of tuples
data = [
    (3, "Mila", 9, 60301),
    (12, "Antonella", None, 31000),  # Use None for null values
    (13, "Emery", None, 67084),
    (1, "Kalel", 11, 21241),
    (9, "Mikaela", None, 50937),
    (11, "Joziah", 6, 28485)
]

# Create the DataFrame
employees_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
employees_df.show()


# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

employees_df.dropna().alias("e1").join(employees_df.alias("e2"),expr("e1.manager_id = e2.employee_id"),"anti").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 38. 626. Exchange Seats
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField,StructField,IntegerType,StringType

# COMMAND ----------

schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("student",StringType(),True)
])

# COMMAND ----------

data = [(1, "Abbot"),(2,"Doris"),(3,"Emerson"),(4,"Green"),(5,"Jeames")]

# COMMAND ----------

strudent_df = spark.createDataFrame(data,schema)

# COMMAND ----------

strudent_df.show()

# COMMAND ----------

from pyspark.sql.functions import lead,lag,when,col,coalesce

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

win_spec = Window.orderBy("id")

# COMMAND ----------

strudent_df.withColumn("swap_student",when(
    col("id") %2 !=0,coalesce(lead("student").over(win_spec),"student"))
                       .otherwise(lag("student").over(win_spec))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 39. 1341. Movie Rating

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


# Define schemas for the three tables
movies_schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), False)
])

users_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("name", StringType(), False)
])

movie_ratings_schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("rating", IntegerType(), False),
    StructField("created_at", StringType(), False)
])

# Define data for the tables
movies_data = [
    (1, "Avengers"),
    (2, "Frozen 2"),
    (3, "Joker")
]

users_data = [
    (1, "Daniel"),
    (2, "Monica"),
    (3, "Maria"),
    (4, "James")
]

movie_ratings_data = [
    (1, 1, 3, "2020-01-12"),
    (1, 2, 4, "2020-02-11"),
    (1, 3, 2, "2020-02-12"),
    (1, 4, 1, "2020-01-01"),
    (2, 1, 5, "2020-02-17"),
    (2, 2, 2, "2020-02-01"),
    (2, 3, 2, "2020-03-01"),
    (3, 1, 3, "2020-02-22"),
    (3, 2, 4, "2020-02-25")
]

# Create DataFrames for the tables
movies_df = spark.createDataFrame(movies_data, schema=movies_schema)
users_df = spark.createDataFrame(users_data, schema=users_schema)
movie_ratings_df = spark.createDataFrame(movie_ratings_data, schema=movie_ratings_schema)

# Show the DataFrames
movies_df.show()
users_df.show()
movie_ratings_df.show()


# COMMAND ----------

movie_ratings_df = movie_ratings_df.withColumn("created_at",to_date("created_at","yyyy-MM-dd"))

# COMMAND ----------

movie_ratings_df.show()

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import col,rank

# COMMAND ----------

movie_ratings_df.groupBy("user_id")\
    .agg(count("movie_id").alias("movie_count"))\
    .join(users_df,"user_id","inner")\
        .withColumn("ranking",rank().over(Window.orderBy(col("movie_count").desc(),col("name")))).show()

# COMMAND ----------

result_1_df = movie_ratings_df.groupBy("user_id")\
    .agg(count("movie_id").alias("movie_count"))\
    .join(users_df,"user_id","inner")\
        .withColumn("ranking",rank().over(Window.orderBy(col("movie_count").desc(),col("name"))))\
            .filter("ranking=1").select(col("name").alias("result"))

# COMMAND ----------

result_1_df.show()

# COMMAND ----------

from pyspark.sql.functions import year,month,date_format

# COMMAND ----------

movie_ratings_df.show()

# COMMAND ----------

movie_ratings_df.filter((year("created_at")==2020) & (month("created_at")==2)).show()

# COMMAND ----------

movie_ratings_df.filter((year("created_at")==2020) & (date_format("created_at","MMMM")=="February")).show()

# COMMAND ----------

from pyspark.sql.functions import avg

# COMMAND ----------

movie_ratings_df.filter((year("created_at")==2020) & (date_format("created_at","MMMM")=="February"))\
    .groupBy("movie_id").agg(avg("rating").alias("avg_rating"))\
        .join(movies_df,"movie_id","inner")\
            .withColumn("ranking",rank().over(Window.orderBy(col("avg_rating").desc(),col("title"))))\
                .show()

# COMMAND ----------

result_2_df = movie_ratings_df.filter((year("created_at")==2020) & (date_format("created_at","MMMM")=="February"))\
    .groupBy("movie_id").agg(avg("rating").alias("avg_rating"))\
        .join(movies_df,"movie_id","inner")\
            .withColumn("ranking",rank().over(Window.orderBy(col("avg_rating").desc(),col("title"))))\
                .filter("ranking = 1").select(col("title").alias("result"))

# COMMAND ----------

result_2_df.show()

# COMMAND ----------

result_1_df.union(result_2_df).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 40.[Not Resolved] 1321. Restaurant Growth
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType



# Define the schema for the DataFrame
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("visited_on", StringType(), False),
    StructField("amount", IntegerType(), False)
])

# Define the data as a list of tuples
data = [
    (1, "Jhon", "2019-01-01", 100),
    (2, "Daniel", "2019-01-02", 110),
    (3, "Jade", "2019-01-03", 120),
    (4, "Khaled", "2019-01-04", 130),
    (5, "Winston", "2019-01-05", 110),
    (6, "Elvis", "2019-01-06", 140),
    (7, "Anna", "2019-01-07", 150),
    (8, "Maria", "2019-01-08", 80),
    (9, "Jaze", "2019-01-09", 110),
    (1, "Jhon", "2019-01-10", 130),
    (3, "Jade", "2019-01-10", 150)
]

# Create the DataFrame
customer_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
customer_df.show()


# COMMAND ----------

from pyspark.sql.functions import to_date,avg

# COMMAND ----------

win_spec = Window.orderBy("visited_on").rowsBetween(-6,0)

# COMMAND ----------

customer_df.withColumn("visited_on",to_date("visited_on","yyyy-MM-dd"))\
    .withColumn("moving_avg_7",avg(col("amount")).over(win_spec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 41. 602. Friend Requests II: Who Has the Most Friends

# COMMAND ----------

from pyspark.sql import Row

# Define the data as a list of Row objects
data = [
    Row(requester_id=1, accepter_id=2, accept_date='2016/06/03'),
    Row(requester_id=1, accepter_id=3, accept_date='2016/06/08'),
    Row(requester_id=2, accepter_id=3, accept_date='2016/06/08'),
    Row(requester_id=3, accepter_id=4, accept_date='2016/06/09')
]

# Create a DataFrame
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.functions import to_date

# COMMAND ----------

df = df.withColumn("accept_date",to_date("accept_date","yyyy/MM/dd"))

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import count,sum,col

# COMMAND ----------

df.groupBy("requester_id").agg(count("requester_id").alias("count"))\
    .unionAll(df.groupBy("accepter_id").agg(count("accepter_id").alias("count")))\
        .groupBy("requester_id").agg(sum("count").alias("total_count"))\
            .orderBy(col("total_count").desc())\
                .limit(1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 42. 585. Investments in 2016

# COMMAND ----------

from pyspark.sql import Row

# Define the data as a list of Row objects
data = [
    Row(pid=1, tiv_2015=10, tiv_2016=5, lat=10, lon=10),
    Row(pid=2, tiv_2015=20, tiv_2016=20, lat=20, lon=20),
    Row(pid=3, tiv_2015=10, tiv_2016=30, lat=20, lon=20),
    Row(pid=4, tiv_2015=10, tiv_2016=40, lat=40, lon=40)
]

# Create a DataFrame
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()


# COMMAND ----------

df.withColumn("count_lat_lon",count("*").over(Window.partitionBy("lat","lon")))\
    .withColumn("count_tiv_2015",count("*").over(Window.partitionBy("tiv_2015"))).show()

# COMMAND ----------

df.withColumn("count_lat_lon",count("*").over(Window.partitionBy("lat","lon")))\
    .withColumn("count_tiv_2015",count("*").over(Window.partitionBy("tiv_2015")))\
        .filter("count_lat_lon = 1 and count_tiv_2015 > 1")\
            .agg(sum("tiv_2016").alias("tiv_2016")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 43. 185. Department Top Three Salaries

# COMMAND ----------

from pyspark.sql import Row

employee_data = [
    Row(id=1, name="Joe", salary=85000, departmentId=1),
    Row(id=2, name="Henry", salary=80000, departmentId=2),
    Row(id=3, name="Sam", salary=60000, departmentId=2),
    Row(id=4, name="Max", salary=90000, departmentId=1),
    Row(id=5, name="Janet", salary=69000, departmentId=1),
    Row(id=6, name="Randy", salary=85000, departmentId=1),
    Row(id=7, name="Will", salary=70000, departmentId=1)
]

# Define the data for the Department table
department_data = [
    Row(id=1, name="IT"),
    Row(id=2, name="Sales")
]

# Create DataFrames for Employee and Department
employee_df = spark.createDataFrame(employee_data)
department_df = spark.createDataFrame(department_data)

# Show the DataFrames
employee_df.show()
department_df.show()


# COMMAND ----------

from pyspark.sql.functions import dense_rank,col

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

employee_df.limit(5).show()

# COMMAND ----------

employee_df.withColumn("ranking",dense_rank().over(Window.partitionBy("departmentId").orderBy(col("salary").desc())))\
    .filter("ranking <= 3").alias("emp")\
        .join(
            department_df.alias("dept"),col("departmentId")== department_df["id"],"inner"
            )\
            .select("dept.name","emp.name","salary").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 44. 1667. Fix Names in a Table
# MAGIC

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

data = [Row(user_id = 1, name = "aLice"),
        Row(user_id = 2, name = "bOB")]

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

from pyspark.sql.functions import initcap,lower,upper

# COMMAND ----------

df.withColumn("name",initcap("name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 45. 1527. Patients With a Condition
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import split

# Sample data
data = [(1, "Daniel", "YFEV COUGH"),
        (2, "Alice", ""),
        (3, "Bob", "DIAB100 MYOP"),
        (4, "George", "ACNE DIAB100"),
        (5, "Alain", "DIAB201")]

# Define the schema
schema = ["patient_id", "patient_name", "conditions"]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

df = df.withColumn("condition1",split("conditions"," ").getItem(0))\
    .withColumn("condition2",split("conditions"," ").getItem(1))

# COMMAND ----------

df.filter((col("condition1").like("DIAB1%"))|(col("condition2").like("DIAB1%"))).show()

# COMMAND ----------

df.filter((col("condition1").like("DIAB1%"))|(col("condition2").like("DIAB1%")))\
.drop("condition1","condition2").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 46. 196. Delete Duplicate Emails
# MAGIC

# COMMAND ----------

data = [Row(id = 1, email = "john@example.com"),
        Row(id = 2, email = "bob@example.com"),
        Row(id = 3, email = "john@example.com"),]

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

df.dropDuplicates(subset=["email"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 47. 176. Second Highest Salary
# MAGIC

# COMMAND ----------

data = [
    Row(id= 1, salary = 100),
    Row(id= 2, salary = 200),
    Row(id= 3, salary = 300)
]

# COMMAND ----------

salary_df = spark.createDataFrame(data)

# COMMAND ----------

n = input("Please enter the order")
salary_df.withColumn("ranking",dense_rank().over(Window.orderBy(col("salary").desc())))\
    .filter(f"ranking = {n}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 48. 1484. Group Sold Products By The Date
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import expr



# Define the data
data = [
    Row(sell_date="2020-05-30", product="Headphone"),
    Row(sell_date="2020-06-01", product="Pencil"),
    Row(sell_date="2020-06-02", product="Mask"),
    Row(sell_date="2020-05-30", product="Basketball"),
    Row(sell_date="2020-06-01", product="Bible"),
    Row(sell_date="2020-06-02", product="Mask"),
    Row(sell_date="2020-05-30", product="T-Shirt")
]

# Create a DataFrame
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.functions import collect_list,concat,concat_ws,col,sort_array,collect_set,size

# COMMAND ----------

df.groupBy("sell_date").agg(collect_set("product").alias("product_list"))\
    .withColumn("num_sold",size("product_list"))\
    .withColumn("sorted_list",sort_array("product_list"))\
    .withColumn("concated_list",concat_ws(",",col("sorted_list"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 49.1327. List the Products Ordered in a Period
# MAGIC

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Create a Spark session

# Define schema for the Products table
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("product_category", StringType(), False)
])

# Define schema for the Orders table
orders_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("order_date", StringType(), False),
    StructField("unit", IntegerType(), False)
])

# Define data for the Products table
products_data = [
    Row(1, "Leetcode Solutions", "Book"),
    Row(2, "Jewels of Stringology", "Book"),
    Row(3, "HP", "Laptop"),
    Row(4, "Lenovo", "Laptop"),
    Row(5, "Leetcode Kit", "T-shirt")
]

# Define data for the Orders table
orders_data = [
    Row(1, "2020-02-05", 60),
    Row(1, "2020-02-10", 70),
    Row(2, "2020-01-18", 30),
    Row(2, "2020-02-11", 80),
    Row(3, "2020-02-17", 2),
    Row(3, "2020-02-24", 3),
    Row(4, "2020-03-01", 20),
    Row(4, "2020-03-04", 30),
    Row(4, "2020-03-04", 60),
    Row(5, "2020-02-25", 50),
    Row(5, "2020-02-27", 50),
    Row(5, "2020-03-01", 50)
]

# Create DataFrames for Products and Orders
products_df = spark.createDataFrame(products_data, schema=products_schema)
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Show the DataFrames
products_df.show()
orders_df.show()


# COMMAND ----------

from pyspark.sql.functions import year,date_format,to_date,month,sum

# COMMAND ----------

orders_df.filter((year(col("order_date"))==2020) & (month("order_date")==2))\
    .groupBy("product_id").agg(sum("unit").alias("unit"))\
        .filter("unit>=100")\
            .join(products_df,"product_id","inner").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 50.[Not Resolved] 1517. Find Users With Valid E-Mails
# MAGIC
