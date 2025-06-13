# Databricks notebook source
df = spark.read.options(header=True,inferSchema=True).csv('dbfs:/FileStore/practise/walmart_stock.csv')

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.take(5))

# COMMAND ----------

display(df.head(5))

# COMMAND ----------

display(df.tail(5))

# COMMAND ----------

for line in df.head(5):
    print(line)

# COMMAND ----------

df.describe().display()

# COMMAND ----------

df1=df.describe()

# COMMAND ----------

df_dt=df1.select(
    df1['summary'],df1['open'].cast("float"),df1['high'].cast("float"),df1['low'].cast("float"),df1['close'].cast("float"),df1['volume'].cast("float"),df1['Adj Close'].cast("float"))

# COMMAND ----------

df_dt.dtypes

# COMMAND ----------

numeric_column_list = [col_name for col_name,col_dtype in df_dt.dtypes if col_dtype in["float"]]

# COMMAND ----------

numeric_column_list

# COMMAND ----------

from pyspark.sql.functions import col,round

# COMMAND ----------

df_dt.select(df_dt['summary'],*[round(col(col_name),2).alias(col_name + "_rounded") for col_name in numeric_column_list]).display()

# COMMAND ----------

display(df.head(5))


# COMMAND ----------

df_new = df.select((df['high']/df['volume']).alias('HV Ratio')).show()

# COMMAND ----------

display(df.sort(df['high'].desc()).select('date','high').head(1))

# COMMAND ----------

df.sort(df['high'].desc()).select('date','high').head(1)[0]['date']

# COMMAND ----------

from pyspark.sql.functions import avg,sum,mean

# COMMAND ----------

df.select(avg(df['close'])).display()

# COMMAND ----------

df.select(mean('close').alias('mean_close')).display()

# COMMAND ----------

df.select(sum(df['close'])).display()

# COMMAND ----------

from pyspark.sql.functions import max,min

# COMMAND ----------

df.select(max('volume').alias('max_volume'),min(df['volume']).alias('min_volume')).display()

# COMMAND ----------

df.filter(df['close']<60).display()

# COMMAND ----------

df.filter(df['close']<60).count()

# COMMAND ----------

df.filter(df['high']>80).count()*100 / df.count()

# COMMAND ----------

df.corr('high','volume')

# COMMAND ----------

from pyspark.sql.functions import corr

# COMMAND ----------

df.select(corr(df['High'], df['Volume'])).display()

# COMMAND ----------

from pyspark.sql.functions import year

# COMMAND ----------

df.columns

# COMMAND ----------

df.select(year(df['Date'])).show()

# COMMAND ----------

from pyspark.sql.functions import asc,col

# COMMAND ----------

df.groupBy(year(df['date']).alias('year')).agg(max(df['high']).alias('max_high'),min(df['high']).alias('min_high')).sort(col('year').asc()).display()

# COMMAND ----------

from pyspark.sql.functions import month,avg

# COMMAND ----------

df.groupBy(month(df['date'])).agg(avg(df['close'])).display()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

df.groupBy(date_format(df['date'],'MMM').alias('Month')).avg('close').show()

# COMMAND ----------

data = [("Arin","arinmaity@ind.tcs.com",10000),("Papai","papai123@uk.bt.org",5000000)]
column = ['Name','Email','Salary']
new_df = spark.createDataFrame(data,column)
new_df.withColumn("user_id",split(new_df['Email'],'@')[0])\
        .withColumn("country",split(split(new_df['Email'],'@')[1],'\.')[1])\
            .display()
