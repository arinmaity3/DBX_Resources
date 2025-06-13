# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.Unpacking

# COMMAND ----------

list1 = [1,2,3,4,5,6,7,8]
first,second,*mid,end = list1
print("first is: ",first)
print("second is:",second)
print("mid is:",mid)
print("*mid is:",*mid)
print("end is",end)

# COMMAND ----------

list1 = [1,2,3,4,5,6,7,8]
*_, = list1
print(_)
print(*_)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Decorators

# COMMAND ----------

#Demo1
def my_decorator(function):
    def wrapper():
        print("I am decorating the function")
        function()
    return wrapper

@my_decorator
def print_hello():
    print("Hello Arin!")

# COMMAND ----------

print_hello()

# COMMAND ----------

#Demo2 : Limitation
def my_decorator(function):
    def wrapper():
        print("I am decorating the function")
        function()
    return wrapper

@my_decorator
def print_hello(name):
    print(f"Hello {name}!")

# COMMAND ----------

print_hello("name")

# COMMAND ----------

#Fixing Function Parameter problem
def my_decorator(function):
    def wrapper(*args,**kwargs):
        print("I am decorating the function")
        function(*args,**kwargs)
    return wrapper

@my_decorator
def print_hello(name):
    print(f"Hello {name}!")

# COMMAND ----------

print_hello("Arin")

# COMMAND ----------

#Demo 3: Practical example using logging
import time
def logged(function):
    def wrapper(*args,**kwargs):
        before = time.time()
        value = function(*args,**kwargs)
        after = time.time()
        print(f"Function \"{function.__name__}\"has been executed in {after - before}.")
        return value
    return wrapper

@logged
def add(a,b):
    return a+b

# COMMAND ----------

add(3,4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Generators

# COMMAND ----------

def my_generator(n):
    for i in range(n):
        yield(i**3)

# COMMAND ----------

values = my_generator(90000000)
print(next(values))
print(next(values))
print(next(values))
print(next(values))
print(next(values))
print(next(values))

# COMMAND ----------

#Demo 1 : Generating Infinite Sequence

# COMMAND ----------

def infinite_seq():
    result = 1
    while True:
        yield(result)
        result += 1

# COMMAND ----------

values = infinite_seq()
# print(next(values))
# print(next(values))
# print(next(values))

# COMMAND ----------

# for i in values:
#     print(i)
#     if i == 100:
#         break

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. How does "Set" work
# MAGIC Ref: https://www.youtube.com/watch?v=Gp-qih4T9tA

# COMMAND ----------

fruitList = ["Apple","Orange","Orange","Mango","Apple"]

# COMMAND ----------

fruit_hash_vals = list((map(hash,fruitList)))

# COMMAND ----------

fruit_hash_vals

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. *Args and **Kwargs
# MAGIC Ref: https://www.youtube.com/watch?v=4jBJhCaNrWU

# COMMAND ----------

def pizza_details(size,*toppings,**delivery):
    print("size:",size)
    print("toppings:", toppings)
    print("delivery details: ",delivery)

# COMMAND ----------

pizza_details("large","olive","corn","mushroom","pineapple",delivery_guy="Arnab",tip=20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Scopes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Ref: https://www.youtube.com/watch?v=38uGbVYICwg&list=TLPQMjYwODIwMjOiicZSXQ2sTA&index=13
# MAGIC 1. Built - In
# MAGIC 2. Global
# MAGIC 3. Local
# MAGIC 4. Non- Local

# COMMAND ----------

#Demo 1
x = 1
def func():
    print(x)
    x = 2
func()
#A:1
#B:2
#C:Error

# COMMAND ----------

#Demo 2
x = 1
def func():
    x=100
    print("In Function",x)
func()
print("golbal scope",x)

# COMMAND ----------

#Demo 3: In order to change global variable
x = 1
def func():
    global x
    x =100
    print("In Function",x)
func()
print("golbal scope",x)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. extend | append

# COMMAND ----------

# MAGIC %md
# MAGIC - Use append() when you want to add a single element to the end of a list.
# MAGIC - Use extend() when you want to add multiple elements from an iterable to the end of a list.

# COMMAND ----------

a =[1,2,3]
b = [10,20,30]

# COMMAND ----------

a.extend(b)
print(a)

# COMMAND ----------

a =[1,2,3]
b = [10,20,30]

# COMMAND ----------

a.append(b)
print(a)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Exception
# MAGIC
