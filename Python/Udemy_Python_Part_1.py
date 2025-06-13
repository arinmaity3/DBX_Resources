# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Argument vs Parameter

# COMMAND ----------

 #Parameters: x,y
 def func(x,y):
     pass

# COMMAND ----------

#Arguments : a,b
a,b= 2,3
func(a,b)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Positional and Keyword Arguments

# COMMAND ----------

def my_func1(a,b=100):
    return a+b

# COMMAND ----------

my_func1(10,20)

# COMMAND ----------

my_func1(10)

# COMMAND ----------

def func_3(a,b=20,c):
    return a+b+c

# COMMAND ----------

def func_3(a,b=20,c=10):
    return a+b+c

# COMMAND ----------

#Keyword / Named Argument. In this case 10 is positional argument, and c is keyword arg
func_3(10,c=300)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Unpacking Iterables

# COMMAND ----------

a = 1,2,3

# COMMAND ----------

type(a)

# COMMAND ----------

b= (1)

# COMMAND ----------

type(b)

# COMMAND ----------

c= 1,

# COMMAND ----------

type(c)

# COMMAND ----------

d = (2,)

# COMMAND ----------

type(d)

# COMMAND ----------

e = ()

# COMMAND ----------

type(e)

# COMMAND ----------

f = tuple()

# COMMAND ----------

type(f)

# COMMAND ----------

a,b,c = "1234"

# COMMAND ----------

a,*b,c = "1234"

# COMMAND ----------

a,b,c

# COMMAND ----------

a,b = 10,20

# COMMAND ----------

a ="Hello orld"

# COMMAND ----------

" w".join(a.split())

# COMMAND ----------

l = [1,2,3,4,5,6]
first,rest = l[0],l[1:]

# COMMAND ----------

first,rest

# COMMAND ----------

f,*r = l

# COMMAND ----------

print(f"f = {f} and r = {r}")

# COMMAND ----------

a,*b = {1,2,3}

# COMMAND ----------

a= [1,2,3]
b=[10,20,30]

# COMMAND ----------

a.extend(b)
print(a)

# COMMAND ----------

a= [1,2,3]
b=[10,20,30]

# COMMAND ----------

print([a,b])
print([*a,*b])

# COMMAND ----------

a,*b,(c,d,e) = [1,2,3,"xyz"]

# COMMAND ----------

print(f" a={a}\n b={b}\n c={c}\n d={d}\n e={e}")

# COMMAND ----------

l =list(range(1,7))

# COMMAND ----------

first,rest = l[0],l[1:]
print(f"first= {first} \n rest = {rest}")

# COMMAND ----------

first,*rest = l
print(f"first= {first} \n rest = {rest}")

# COMMAND ----------

s = {1,2,3}

# COMMAND ----------

first,rest= s[0],s[1:]

# COMMAND ----------

first,*rest = s
print(f"first= {first} \n rest = {rest}")

# COMMAND ----------

a,*b = "Python"
print(f"a= {a} \n b = {b}")

# COMMAND ----------

# *b always result in list format

# COMMAND ----------

a= "Python"
print(a[0],a[1],a[2:-2],a[-2],a[-1])
f,s,*m,sl,l = a
print(f,s,*m,sl,l)

# COMMAND ----------

s1 = {1,2,3}
s2 = {4,5,6}

# COMMAND ----------

{*s1,*s2}

# COMMAND ----------

help(set)

# COMMAND ----------

s1.union(s2)

# COMMAND ----------

print(s1)
print(s2)

# COMMAND ----------

d1 = {'key1':1,'key2':2,'key3':3}
d2 = {'key1':3,'key3':4,'key4':5}

# COMMAND ----------

a,*b = d1

# COMMAND ----------

print(f"a = {a} \n b = {b}")

# COMMAND ----------

[*d1,*d2]

# COMMAND ----------

{**d1,**d2}

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. *args

# COMMAND ----------

def func(*a):
    print(sum(a))
    return a

# COMMAND ----------

func(1,2,3,4,5)

# COMMAND ----------

func(1)

# COMMAND ----------

func()

# COMMAND ----------

def cal_avg(*args):
    # if args:
    #     return(sum(args)/len(args))
    # return 0
    return len(args) and sum(args)/len(args)

# COMMAND ----------

cal_avg(1,2,3,4,5,6,7,8,9,10)

# COMMAND ----------

cal_avg()

# COMMAND ----------

cal_avg([1,2,3,4,5,6,7,8,9,10])

# COMMAND ----------

cal_avg(*[1,2,3,4,5,6,7,8,9,10])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Keyword Arguments

# COMMAND ----------

#Docstring

# COMMAND ----------

def num_sum(*args):
    '''This function can be used to sum all the inputs.
    This is second line
    '''
    return sum(args)

# COMMAND ----------

num_sum(1,2,3,4)

# COMMAND ----------

help(num_sum)

# COMMAND ----------

print(num_sum.__doc__)

# COMMAND ----------

def new_func(a:int,b:str)->list:
    return [a,b]

# COMMAND ----------

new_func(1,"Arin")

# COMMAND ----------

new_func.__annotations__

# COMMAND ----------

help(new_func)

# COMMAND ----------

power = lambda x:x**2

# COMMAND ----------

type(power)

# COMMAND ----------

power(100)

# COMMAND ----------

a = list(range(10))

# COMMAND ----------

a

# COMMAND ----------

set(map(power,a))

# COMMAND ----------

def func(x):
    return x**2

# COMMAND ----------

type(func)

# COMMAND ----------

func

# COMMAND ----------

func  = lambda x=10 : x**2

# COMMAND ----------

func()

# COMMAND ----------

func(5)

# COMMAND ----------

type(func)

# COMMAND ----------

func

# COMMAND ----------

id(func)

# COMMAND ----------

f = func

# COMMAND ----------

id(f) , id(func)

# COMMAND ----------



# COMMAND ----------

new_func = lambda pos,*args,**kwargs: (pos,args,kwargs)

# COMMAND ----------

new_func()

# COMMAND ----------

new_func(1,"Arin","Papai",a=1,b="2")

# COMMAND ----------

help(sorted)

# COMMAND ----------

char_list = ['D','a',"E",'B','c']

# COMMAND ----------

sorted(char_list)

# COMMAND ----------



# COMMAND ----------

sorted(char_list,key=lambda x :x.upper())

# COMMAND ----------

dict1 = {'z':1,'x':3,'y':2}

# COMMAND ----------

sorted(dict1)

# COMMAND ----------

sorted(dict1,key= lambda k : dict1[k])

# COMMAND ----------

sorted(dict1.items(),key= lambda x:x[1])

# COMMAND ----------

m = sorted(dict1.items(),key= lambda x:x[1])

# COMMAND ----------

type(m)

# COMMAND ----------

dict(sorted(dict1.items(),key= lambda x:x[1]))

# COMMAND ----------

s_list = ["baaaa","asnsnsz","caay"]

# COMMAND ----------

sorted(s_list)

# COMMAND ----------

sorted(s_list,key=lambda s: s[-1])

# COMMAND ----------

from random import random,randint

# COMMAND ----------

randint(1,10000)

# COMMAND ----------

random()

# COMMAND ----------

a = [1,2,3,4,5,6,7,8]

# COMMAND ----------

sorted(a,key= lambda x :random())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Function Introspection

# COMMAND ----------

def my_func(a,b):
    return a+b

# COMMAND ----------

dir(my_func)

# COMMAND ----------

my_func.catagory = "math"
my_func.subcatagory = "arithmatic"

# COMMAND ----------

my_func.__dict__

# COMMAND ----------

print(my_func.catagory,my_func.subcatagory)

# COMMAND ----------

my_func.__name__

# COMMAND ----------

new_func = my_func

# COMMAND ----------

id(new_func),id(my_func)

# COMMAND ----------

new_func(2,3)

# COMMAND ----------

new_func.__name__

# COMMAND ----------

a =1

# COMMAND ----------

isinstance(a,int)

# COMMAND ----------

callable(print)

# COMMAND ----------

callable(int)

# COMMAND ----------

callable(10)

# COMMAND ----------


