# Databricks notebook source
a=10000000
def func(num):
    global a
    a = 10
    print(a+num)

# COMMAND ----------

print(f"Before Function execution: a is :{a}")

# COMMAND ----------

func(1)

# COMMAND ----------

print(f"After Function execution: a is :{a}")

# COMMAND ----------

globals()

# COMMAND ----------

id(func)

# COMMAND ----------

    func

# COMMAND ----------

globals()['func'](100)

# COMMAND ----------

locals()

# COMMAND ----------

import sys

# COMMAND ----------

import math

# COMMAND ----------

sys.modules

# COMMAND ----------

sys.modules['math']

# COMMAND ----------

type(math)

# COMMAND ----------

math.__name__

# COMMAND ----------

math.__dict__

# COMMAND ----------

math.__spec__

# COMMAND ----------

import fractions

# COMMAND ----------

fractions

# COMMAND ----------

fractions.__file__

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

from types import ModuleType

# COMMAND ----------

type(ModuleType)

# COMMAND ----------

isinstance(fractions,ModuleType)

# COMMAND ----------

isinstance(math,ModuleType)

# COMMAND ----------

my_mod = ModuleType("my_module")

# COMMAND ----------

my_mod.__dict__

# COMMAND ----------

my_mod.__doc__ = "This is a test module"

# COMMAND ----------

my_mod.__dict__

# COMMAND ----------

help(my_mod)

# COMMAND ----------

sys.path

# COMMAND ----------

sys.prefix

# COMMAND ----------

sys.exec_prefix

# COMMAND ----------

from pyspark.sql.functions import pow

# COMMAND ----------

type(spark)

# COMMAND ----------

spark.__dict__

# COMMAND ----------


