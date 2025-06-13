# Databricks notebook source
class Person:
    pass

# COMMAND ----------

type(Person)

# COMMAND ----------

type(type)

# COMMAND ----------

Person.__name__

# COMMAND ----------

p = Person()

# COMMAND ----------

type(p)

# COMMAND ----------

p.__class__

# COMMAND ----------

isinstance(p,Person)

# COMMAND ----------

isinstance(20,int)

# COMMAND ----------

isinstance(True,bool)

# COMMAND ----------

help(type)

# COMMAND ----------

class MyClass:
    language = "Python"
    version = "3.9.5"

# COMMAND ----------

!python --version

# COMMAND ----------

getattr(MyClass,"language")

# COMMAND ----------

getattr(MyClass,"version")

# COMMAND ----------

getattr(MyClass,"n_version")

# COMMAND ----------

getattr(MyClass,"n_version","default_value")

# COMMAND ----------

MyClass.language

# COMMAND ----------

MyClass.version

# COMMAND ----------

MyClass.nversion

# COMMAND ----------

setattr(MyClass,"version","2.7")

# COMMAND ----------

MyClass.version

# COMMAND ----------

MyClass.version = '3.9.5'

# COMMAND ----------

MyClass.version

# COMMAND ----------

MyClass.n_version = "3.9"

# COMMAND ----------

getattr(MyClass,"n_version")

# COMMAND ----------

MyClass.__dict__

# COMMAND ----------

del MyClass.n_version

# COMMAND ----------

#delattr(MyClass,"n_version")

# COMMAND ----------

MyClass.__dict__

# COMMAND ----------

class Program:
    language = "Python"

    def say_hello():
        print(f"hello from {Program.language}")

# COMMAND ----------

my_program = Program()

# COMMAND ----------

type(my_program)

# COMMAND ----------

isinstance(my_program,Program)

# COMMAND ----------

my_program.__dict__

# COMMAND ----------

Program.__dict__

# COMMAND ----------

my_program.__class__

# COMMAND ----------

my_program.language

# COMMAND ----------

my_program.__dict__

# COMMAND ----------

my_program.language = "scala"

# COMMAND ----------

Program.__dict__

# COMMAND ----------

my_program.__dict__

# COMMAND ----------

Program.say_hello

# COMMAND ----------

my_program.say_hello

# COMMAND ----------

Program.say_hello()

# COMMAND ----------

my_program.say_hello()

# COMMAND ----------

class Program1:
    language = "Python"
    def say_hello(obj):
        print("hello Arin!")

# COMMAND ----------

p = Program1()

# COMMAND ----------

p.say_hello()

# COMMAND ----------

class Person:
    def say_hello():
        print("Hello!")

# COMMAND ----------

Person.say_hello

# COMMAND ----------

type(Person.say_hello)

# COMMAND ----------

Person.__dict__

# COMMAND ----------

p =Person()

# COMMAND ----------

p.__dict__

# COMMAND ----------

id(p)

# COMMAND ----------

hex(id(p))

# COMMAND ----------

p.say_hello

# COMMAND ----------

type(p.say_hello)

# COMMAND ----------

type(Person.say_hello)

# COMMAND ----------

p.say_hello()

# COMMAND ----------

class Person:
    def say_hello(*args):
        print(f"Argument passed: {args}")


# COMMAND ----------

p = Person()

# COMMAND ----------

Person.say_hello()

# COMMAND ----------

p.say_hello()

# COMMAND ----------

class Person:
    def set_name(obj,new_name):
        obj.name = new_name
        #setattr(obj,"name",new_name)

# COMMAND ----------

p = Person()

# COMMAND ----------

p.set_name("Arin")

# COMMAND ----------

p.name

# COMMAND ----------

p.__dict__

# COMMAND ----------

Person.__dict__

# COMMAND ----------

from types import MethodType

# COMMAND ----------

class Employee:
    name = "Arin"

# COMMAND ----------

Employee.__dict__

# COMMAND ----------

e1 = Employee()

# COMMAND ----------

e1.__dict__

# COMMAND ----------

e1.print_name = lambda : "Employee_name"

# COMMAND ----------

type(e1.print_name)

# COMMAND ----------

e1.print_name()

# COMMAND ----------

e1.print_name = MethodType(lambda self: f"Employee_name is {self.name}",e1) 

# COMMAND ----------

e1.__dict__

# COMMAND ----------

hex(id(e1))

# COMMAND ----------

Employee.__dict__

# COMMAND ----------

class Teacher:
    def __init__(self,name,subject):
        self.name = name
        self.subject = subject
    def _register_method(self):
        self.teacher_job = MethodType(lambda self : f"{self.name} teachs {self.subject}",self)
    def _teachs_(self):
        get_job = getattr(self,"teacher_job",None)
        if get_job:
            print(f"{self.teacher_job()}")
        else:
            raise AttributeError("Please register method")

# COMMAND ----------

teacher1 = Teacher("Asit Baran Maity","Math")

# COMMAND ----------

Teacher.__dict__

# COMMAND ----------

teacher1.__dict__

# COMMAND ----------

teacher1._teachs_()

# COMMAND ----------

teacher1._register_method()

# COMMAND ----------

teacher1.__dict__

# COMMAND ----------

teacher1._teachs_()

# COMMAND ----------

teacher2 = Teacher("Arin","AI")

# COMMAND ----------

teacher2.__dict__

# COMMAND ----------

teacher2._register_method()

# COMMAND ----------

teacher2.__dict__

# COMMAND ----------

teacher2.teacher_job()

# COMMAND ----------

teacher1.teacher_job()

# COMMAND ----------

teacher2._teachs_()

# COMMAND ----------


