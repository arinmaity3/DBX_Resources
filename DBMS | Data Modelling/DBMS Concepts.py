# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Stored Procedure
# MAGIC - A prepared SQL code which can be saved and reused.
# MAGIC - https://www.youtube.com/watch?v=dC5aa-8NP04&t=58s

# COMMAND ----------

# MAGIC %sql
# MAGIC --Synatax
# MAGIC CREATE PROCEDURE procedure_name
# MAGIC AS
# MAGIC  Sql_statement
# MAGIC  GO;
# MAGIC
# MAGIC  EXEC procedure_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Synatax with Parameter
# MAGIC CREATE PROCEDURE procedure_name
# MAGIC @param1 data-type, @param2 data-type 
# MAGIC AS
# MAGIC Sql_statement
# MAGIC GO;
# MAGIC
# MAGIC EXEC procedure_name @param=value1,@param=value2

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Index:

# COMMAND ----------

# MAGIC %md
# MAGIC 1.	Clustered
# MAGIC 2.	Non-Clustered
# MAGIC 3.	Unique
# MAGIC 4.	Filtered
# MAGIC 5.	XML
# MAGIC 6.	Full text
# MAGIC 7.	Spatial
# MAGIC 8.	Columnstore
# MAGIC 9.	Index with included columns
# MAGIC 10.	Index on computed columns
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - https://www.youtube.com/watch?v=YuRO9-rOgv4
# MAGIC - https://www.youtube.com/watch?v=NGslt99VOCw
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Example:

# COMMAND ----------

data = [
    (1,"Ram",2500,"Male"),
    (2,"Pam",4000,"Female"),
    (3,"Sam",6000,"Female"),
    (4,"Tam",1000,"Male")
]

# COMMAND ----------

schema = ["ID","Name","Salary","Gender"]

# COMMAND ----------

df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tbl_Employee
# MAGIC WHERE salary >5000 and Salary <7000
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Table Scan:
# MAGIC Whenever there is no index to help query.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Syntax for creating Index:
# MAGIC Create Index IX_tbl_Employee_Salary
# MAGIC ON tbl_Employee (Salary ASC)
# MAGIC
# MAGIC -- Inbuilt stored procedure for accessing all the indexes for a table:
# MAGIC  Sp_Helpindex tbl_Employee
# MAGIC
# MAGIC -- For Dropping index:
# MAGIC Drop index table_name.index_name
# MAGIC -- https://www.youtube.com/watch?v=i_FwqzYMUvk&list=PL08903FB7ACA1C2FB&index=36
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC Normalization is a database design technique used to organize data efficiently and reduce data redundancy while maintaining data integrity. The primary goal of normalization is to structure the database in such a way that it minimizes data anomalies, improves data consistency, and makes it easier to maintain and update the database over time.
# MAGIC
# MAGIC Normalization is typically carried out through a series of steps, each represented by a normal form (NF). There are several normal forms, with the most common ones being the first three: 1NF, 2NF, and 3NF.
# MAGIC
# MAGIC 1. First Normal Form (1NF):
# MAGIC    - In 1NF, a table is considered normalized if it meets the following criteria:
# MAGIC      - It has a primary key that uniquely identifies each row in the table.
# MAGIC      - All columns contain atomic (indivisible) values. This means that each cell of the table must contain a single value, not a list or a composite value.
# MAGIC
# MAGIC 2. Second Normal Form (2NF):
# MAGIC    - A table is in 2NF if it is already in 1NF and additionally:
# MAGIC      - It has no partial dependencies. This means that non-key attributes depend on the entire primary key, not just a part of it.
# MAGIC    
# MAGIC 3. Third Normal Form (3NF):
# MAGIC    - A table is in 3NF if it is already in 2NF and additionally:
# MAGIC      - It has no transitive dependencies. This means that non-key attributes should not depend on other non-key attributes.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Keys

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Primary Key: 
# MAGIC A primary key is a column or set of columns in a table that uniquely identifies each row (record) in that table.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Foreign Key:
# MAGIC A foreign key is a column or set of columns in one table that is used to establish a link between the data in two tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Super Key:
# MAGIC A super key is a set of one or more columns that can uniquely identify rows within a table.
# MAGIC It's a broader concept than a primary key, as it may contain more columns than necessary to uniquely identify records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Candidate Key:
# MAGIC A candidate key is a minimal super key, meaning it is a super key with no unnecessary columns.
# MAGIC It's a potential choice for a primary key.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Alternate Key / Secondary Key:
# MAGIC An alternate key is a candidate key that is not selected as the primary key.
# MAGIC It can be unique but is not used as the primary means of identifying records.

# COMMAND ----------

# MAGIC %md
# MAGIC #####Natural Key:
# MAGIC
# MAGIC A natural key is a key that is derived from the data itself and has inherent meaning.
# MAGIC It is based on real-world attributes or characteristics of the data.

# COMMAND ----------

# MAGIC %md
# MAGIC #####Surrogate Key:
# MAGIC
# MAGIC A surrogate key is an artificial, system-generated key that is used as a substitute for a natural key.

# COMMAND ----------

# MAGIC %md
# MAGIC It has no inherent meaning and is often an integer or GUID (Globally Unique Identifier).
# MAGIC Surrogate keys are particularly useful when dealing with composite or lengthy natural keys.
# MAGIC Example: Using an auto-incremented integer as a surrogate key in a customer table.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. DDL vs DML vs DCL
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DDL(Data Definition Language):
# MAGIC  deals with defining and modifying the structure of the database.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE: Used to create database objects like tables, indexes, views, etc.
# MAGIC CREATE TABLE employees (
# MAGIC     id INT PRIMARY KEY,
# MAGIC     name VARCHAR(50),
# MAGIC     department VARCHAR(50)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALTER: Modifies the structure of existing database objects.
# MAGIC --Adding a column
# MAGIC ALTER TABLE table_name
# MAGIC ADD column_name datatype;
# MAGIC
# MAGIC --Modifying a Column (Changing Data Type):
# MAGIC ALTER TABLE table_name
# MAGIC ALTER COLUMN column_name new_datatype;
# MAGIC
# MAGIC -- Modifying a View (Changing the Definition):
# MAGIC ALTER VIEW view_name AS
# MAGIC SELECT column1, column2, ...
# MAGIC FROM new_tables_or_conditions;
# MAGIC
# MAGIC -- Modifying a Stored Procedure:
# MAGIC ALTER PROCEDURE procedure_name
# MAGIC AS
# MAGIC BEGIN
# MAGIC     -- Updated logic or changes
# MAGIC     ...
# MAGIC END;
# MAGIC
# MAGIC -- Modifying a Function:
# MAGIC ALTER FUNCTION function_name
# MAGIC RETURNS return_datatype
# MAGIC AS
# MAGIC BEGIN
# MAGIC     -- Updated logic or changes
# MAGIC     ...
# MAGIC END;
# MAGIC
# MAGIC -- Modifying a Constraint:
# MAGIC ALTER TABLE table_name
# MAGIC DROP CONSTRAINT constraint_name;
# MAGIC
# MAGIC ALTER TABLE table_name
# MAGIC ADD CONSTRAINT constraint_name
# MAGIC     FOREIGN KEY (column_name) 
# MAGIC     REFERENCES other_table(other_column);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP: Deletes database objects.
# MAGIC
# MAGIC DROP TABLE table_name;
# MAGIC
# MAGIC DROP VIEW view_name;
# MAGIC
# MAGIC DROP PROCEDURE procedure_name;
# MAGIC
# MAGIC DROP FUNCTION function_name;
# MAGIC
# MAGIC DROP INDEX index_name ON table_name;
# MAGIC
# MAGIC -- First set the database to single user mode
# MAGIC -- Then drop the database
# MAGIC ALTER DATABASE database_name SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
# MAGIC DROP DATABASE database_name;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE: Removes all records from a table, but keeps the table structure intact.
# MAGIC
# MAGIC TRUNCATE TABLE table_name;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DML(Data Manipulation Language):
# MAGIC  handles the manipulation of data stored within the database.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT: Retrieves data from one or more tables.
# MAGIC SELECT * FROM table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT: Adds new records into a table.
# MAGIC -- Inserting multiple rows into a 'users' table
# MAGIC INSERT INTO table_name (name, age)
# MAGIC VALUES
# MAGIC ('Alice', 25),
# MAGIC ('Bob', 30),
# MAGIC ('Charlie', 28);
# MAGIC -----------------------------------------------
# MAGIC INSERT INTO table_name (column1, column2, ...)
# MAGIC SELECT value1_1, value1_2, ...
# MAGIC UNION ALL
# MAGIC SELECT value2_1, value2_2, ...
# MAGIC UNION ALL
# MAGIC ...
# MAGIC SELECT valuen_1, valuen_2, ...;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE: Modifies existing records in a table.
# MAGIC
# MAGIC UPDATE employees
# MAGIC SET salary = 60000, department = 'IT'
# MAGIC WHERE employee_id = 101;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE: Removes records from a table.
# MAGIC
# MAGIC DELETE FROM employees
# MAGIC WHERE employee_id = 101;
# MAGIC
# MAGIC -- If you want to delete all records from the employees table, you can execute:
# MAGIC DELETE FROM employees;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DCL(Data Control Language):
# MAGIC  manages the permissions and access rights of database users to control security and access to data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GRANT: Gives specific privileges to database users or roles.
# MAGIC GRANT SELECT, INSERT ON employees TO sales_role;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVOKE: Removes specific privileges from database users or roles.
# MAGIC REVOKE privilege_type
# MAGIC ON object_name
# MAGIC FROM {user_name | role_name | PUBLIC};
# MAGIC
# MAGIC
# MAGIC -- REVOKE: Keyword indicating the revocation of privileges.
# MAGIC -- privilege_type: The type of privilege or permission that is being revoked (e.g., SELECT, INSERT, UPDATE, DELETE, ALL, etc.).
# MAGIC -- ON object_name: Specifies the object (table, view, procedure, etc.) from which the privileges are being revoked.
# MAGIC -- FROM {user_name | role_name | PUBLIC}: Specifies the user, role, or the PUBLIC keyword (which refers to all users) from which the privileges are being revoked.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. View vs Materelized View

# COMMAND ----------

# Views are virtual representations generated dynamically from queries, providing a logical abstraction over tables, while materialized views are physical copies of query results stored in the database, offering performance benefits but requiring maintenance for data synchronization.
# Views are suitable for dynamic data retrieval and simplified access, while materialized views are used for improving query performance by storing precomputed data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Procedure vs Function

# COMMAND ----------

# Return Value: Functions return values; procedures may not return a value explicitly.
# Usage in Queries: Functions can be used within SQL queries; procedures are executed as standalone statements.
# Data Modification: Functions are read-only (with exceptions), while procedures can modify data.
# Reusability: Functions can be reused in various SQL statements; procedures encapsulate a sequence of SQL statements and business logic.
