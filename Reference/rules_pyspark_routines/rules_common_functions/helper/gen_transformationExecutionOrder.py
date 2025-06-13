# Databricks notebook source
from pyspark.sql.functions import row_number,lit,col,when,split,explode,expr,count,sum,lower
from pyspark.sql.types import StructType,StructField, ShortType
from pyspark.sql import functions as F


class gen_transformationExecutionOrder():
  @staticmethod
  def transformationExecutionOrder_get(lstOfScopedAnalytics,ERPSystemID):
      try:
          dicOfExecutionOrder = {}
          dicOfHierachy = {}
          level = 1
          isNonExecutable = True

          objGenHelper = gen_genericHelper()

          #get the mandatory routines to be executed based on scoped analytics
          lstOfDependentRoutines = objGenHelper.mandatoryRoutines_get(lstOfScopedAnalytics)
          lstOfScopedAnalytics = lstOfScopedAnalytics + lstOfDependentRoutines

          #read the list of routines needs to be excluded from the dependency          
          try:
              dbutils.fs.ls(gl_metadataPath + "dic_ddic_nonExecutableRoutines.csv")
          except Exception as e:
              isNonExecutable = False
              pass

          if(isNonExecutable == True):
              dfNonExecutableRoutines = objGenHelper.gen_readFromFile_perform(gl_metadataPath + "dic_ddic_nonExecutableRoutines.csv").\
                                        select(col("ERPSystemID"),\
                                              col("objectName"),\
                                              split(col("auditProcedure"),",").alias("auditProcedure")).\
                                        filter(col("ERPSystemID") == ERPSystemID)
                            
              dfNonExecutableRoutines = dfNonExecutableRoutines.select(col("ERPSystemID"),\
                                          col("objectName"),\
                                          explode(col("auditProcedure")).alias("auditProcedure")).\
                                          filter(col("auditProcedure").isin(lstOfScopedAnalytics))
                    
          
          #read the exectuion order from ADLS
          dfDependencyList = objGenHelper.gen_readFromFile_perform(gl_metadataPath + "dic_ddic_executionDependency.csv").\
                                 select(col("ERPSystemID"),\
                                 col("parentMethod"),\
                                 col("childMethod"),\
                                 col("id"),\
                                 col("parameters"),\
                                 split(col("auditProcedure"),",").alias("auditProcedure")).\
                                 filter(col("ERPSystemID") == ERPSystemID)
          
          #Filter out the non executables
          if(isNonExecutable == True):
              dfDependencyList = dfDependencyList.alias("A").\
                             join(dfNonExecutableRoutines.alias("B"),\
                                  [(col("A.ERPSystemID") == col("B.ERPSystemID"))\
                                  & (col("A.parentMethod") == col("B.objectName"))],how='left').\
                             join(dfNonExecutableRoutines.alias("C"),\
                                  [(col("A.ERPSystemID") == col("C.ERPSystemID"))\
                                   & (col("A.parentMethod") == col("C.objectName"))],how='left').\
                             filter((col("C.objectName").isNull()) | (col("B.objectName").isNull())).\
                             select(col("A.*"))
                   
          #bring the execution order in audit procedure level and identify the first level
          dfDependencyOrder = dfDependencyList.select(col("parentMethod"),\
                              col("childMethod"),col("id"),\
                              col("parameters"),\
                              explode(col("auditProcedure")).alias("auditProcedure")).\
                              filter(col("auditProcedure").isin(lstOfScopedAnalytics)).\
                              select(col("parentMethod"),col("childMethod"),\
                              col("id").cast("int"),\
                              col("parameters")).distinct().\
                              withColumn("hierarchy", expr("CASE WHEN childMethod IS NULL THEN  1 ELSE 0 END"))
          
          #generate the child IDs based on the parent IDs
          dfExecutionOrder = dfDependencyOrder.alias("A").\
                               join(dfDependencyOrder.alias("B"),\
                               [col("B.parentMethod") == col("A.childMethod")],how='left').\
                               select(col("A.id").alias("parentID"),col("A.parentMethod")
                               ,when(col('B.id').isNull() == True,lit(0)).otherwise(col("B.id")).alias("childID")\
                               ,when(col('B.parentMethod').isNull() == True,lit('')).otherwise(col("B.parentMethod"))\
                               .alias("childMethod")).distinct()

    
          #prepare the execution dependency as dictionary of values
          for i in dfExecutionOrder.groupBy(col("parentID")).\
              agg(F.collect_set("childID")).alias("childID").collect():
              dicOfExecutionOrder[i[0]] = sorted(i[1])

          #populate the first level hierchies
          for k,v in dicOfExecutionOrder.items():
              if(v == [0]):
                  dicOfHierachy[k] = level

          #collect first level hierachies and parent ids

          setOfHierachy = set(dicOfHierachy.keys())
          setOfRoutines = set(dicOfExecutionOrder.keys())

          while(1==1):
              level = level + 1
              if(setOfRoutines == setOfHierachy):
                  break
              for k,v in dicOfExecutionOrder.items():
                 if(k not in (dicOfHierachy.keys())):
                     if(set(v) == setOfHierachy.intersection(set(v))):
                         dicOfHierachy[k] = level  
              setOfHierachy = set(dicOfHierachy.keys())
          
          depSchema = StructType([StructField("id",ShortType(),False),
                        StructField("hierarchy",ShortType(),False)])

          dfExecutionHierarchy = spark.createDataFrame(data = list(dicOfHierachy.items()), schema = depSchema)
         
          return dfDependencyOrder.select(col("id").alias("parentID"),\
                             col("parentMethod").alias("objectName"),\
                             col("parameters")).distinct().\
                             join(dfExecutionHierarchy,\
                             col("id") == col("parentID"),how='inner').\
                             select(col("objectName"),\
                             when(col("parameters").isNull() == True,lit("")).\
                                    otherwise(col("parameters")).alias("parameters"),\
                             col("hierarchy").cast("int"))       
      except Exception as err:
          raise

