# Databricks notebook source
from pyspark.sql.functions import md5,expr, col,window,desc,asc,row_number,collect_list
import uuid
import sys
import traceback

def app_removeDuplicateRecords_perform(schemaName,tableName,filePath,executionID):
  try:
    
    objGenHelper = gen_genericHelper()   

    logID = executionLog.init(PROCESS_ID.CLEANSING,\
                              validationID = VALIDATION_ID.REMOVE_DUPLICATE,
                              schemaName   = schemaName,
                              tableName    = tableName,
                              executionID  = executionID) 

    stgTableName = schemaName + "_" + tableName
    keyColumnView =  uuid.uuid4().hex
    duplicateRecords = 0
    dfDuplicateRecords = None
    
    try:      
      dfSource = spark.read.table(stgTableName)      
    except Exception as err:
      try:
        dfSource = objGenHelper.gen_readFromFile_perform(filePath)
      except Exception as fileNotFound:
        executionStatus = "File not found '" + stgTableName + "'"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return

    #get the key columns, order by columns, partition columns
    
    dfKeyColumns = df_RD_KeyColumns.alias("C").filter((upper(col("C.schemaName")) == schemaName.upper()) & \
                  (upper(col("C.tableName")) == tableName.upper()))

    if (dfKeyColumns.limit(1).count() == 0):
        executionStatus = " Key columns not defined for the table '" + schemaName +"."+tableName + "'"
        executionStatusID = LOG_EXECUTION_STATUS.NOT_APPLICABLE
        executionLog.add(LOG_EXECUTION_STATUS.NOT_APPLICABLE,logID,executionStatus)
        return [executionStatusID,executionStatus,None]

    for r in dfKeyColumns.select(col("partitionColumn"),\
                                          col("orderByColumn"),\
                                          col("tableCollation"),\
                                          col("isExtended"),\
                                          col("columnList")).collect():
     
      if(r["orderByColumn"] == None or r["orderByColumn"] == ''):
        orderByColumn = "''"
      else:
        orderByColumn = r["orderByColumn"]
        
      if(r["isExtended"] == 1):
        keyColumnMD5 = r["partitionColumn"]
      else:
        keyColumnMD5 = ','.join("{0}".format(c) for c in r["columnList"])
        
      if (r["tableCollation"] != "CS"):
        keyColumnMD5Query = "md5(concat(md5(upper("+ keyColumnMD5.replace(",",")),md5(upper(")+"))))"
      else:
        keyColumnMD5Query = "md5(concat(md5("+ keyColumnMD5.replace(",","),md5(")+")))"
 
     
    dfSource = dfSource.withColumn("md5KeyValue",expr(keyColumnMD5Query))    
    dfSource.createOrReplaceTempView(keyColumnView)    
  
    dfDuplicateRecords = spark.sql("SELECT kpmgRowID,md5KeyValue,\
                     ROW_NUMBER() OVER (PARTITION BY md5KeyValue ORDER BY " \
                    +  orderByColumn + ") AS rowNumber FROM " + keyColumnView).\
                    filter(col("rowNumber")>1).cache()
          
    duplicateRecords = dfDuplicateRecords.count()
    if(duplicateRecords >0):
      dfERPStagingFile = DeltaTable.forPath(spark, filePath)      
      dfERPStagingFile.alias("source")\
                      .merge(source = dfDuplicateRecords.alias("dup"),condition = "source.kpmgRowID = dup.kpmgRowID") \
                      .whenMatchedDelete(condition = lit(True))\
                      .execute()
      lstOfDuplicateRows = list()
      lstOfDuplicateRows = [stgTableName,duplicateRecords]

      executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
      executionStatus = str(duplicateRecords) + " duplicate records found and removed successfully for the table '" + tableName + "'"      
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus,duplicateRecords = duplicateRecords)
      return [executionStatusID,executionStatus,lstOfDuplicateRows] 
    else:
      executionStatus = "No duplicate records found for the table '" + tableName + "'"
      executionStatusID = LOG_EXECUTION_STATUS.SUCCESS   
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus,duplicateRecords = duplicateRecords)
      return [executionStatusID,executionStatus,None] 
    
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()    
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus,duplicateRecords = duplicateRecords)
    executionStatusID = LOG_EXECUTION_STATUS.FAILED 
    return [executionStatusID,executionStatus,None] 
  finally:
    spark.sql("DROP VIEW IF EXISTS " + keyColumnView)
    if dfDuplicateRecords is not None:
      dfDuplicateRecords.unpersist()
    
