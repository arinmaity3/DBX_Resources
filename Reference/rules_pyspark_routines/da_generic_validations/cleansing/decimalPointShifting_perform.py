# Databricks notebook source
from pyspark.sql.functions import concat,col,expr,count,collect_list,lit,trim,concat_ws,length,substring,pow,when, array_except,size
from pyspark.sql.types import DecimalType
import re
from delta.tables import *
import sys
import traceback

def app_decimalPointShifting_perform(schemaName,tableName,filePath,executionID,isForShifting = ""):
  try:

    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.DPS,\
                              validationID = VALIDATION_ID.DPS,
                              schemaName   = schemaName,
                              tableName    = tableName,
                              executionID  = executionID) 

    if(isForShifting == 0):
        executionStatus = "Not applicable"
        executionStatusID = LOG_EXECUTION_STATUS.NOT_APPLICABLE
        executionLog.add(LOG_EXECUTION_STATUS.NOT_APPLICABLE,logID,executionStatus)
        return [executionStatusID,executionStatus] 

    stgTableName = schemaName + "_" + tableName
    
    if not objGenHelper.isStagingFileEmpty(stgTableName):
        app_L0_STG_DecimalPointShiftingQueryList=spark.read.table("app_L0_STG_DecimalPointShiftingQueryList") 
        queryMetadata=app_L0_STG_DecimalPointShiftingQueryList\
                      .filter(col("tableName")==lit(tableName))
        if queryMetadata.first() is not None:
            isFullyShifted=queryMetadata.first().isFullyShifted
            isNotShiftedRows=eval(f'{stgTableName}.filter(expr("size(array_except(transform(split(\'{isFullyShifted}\',\',\'),x->int(x)),isShifted))>0"))')
            if isNotShiftedRows.first() is None:
                executionStatusID = LOG_EXECUTION_STATUS.SUCCESS 
                executionStatus = "DecimalPointShifting is already performed for the table: '" + stgTableName + "'"
                executionLog.add(executionStatusID,logID,executionStatus)
                return  [executionStatusID,executionStatus] 
            else:
                lsQueryRows=queryMetadata.collect()
                for queryRow in lsQueryRows:
                    ls_fieldNames=queryRow["fieldNames"]
                    sqljoinComponent=queryRow["joinComponent"]
                    updateNum=queryRow["updateNum"]
                    prepareAndExecuteMergeQuery(filePath,tableName,ls_fieldNames,sqljoinComponent,updateNum)
                #erpDelta = DeltaTable.forPath(spark, filePath)
                #erpDelta.update(condition ="isShifted = 0 or isNull(isShifted)",set={ "isShifted": "1" })
                executionStatusID = LOG_EXECUTION_STATUS.SUCCESS 
                executionStatus = "DecimalPointShifting Completed for table: '" + stgTableName + "'"
        else:
            executionStatusID = LOG_EXECUTION_STATUS.FAILED
            executionStatus = "Metadata not available for table: '" + stgTableName + "'" 
    else:
        executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        executionStatus=concat("Staging file is empty for table: ",stgTableName)    
       
    executionLog.add(executionStatusID,logID,executionStatus)
    return [executionStatusID,executionStatus] 
  except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()    
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      executionStatusID = LOG_EXECUTION_STATUS.FAILED 
      return [executionStatusID,executionStatus] 
