# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import isnan, when, count
import builtins as p
from pyspark.sql.window import Window 
import itertools
import random


def _mandatoryFieldERP_validate(lstOfLoadedTables):
  try:
                
    schemaName = lstOfLoadedTables[0]
    tableName  = lstOfLoadedTables[1]
    sparkTableName = lstOfLoadedTables[2]
    filePath   = lstOfLoadedTables[3]    
    validationID = lstOfLoadedTables[4] 
    executionID = lstOfLoadedTables[5] 
    
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = validationID,
                              executionID = executionID,
                              writeSummaryFile = False)
    
    executionStatus = ""
    try:            
      dfSource = spark.read.table(sparkTableName)            
    except Exception as err:
      try:        
        dfSource = objGenHelper.gen_readFromFile_perform(filePath)
      except Exception as fileNotFound:
        executionStatus = "File not found '" + stgTableName + "'"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return
                
    lstOfKeyColumns = list()
    lstOfKeyColumns_date_time = list()
    
    [lstOfKeyColumns.append(k.columnName) for k in gl_metadataDictionary['dic_ddic_column'].alias("C")\
                        .select(col("C.columnName"),col("sparkDataType"))\
                        .filter((upper(col("C.schemaName")) == schemaName.upper()) & \
                         (upper(col("C.tableName")) == tableName.upper()) & (col("C.isKey") == 'X') &\
                         (~col("C.sparkDataType").isin("timestamp","date")))\
                        .orderBy(col("C.position"))\
                        .distinct().collect()]
    
    [lstOfKeyColumns_date_time.append(k.columnName) for k in gl_metadataDictionary['dic_ddic_column'].alias("C")\
                        .select(col("C.columnName"),col("sparkDataType"))\
                        .filter((upper(col("C.schemaName")) == schemaName.upper()) & \
                         (upper(col("C.tableName")) == tableName.upper()) & (col("C.isKey") == 'X') &\
                         (col("C.sparkDataType").isin("timestamp","date")))\
                        .orderBy(col("C.position"))\
                        .distinct().collect()]
    
    dfKeyColumns = dfSource.select(lstOfKeyColumns)
    dfKeyColumns_date_time = dfSource.select(lstOfKeyColumns_date_time)
    lstOfMissingValues = list()    
    
    [lstOfMissingValues.append(r)
    for r in dfKeyColumns.select([count(when((isnan(c)) | (col(c).isNull()) |\
                         ((lit(dict(dfKeyColumns.dtypes)[c])=='string') & (col(c)=='')),c))\
                         .alias(c) for c in dfKeyColumns.columns]).rdd.flatMap(lambda x: x).collect()]
    
    [lstOfMissingValues.append(r)
    for r in dfKeyColumns_date_time.select([count(when((col(c).isNull()) |\
                         ((lit(dict(dfKeyColumns_date_time.dtypes)[c])=='string') & (col(c)=='')),c))\
                         .alias(c) for c in dfKeyColumns_date_time.columns]).rdd.flatMap(lambda x: x).collect()]
    
    if (p.max(lstOfMissingValues) != 0):
      dictOfResults = dict(zip(lstOfKeyColumns, lstOfMissingValues)) 
      lstOfDetails = list()
      
      for (key, value) in dictOfResults.items():
        if (value != 0):         
          groupSlNo = (random.randint(0,5000000))
          lstOfDetails.append([groupSlNo,validationID,tableName,None,'tableName',sparkTableName])
          lstOfDetails.append([groupSlNo,validationID,tableName,None,'columnName',key])
          lstOfDetails.append([groupSlNo,validationID,tableName,None,'numberOfBlankRows',value])
          executionStatus = executionStatus + key +","
      dfResults = spark.createDataFrame(schema = gl_ValidationResultDetailSchema, data = lstOfDetails)
      
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,
                       executionStatus[:-1],
                       writeSummaryFile = False,
                       dfDetail = dfResults)
      
      return [LOG_EXECUTION_STATUS.WARNING,sparkTableName + ":" + executionStatus[:-1],dfResults]    
    else:
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,
                       executionStatus[:-1],
                       writeSummaryFile = False,
                       dfDetail = None)      
      return [LOG_EXECUTION_STATUS.SUCCESS,sparkTableName,None]
  
  except Exception as err:
    raise


def app_mandatoryColumnValue_validate(maxWorkers,executionID):
  try:
    
    objGenHelper = gen_genericHelper()

    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.BLANK_DATA_ERP,
                              executionID = executionID)

    scopedAnalytics = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','SCOPED_ANALYTICS').split(",")    

    lstOfLoadedTables = list()
    w = Window().orderBy(lit(''))   
    
    [lstOfLoadedTables.append([t.schemaName,
                           t.tableName,
                           t.sparkTableName,
                           gl_layer0Staging + t.sparkTableName +".delta",                           
                           VALIDATION_ID.BLANK_DATA_ERP.value,
                           executionID])\
    for t in gl_metadataDictionary['dic_ddic_tablesRequiredForERPPackages'].alias("M").\
      join(gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT].alias("R"),\
      [upper(col("M.sparkTableName")) == upper(col("R.tableName"))],how = 'inner').\
      filter(col("M.auditProcedureName").isin(scopedAnalytics) & \
      (col("netRecordCount") != 0) & \
      (col("M.ERPSystemID") == gl_ERPSystemID)).\
      select(col("M.schemaName"),
             col("M.tableName"),
             col("M.sparkTableName")).distinct().collect()]
        
    with ThreadPoolExecutor(max_workers = maxWorkers) as executor:
      lstOfStatus = list(executor.map(_mandatoryFieldERP_validate ,lstOfLoadedTables)) 
   
    lstOfFailedStatus = list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.WARNING, lstOfStatus))
    if(len(lstOfFailedStatus) != 0):
      failedTableList = list()
      [failedTableList.append(t[1]) for t in lstOfFailedStatus]
      executionStatus = ",".join(failedTableList)       
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus] 
    else:
      executionStatus = "Mandatory field validation succeded."
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus] 
  except Exception as err:
    raise err
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
 
