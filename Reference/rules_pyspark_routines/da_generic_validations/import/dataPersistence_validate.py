# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import length


def _dataPersistence_validate(fileFullPath,fileEncoding,columnDelimiter,fileName,fileID,fileType,textQualifier=None):
  #validation dataPersistence
  try:
    objGenHelper = gen_genericHelper()
    
    if textQualifier is None:
      textQualifier = '"'
    
    metacharacters_list = ['.','*', '+','[',']','(',')','^','$','{','}','|','?','"']
    if textQualifier in metacharacters_list:
      textQualifier = '\\'+textQualifier      
    patterntofind = '(['+textQualifier+'])([^'+textQualifier+']*)(['+textQualifier+'])'
    
    df=spark.read.option("encoding", fileEncoding).option("delimiter",'\n').csv('dbfs://'+fileFullPath)
    
    df1=df.withColumn("patterntofind", lit(patterntofind))
    df2=df1.withColumn("lenColumn", length(col("_c0"))).createOrReplaceTempView("source_table")
    
    if gl_ERPSystemID ==10:
        status=spark.sql("select  case when  count(distinct " \
            "length(regexp_replace(_c0,patterntofind,'$1$3'))-length(replace(regexp_replace(_c0,patterntofind,'$1$3'), " \
            " '"+columnDelimiter+"','')))>1 " \
            " then 0 else 1 end "\
            " delimiterLength  from source_table").collect()[0][0]

        inputTextQualifier = gl_parameterDictionary["InputParams"].filter((col("fileID") == fileID) & (col("fileType") == fileType))\
            .select(col('textQualifier')).collect()[0][0]

        if ((status == 0) and (inputTextQualifier is None)):
            status = spark.sql("select  case when  count(distinct " \
                "length(_c0) - length(replace(_c0,'"+columnDelimiter+"',''))) > 1 " \
                " then 0 else 1 end "\
                " delimiterLength  from source_table").collect()[0][0]

    else:
        status=spark.sql("select  case when  count(distinct " \
            "length(_c0)-length(replace(_c0,'"+columnDelimiter+"','')))>1 "\
            " then 0 else 1 end "\
            " delimiterLength  from source_table").collect()[0][0]

    
    if status ==1:
      executionStatus = "Data persistence validation succeeded for the file '" + fileName + "'"
      executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
    else:
      executionStatus = "Data persistence validation failed for the file '" + fileName + "'"
      executionStatusID = LOG_EXECUTION_STATUS.FAILED        
      
    return [executionStatusID,executionStatus]  
  except Exception as err:
    raise

def app_dataPersistence_validate(fileName,fileID,fileType,executionID,columnDelimiter,filePath,fileEncoding,textQualifier=None):
  try:
    
    
    logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.DATA_PERSISTENCE,executionID = executionID)
    
    objGenHelper = gen_genericHelper()
    if textQualifier is None:
      textQualifier = '"'
  
    status = 1        
    lstOfStatus=_dataPersistence_validate(filePath,fileEncoding,columnDelimiter,fileName,fileID,fileType,textQualifier)
      
    if(lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):            
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])  
      return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1]]
    else:            
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,lstOfStatus[1])  
      return [LOG_EXECUTION_STATUS.FAILED,lstOfStatus[1]]
        
  except Exception as err:        
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]  
