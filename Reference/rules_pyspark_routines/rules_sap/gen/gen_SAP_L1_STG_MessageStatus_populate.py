# Databricks notebook source
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
import sys
import traceback

def gen_SAP_L1_STG_MessageStatus_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    global gen_L1_STG_MessageStatus

    row_number_string  = Window.partitionBy("objectKey").orderBy(col("DATVR").desc(),col("UHRVR").desc(),col("transmissionMedium").desc(),col("KSCHL").desc())

    gen_L1_STG_MessageStatus = erp_NAST.alias("NAST")\
      .select(col("NAST.OBJKY").alias("objectKey")\
      ,col("NAST.VSTAT").alias("processingStatus")\
      ,col("NAST.NACHA").alias("transmissionMedium")\
      ,col("DATVR").alias("DATVR")\
      ,col("UHRVR").alias("UHRVR")\
      ,col("KSCHL").alias("KSCHL"))
 
    gen_L1_STG_MessageStatus = gen_L1_STG_MessageStatus.withColumn("messageRank",row_number().over(row_number_string))\
    .select(col("objectKey").alias("objectKey")\
    ,col("processingStatus").alias("processingStatus")\
    ,col("transmissionMedium").alias("transmissionMedium")\
    ,col("messageRank").alias("messageRank"))

    gen_L1_STG_MessageStatus = objDataTransformation.gen_convertToCDMandCache \
        (gen_L1_STG_MessageStatus,'gen','L1_STG_MessageStatus',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_STG_MessageStatus populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
