# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,max

def fin_L1_JEDocumentClassification_03_prepare():
 """Populate fin_L1_STG_JEDocumentClassification_03_prepare"""  
 try:

    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L1_STG_JEDocumentClassification_03_prepare

    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    dfJrnlMaxDescriptions=fin_L1_TD_Journal.groupBy("transactionIDbyJEDocumnetNumber")\
                                           .agg(max("headerDescription").alias("headerDescription"),\
                                                max("lineDescription").alias("lineDescription"))

    fin_L1_STG_JEDocumentClassification_03_prepare = dfJrnlMaxDescriptions.alias('jrnl1')\
                       .filter(((col("jrnl1.headerDescription").isNull())\
                               |(col("jrnl1.headerDescription")=="" ))\
                              &((col("jrnl1.lineDescription").isNull())\
                               |(col("jrnl1.lineDescription")=="" )))\
                       .select(col("jrnl1.transactionIDbyJEDocumnetNumber"))

    fin_L1_STG_JEDocumentClassification_03_prepare  = objDataTransformation.gen_convertToCDMandCache\
                      (fin_L1_STG_JEDocumentClassification_03_prepare,\
                     'fin','L1_STG_JEDocumentClassification_03_prepare',False)

    executionStatus = "fin_L1_STG_JEDocumentClassification_03_prepare populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]   

 except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 finally:
    spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_JEWithDescription")
