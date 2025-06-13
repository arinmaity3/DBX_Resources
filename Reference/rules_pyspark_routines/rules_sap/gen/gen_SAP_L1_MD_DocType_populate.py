# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when
def gen_SAP_L1_MD_DocType_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_DocType
    
    df_DocTypeClientDataReportingLanguage=erp_T003.alias("t003")\
        .join (erp_T003T.alias("t003t")\
            ,(col("t003.BLART")  == col("t003t.BLART")), "inner")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
            ,(col("lngkey.sourceSystemValue") == col("t003t.SPRAS")), "inner")\
        .join (knw_clientDataReportingLanguage.alias("rlg1")\
            ,((col("rlg1.languageCode")  == col("lngkey.targetSystemValue"))\
              & (col("rlg1.clientCode")   == col("t003.MANDT") )), "inner")\
        .select(col('t003.MANDT').alias('clientCode')\
            ,col('t003.BLART').alias('documentType')\
            ,col('t003t.LTEXT').alias('documentTypeDescription')\
            ,col('rlg1.languageCode'))\
            .distinct()                     
   
    erp_T003_NotExists = erp_T003.alias('t003')\
        .join(df_DocTypeClientDataReportingLanguage.alias('dtyp')\
            ,((col("t003.BLART")   == col("dtyp.documentType"))\
              & (col("t003.MANDT")   == col("dtyp.clientCode") )) , 'leftanti')
   
    df_DocTypeClientDataSystemReportingLanguage=erp_T003_NotExists.alias("t003")\
        .join (erp_T003T.alias("t003t")\
             ,(col("t003t.BLART").eqNullSafe(col("t003.BLART"))), "left")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
             ,(col("lngkey.sourceSystemValue").eqNullSafe(col("t003t.SPRAS"))), "left")\
        .join (knw_clientDataSystemReportingLanguage.alias("rlg1")\
            ,((col("rlg1.languageCode").eqNullSafe(col("lngkey.targetSystemValue")))\
               & (col("rlg1.clientCode").eqNullSafe(col("t003.MANDT") ))), "left")\
        .select(col('t003.BLART').alias('documentType')\
            ,when((col('t003t.LTEXT').isNull()),'#NA#')\
              .otherwise(col('t003t.LTEXT')).alias('documentTypeDescription')\
            ,when((col('rlg1.languageCode').isNull()),'NA')\
              .otherwise(col('rlg1.languageCode')).alias('languageCode')\
               )
    
    df_DocTypeClientDataReportingLanguage=df_DocTypeClientDataReportingLanguage.drop('clientCode')                     

    gen_L1_MD_DocType=df_DocTypeClientDataReportingLanguage\
        .union(df_DocTypeClientDataSystemReportingLanguage)   
    
    gen_L1_MD_DocType =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_DocType,'gen','L1_MD_DocType',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_MD_DocType populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
