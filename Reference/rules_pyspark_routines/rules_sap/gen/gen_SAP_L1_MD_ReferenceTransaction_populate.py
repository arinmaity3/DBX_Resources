# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when

def gen_SAP_L1_MD_ReferenceTransaction_populate(): 
  try:    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_ReferenceTransaction
    
    df_RfTransactionClientDataReportingLanguage=erp_TTYP.alias("ttyp")\
        .join (erp_TTYPT.alias("ttypt"),(col("ttyp.AWTYP") == col("ttypt.AWTYP")), "inner")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
          ,(col("lngkey.sourceSystemValue")    == col("ttypt.SPRAS")), "inner")\
        .join (knw_clientDataReportingLanguage.alias("rlg1")\
          ,((col("rlg1.languageCode") == col("lngkey.targetSystemValue"))), "inner")\
        .select(col('ttyp.AWTYP').alias('referenceTransactionCode')\
          ,col('ttypt.OTEXT').alias('referenceTransactionDescription')\
          ,col('rlg1.languageCode'))\
        .distinct()
                     
    erp_TTYP_NotExists = erp_TTYP.alias('ttyp')\
        .join(df_RfTransactionClientDataReportingLanguage.alias('ttyp_CDRL')\
          ,((col("ttyp_CDRL.referenceTransactionCode") == col("ttyp.AWTYP"))), 'leftanti')
    
    df_RfTransactionClientDataSystemReportingLanguage=erp_TTYP_NotExists.alias("ttyp")\
       .join (erp_TTYPT.alias("ttypt")\
         ,(col("ttyp.AWTYP").eqNullSafe(col("ttypt.AWTYP"))), "left")\
       .join (knw_LK_ISOLanguageKey.alias("lngkey")\
         ,(col("lngkey.sourceSystemValue").eqNullSafe(col("ttypt.SPRAS"))), "left")\
       .join (knw_clientDataSystemReportingLanguage.alias("rlg1")\
         ,((col("rlg1.languageCode").eqNullSafe(col("lngkey.targetSystemValue")))\
          ), "left")\
       .select(col('ttyp.AWTYP').alias('referenceTransactionCode')\
         ,when((col('ttypt.OTEXT').isNull()),'#NA#')\
           .otherwise(col('ttypt.OTEXT')).alias('referenceTransactionDescription')\
         ,when((col('rlg1.languageCode').isNull()),'NA')\
           .otherwise(col('rlg1.languageCode')).alias('languageCode'))\
        .distinct()    
    
    gen_L1_MD_ReferenceTransaction=df_RfTransactionClientDataReportingLanguage\
        .union(df_RfTransactionClientDataSystemReportingLanguage)    
     
    gen_L1_MD_ReferenceTransaction =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_ReferenceTransaction,'gen','L1_MD_ReferenceTransaction',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "L1_MD_ReferenceTransaction populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
