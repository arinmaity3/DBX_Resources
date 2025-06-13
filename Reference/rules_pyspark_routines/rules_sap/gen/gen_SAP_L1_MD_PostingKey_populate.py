# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when

def gen_SAP_L1_MD_PostingKey_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_PostingKey
    
    erpSystemIDGeneric = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    erpSAPSystemID     = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')
    reportingLanguage  = knw_KPMGDataReportingLanguage.select('languageCode').distinct().collect()[0][0]
                           

    df_LK_PostinKeyDesc=knw_LK_GD_BusinessDatatypeValueMapping\
        .select('sourceSystemValue','targetSystemValue','targetSystemValueDescription')\
        .filter((col('businessDatatype')==lit('Posting Key')) \
                & (col('targetLanguageCode')==reportingLanguage)\
                & (col('sourceERPSystemID')==erpSAPSystemID)\
                & (col('targetERPSystemID')==erpSystemIDGeneric)\
               )
    
    df_PostingKeyClientDataReportingLanguage=erp_TBSL.alias("tbsl")\
        .join (erp_TBSLT.alias("tbslt")\
              ,((col("tbsl.MANDT")    == col("tbslt.MANDT"))\
                & (col("tbsl.BSCHL")    == col("tbslt.BSCHL")) \
                & ((col("tbslt.UMSKZ").isNull())|((col("tbslt.UMSKZ")=='')))), "inner")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
              ,(col("lngkey.sourceSystemValue")    == col("tbslt.SPRAS")), "inner")\
        .join (knw_clientDataReportingLanguage.alias("rlg1")\
             ,((col("rlg1.languageCode")  == col("lngkey.targetSystemValue"))\
               & (col("rlg1.clientCode")   == col("tbsl.MANDT") )), "inner")\
              .select(col('tbsl.MANDT').alias('clientCode')\
                      ,col('tbsl.BSCHL').alias('postingKey')\
                      ,col('tbslt.LTEXT').alias('postingKeyDescription')\
                      ,col('rlg1.languageCode'))\
              .distinct()
    
    erp_TBSL_NotExists = erp_TBSL.alias('tbsl')\
        .join(df_PostingKeyClientDataReportingLanguage.alias('dpk')\
          ,((col("tbsl.BSCHL")   == col("dpk.postingKey"))\
            & (col("tbsl.MANDT")   == col("dpk.clientCode") )) , 'leftanti')

    df_PostingKeyClientDataSystemReportingLanguage=erp_TBSL_NotExists.alias("tbsl")\
        .join (erp_TBSLT.alias("tbslt"),((col("tbsl.MANDT").eqNullSafe(col("tbslt.MANDT")))\
            & (col("tbsl.BSCHL").eqNullSafe(col("tbslt.BSCHL"))) \
            & ((col("tbslt.UMSKZ").isNull())|((col("tbslt.UMSKZ")=='')))), "left")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
          ,(col("lngkey.sourceSystemValue").eqNullSafe(col("tbslt.SPRAS"))), "left")\
        .join (knw_clientDataSystemReportingLanguage.alias("rlg1")\
          ,((col("rlg1.languageCode").eqNullSafe(col("lngkey.targetSystemValue")))\
             & (col("rlg1.clientCode").eqNullSafe(col("tbsl.MANDT") ))), "left")\
        .select(col('tbsl.MANDT').alias('clientCode')\
          ,col('tbsl.BSCHL').alias('postingKey')\
          ,when((col('tbslt.LTEXT').isNull()),'#NA#')\
            .otherwise(col('tbslt.LTEXT')).alias('postingKeyDescription')\
          ,when((col('rlg1.languageCode').isNull()),'NA')\
            .otherwise(col('rlg1.languageCode')).alias('languageCode')\
                )
    
    df_PostingKey=df_PostingKeyClientDataReportingLanguage\
        .union(df_PostingKeyClientDataSystemReportingLanguage)
    
    gen_L1_MD_PostingKey=df_PostingKey.alias("pk")\
        .join (df_LK_PostinKeyDesc.alias("bdt0")\
          ,(col("bdt0.sourceSystemValue")    == col("pk.postingKey")), "left")\
        .select(col('pk.postingKey')\
          ,col('pk.postingKeyDescription')
          ,when((col('bdt0.targetSystemValueDescription').isNull()),'')\
            .otherwise(col('bdt0.targetSystemValueDescription')).alias('postingKeyKPMG')\
          ,col('pk.languageCode')).distinct()
    
    gen_L1_MD_PostingKey =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_PostingKey,'gen','L1_MD_PostingKey',targetPath=gl_CDMLayer1Path)
      
    executionStatus = "L1_MD_PostingKey populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
