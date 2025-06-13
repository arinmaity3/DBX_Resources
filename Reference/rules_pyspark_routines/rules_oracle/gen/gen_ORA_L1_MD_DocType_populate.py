# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,when

def gen_ORA_L1_MD_DocType_populate(): 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_DocType
    

    gen_L1_MD_DocType = erp_GL_JE_HEADERS.alias('glh')\
               .join(knw_LK_CD_ReportingSetup.alias('rep')\
                     ,(col('glh.LEDGER_ID')==(col('rep.ledgerID'))),how='inner')\
               .join(erp_GL_JE_CATEGORIES_TL.alias('glc')\
                     ,(col('glh.JE_CATEGORY').eqNullSafe(col('glc.JE_CATEGORY_NAME'))),how='left')\
               .join(erp_FND_LANGUAGES_VL.alias('fl')\
                     ,(col('glc.LANGUAGE').eqNullSafe(col('FL.LANGUAGE_CODE'))),how='left')\
               .select(lit(None).alias('companyCode')\
                     ,col('glh.JE_CATEGORY').alias('documentType')\
                     ,when(col('fl.ISO_LANGUAGE').isNull(),col('rep.reportingLanguageCode'))\
                          .otherwise(col('fl.ISO_LANGUAGE')).alias('languageCode')\
                     ,when(col('glc.DESCRIPTION').isNull(),lit('#NA#')).otherwise(col('glc.DESCRIPTION'))\
                          .alias('documentTypeDescription'))\
               .distinct()

    gen_L1_MD_DocType =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_DocType,'gen','L1_MD_DocType',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "L1_MD_DocType populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

