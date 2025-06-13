# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col

def gen_ORA_L1_MD_Transaction_populate(): 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_DocType
    

    gen_L1_MD_Transaction = erp_GL_JE_HEADERS.alias('glh')\
                      .join(knw_LK_CD_ReportingSetup.alias('rep'),\
                           (col('glh.LEDGER_ID')==(col('rep.ledgerID'))),how='inner')\
                      .join(erp_GL_JE_SOURCES_TL.alias('gls'),\
                           (col('gls.JE_SOURCE_NAME').eqNullSafe(col('glh.JE_SOURCE'))),how='left')\
                      .join(erp_FND_LANGUAGES_VL.alias('fl')\
                           ,(col('gls.LANGUAGE').eqNullSafe(col('FL.LANGUAGE_CODE'))),how='left')\
                      .select(lit(None).alias('companyCode')\
                            ,col('gls.JE_SOURCE_NAME').alias('transactionType')\
                            ,when(col('gls.USER_JE_SOURCE_NAME').isNull(),lit('#NA#'))\
                               .otherwise(col('gls.USER_JE_SOURCE_NAME')).alias('transactionTypeDescription')\
                            ,when(col('fl.ISO_LANGUAGE').isNull(),col('rep.reportingLanguageCode'))\
                               .otherwise(col('fl.ISO_LANGUAGE')).alias('languageCode'))\
                      .distinct()

    gen_L1_MD_Transaction =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_Transaction,'gen','L1_MD_Transaction',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "L1_MD_Transaction populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

