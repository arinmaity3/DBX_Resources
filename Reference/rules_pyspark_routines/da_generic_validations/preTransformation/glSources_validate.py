# Databricks notebook source
import sys
import traceback

def app_ORA_GLSources_validate(executionID = ""):
  try:
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.GL_SOURCES,
                              executionID = executionID)
    
           
    recd_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.ledgerID').isNull(),lit(''))\
                        .otherwise(col('rps.ledgerID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Ledger ID column is empty in '[knw].[LK_CD_ReportingSetup].[ledgerID]'"  
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]
      
    recd_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.clientDataReportingLanguage').isNull(),lit(''))\
                        .otherwise(col('rps.clientDataReportingLanguage')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Client data reporting language column is empty in '[knw].[LK_CD_ReportingSetup].[clientDataReportingLanguage]'"       
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]
      
    recd_count = erp_GL_JE_HEADERS.alias('glje').filter(when(col('glje.LEDGER_ID').isNull(),lit(''))\
                        .otherwise(col('glje.LEDGER_ID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "LedgerID column is empty in '[erp].[GL_JE_HEADERS].[LEDGER_ID]'"       
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]
      
    recd_count = erp_GL_JE_HEADERS.alias('glje').filter(when(col('glje.JE_SOURCE').isNull(),lit(''))\
                        .otherwise(col('glje.JE_SOURCE')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "JE source column is empty in '[erp].[GL_JE_HEADERS].[JE_SOURCE]'"       
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]
      
    recd_count = erp_GL_JE_SOURCES_TL.alias('gltl').filter(when(col('gltl.JE_SOURCE_NAME').isNull(),lit(''))\
                        .otherwise(col('gltl.JE_SOURCE_NAME')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "JE source name column is empty in '[erp].[GL_JE_SOURCES_TL].[JE_SOURCE_NAME]'"       
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]
      
    recd_count = erp_GL_JE_SOURCES_TL.alias('gltl').filter(when(col('gltl.LANGUAGE').isNull(),lit(''))\
                        .otherwise(col('gltl.LANGUAGE')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Language column is empty in '[erp].[GL_JE_SOURCES_TL].[LANGUAGE]'"       
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]
      
    df_missing_JEsources = erp_GL_JE_HEADERS.alias('DR')\
           .join(knw_LK_CD_ReportingSetup.alias('t3'),(col('DR.LEDGER_ID')==(col('t3.ledgerID'))),how='inner')\
           .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bus'),\
                 ((col('bus.sourceSystemValue')==(col('t3.clientDataReportingLanguage')))),how='inner')\
           .join(erp_GL_JE_SOURCES_TL.alias('LE'),\
                 (col('DR.JE_SOURCE')==(col('LE.JE_SOURCE_NAME'))),how='left_anti')\
           .select(concat(col('DR.LEDGER_ID').cast("string"),lit("|"),col('DR.JE_SOURCE')).alias("columnList")).distinct()
    missingparameter = df_missing_JEsources.select("columnList").rdd.flatMap(lambda x: x).collect()

    if len(missingparameter)>0:
      Joinstring = ','.join(missingparameter)
      executionStatus = "Scoped JE sources('[erp].[GL_JE_HEADERS].[JE_SOURCE]')in Journal Header does not match with "\
                       "source data loaded in ('[erp].[GL_JE_SOURCES_TL].[JE_SOURCE_NAME]')" + Joinstring +"'"
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus] 
    else:
        executionStatus="GL Journal Sources Validation successful"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
