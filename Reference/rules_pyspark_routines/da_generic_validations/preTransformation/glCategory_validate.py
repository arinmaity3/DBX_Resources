# Databricks notebook source
import sys
import traceback

def app_ORA_GLCategory_validate(executionID = ""):
  try:
    
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.GL_CATEGORY,
                              executionID = executionID)
    
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""

    rec_count = knw_LK_CD_ReportingSetup.alias("base").filter(when(col("base.ledgerID").isNull(),lit(""))\
    .otherwise(col("base.ledgerID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [knw].[LK_CD_ReportingSetup].[ledgerID]"
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]

    rec_count = erp_GL_JE_HEADERS.alias("base").filter(when(col("base.LEDGER_ID").isNull(),lit(""))\
    .otherwise(col("base.LEDGER_ID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [erp].[GL_JE_HEADERS].[LEDGER_ID]"
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]

    rec_count = erp_GL_JE_HEADERS.alias("base").filter(when(col("base.LEDGER_ID").isNull(),lit(""))\
    .otherwise(col("base.LEDGER_ID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [erp].[GL_JE_HEADERS].[LEDGER_ID]"
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]

    rec_count = erp_GL_JE_HEADERS.alias("base").filter(when(col("base.JE_CATEGORY").isNull(),lit(""))\
    .otherwise(col("base.JE_CATEGORY")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "JE category column is empty in [erp].[GL_JE_HEADERS].[JE_CATEGORY]"
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]

    rec_count = erp_GL_JE_CATEGORIES_TL.alias("base").filter(when(col("base.JE_CATEGORY_NAME").isNull(),lit(""))\
    .otherwise(col("base.JE_CATEGORY_NAME")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "JE category name column is empty in [erp].[GL_JE_CATEGORIES_TL].[JE_CATEGORY_NAME]" 
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]

    df_missing_categories = erp_GL_JE_HEADERS.alias("dr")\
    .join(knw_LK_CD_ReportingSetup.alias("rpt"),col("dr.LEDGER_ID") == col("rpt.ledgerID"),how="inner")\
    .join(erp_GL_JE_CATEGORIES_TL.alias("jetl"),upper(col("dr.JE_CATEGORY"))==upper(col("jetl.JE_CATEGORY_NAME")),how="left_anti")\
    .select(concat(col("dr.LEDGER_ID").cast("string"),lit("|"),col("dr.JE_CATEGORY")).alias("columnList")).distinct()
    error_msg = df_missing_categories.select("columnList").rdd.flatMap(lambda x: x).collect()

    if len(error_msg)>0:
      executionStatus = "GL categories([erp].[GL_JE_HEADERS].[JE_CATEGORY]) in Journal Header does not match with source data"
      executionStatus = executionStatus + " loaded in ([erp].[GL_JE_CATEGORIES_TL].[JE_CATEGORY_NAME])" + str(error_msg)
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.WARNING,executionStatus]
    else:
      executionStatus="GL Journal Category Validation successful"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      
  except Exception as err: 
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
