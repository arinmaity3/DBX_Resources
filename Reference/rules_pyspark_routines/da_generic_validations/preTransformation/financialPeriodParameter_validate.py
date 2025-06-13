# Databricks notebook source
import sys
import traceback

def app_ORA_financialPeriodParameter_validate(executionID = ""):
  try:
  
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.FINANCIAL_PERIOD_PARAMETER,
                              executionID = executionID)
    
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""    
  
    startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
  
    rec_count = knw_LK_CD_ReportingSetup.alias("base").filter(when(col("base.ledgerID").isNull(),lit(""))\
      .otherwise(col("base.ledgerID")).cast("string")!="").count()
    if rec_count==0:
        executionStatus = "LedgerID column is empty in [knw].[LK_CD_ReportingSetup].[ledgerID]"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_PERIODS.alias("erpgl").filter(when(col("erpgl.PERIOD_SET_NAME").isNull(),lit(""))\
    .otherwise(col("erpgl.PERIOD_SET_NAME")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Period set name column is empty in [erp].[GL_PERIODS].[PERIOD_SET_NAME]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_PERIODS.alias("erpgl").filter(when(col("erpgl.PERIOD_TYPE").isNull(),lit(""))\
    .otherwise(col("erpgl.PERIOD_TYPE")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Period type column is empty in [erp].[GL_PERIODS].[PERIOD_TYPE]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_PERIODS.alias("erpgl").filter(when(col("erpgl.START_DATE").isNull(),lit(""))\
    .otherwise(col("erpgl.START_DATE")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Start date column is empty in [erp].[GL_PERIODS].[START_DATE]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_PERIODS.alias("erpgl").filter(when(col("erpgl.END_DATE").isNull(),lit(""))\
    .otherwise(col("erpgl.END_DATE")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "End date column is empty in [erp].[GL_PERIODS].[END_DATE]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGERS.alias("erple").filter(when(col("erple.LEDGER_ID").isNull(),lit(""))\
    .otherwise(col("erple.LEDGER_ID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [erp].[GL_LEDGERS].[LEDGER_ID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGERS.alias("erple").filter(when(col("erple.PERIOD_SET_NAME").isNull(),lit(""))\
    .otherwise(col("erple.PERIOD_SET_NAME")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Period set name column is empty in [erp].[GL_LEDGERS].[PERIOD_SET_NAME]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGERS.alias("erple").filter(when(col("erple.ACCOUNTED_PERIOD_TYPE").isNull(),lit(""))\
    .otherwise(col("erple.ACCOUNTED_PERIOD_TYPE")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Accounted period type column is empty in [erp].[GL_LEDGERS].[ACCOUNTED_PERIOD_TYPE]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    df_subqry1 = erp_GL_LEDGERS.alias('gl')\
            .join(erp_GL_PERIODS.alias('gp'),(col("gp.PERIOD_SET_NAME")==col("gl.PERIOD_SET_NAME"))\
                                            &(col("gp.PERIOD_TYPE")==col("gl.ACCOUNTED_PERIOD_TYPE")),how="left")\
            .join(knw_LK_CD_ReportingSetup.alias("rep"),col("rep.ledgerID")==col("gl.LEDGER_ID").cast("string"),how="left_semi")\
            .filter(lit(startDate).between(col('gp.START_DATE'),col('gp.END_DATE')))\
            .select(col('gl.LEDGER_ID'),col('gl.PERIOD_SET_NAME'),col('gl.ACCOUNTED_PERIOD_TYPE'),col('gp.PERIOD_NAME'))


    set1 = knw_LK_CD_ReportingSetup.alias('rep')\
                              .join(df_subqry1.alias('sub'),(col("rep.ledgerID")==col("sub.LEDGER_ID")),how="left")\
                              .filter(col('sub.LEDGER_ID').isNull())\
                              .select(col('rep.ledgerID').alias('ledger_id'),lit(startDate).alias('period_startdate'),lit(None).alias('period_enddate'))

    df_subqry2 = erp_GL_LEDGERS.alias('gl')\
                .join(erp_GL_PERIODS.alias('gp'),(col("gp.PERIOD_SET_NAME")==col("gl.PERIOD_SET_NAME"))\
                                                &(col("gp.PERIOD_TYPE")==col("gl.ACCOUNTED_PERIOD_TYPE")),how="left")\
                .join(knw_LK_CD_ReportingSetup.alias("rep"),col("rep.ledgerID")==col("gl.LEDGER_ID").cast("string"),how="left_semi")\
                .filter(lit(endDate).between(col('gp.START_DATE'),col('gp.END_DATE')))\
                .select(col('gl.LEDGER_ID'),col('gl.PERIOD_SET_NAME'),col('gl.ACCOUNTED_PERIOD_TYPE'),col('gp.PERIOD_NAME'))

    set2 = knw_LK_CD_ReportingSetup.alias('rep')\
                              .join(df_subqry2.alias('sub'),(col("rep.ledgerID")==col("sub.LEDGER_ID")),how="left")\
                              .filter(col('sub.LEDGER_ID').isNull())\
                              .select(col('rep.ledgerID').alias('ledger_id'),lit(None).alias('period_startdate'),lit(endDate).alias('period_enddate'))


    financialperiod = set1.union(set2)


    ORA_L0_TMP_FinancialPeriodValidate = financialperiod.alias('fnp')\
                                          .select(col('ledger_id'),col('period_startdate'),col('period_enddate'),lit('Scoped financial period start date/end date does not match with source data'))

    rec_count = ORA_L0_TMP_FinancialPeriodValidate.count()

    if rec_count!=0:
        executionStatus = "Scoped financial period start period/end period does not matched with the scoped start date and end date"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    else:
        executionStatus="Financial period parameter Validation successful"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err: 
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
