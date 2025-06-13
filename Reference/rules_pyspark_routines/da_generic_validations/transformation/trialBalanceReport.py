# Databricks notebook source
from dateutil.parser import parse
import sys
import traceback
import uuid

def fin_trialBalanceReport():
    try:
        objGenHelper = gen_genericHelper()
        
        reportPath = gl_reportPath + "fin_L1_STG_TrialBalance.csv"
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION) 

        df_GlaOpeningwithaccountName = None
        df_Jetperiod = None
        fin_L1_STG_TrialBalance = None
        finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        startDate = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date())
        endDate = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date())
        periodStart = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_START')
        periodEnd = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_End')
        erpSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
            
        ## Get All account
        #1
        df_GlaOpeningwithaccountName =spark.sql("SELECT \
                              glab.companyCode,glab.accountNumber,glab.accountName\
                              ,IF (bdvm.targetSystemValueDescription IS NULL ,'',bdvm.targetSystemValueDescription)	 AS accountType \
                              ,-1 AS financialPeriod,'openingBalance' AS periodDet \
                              ,sum(glab.CYOB)	 AS openingBalance \
                              ,sum(glab.CYCB)	 AS closingGLBalance \
                              FROM  fin_L1_STG_PYCYBalance glab \
                              LEFT JOIN knw_LK_CD_ReportingLanguage rep on \
                              (\
                                rep.companyCode		=	glab.companyCode\
                                AND rep.languageCodeColumn = 'KPMGDataReportingLanguage'\
                               )\
                               LEFT JOIN knw_LK_GD_BusinessDatatypeValueMapping bdvm  ON\
                               (\
                                 bdvm.businessDatatype		=	'Account Type Indicator'\
                                 AND bdvm.sourceERPSystemID	=   '" + str(erpSystemID) + "' \
                                 AND bdvm.targetLanguageCode  =	rep.languageCode \
                                 AND glab.accountType			=	bdvm.sourceSystemValue\
                                )\
                                WHERE glab.isTransactionAccount=1 \
                                GROUP BY  glab.companyCode,glab.accountNumber,glab.accountName\
                                ,IF (bdvm.targetSystemValueDescription IS NULL ,'',bdvm.targetSystemValueDescription)	\
                                ")
        df_GlaOpeningwithaccountName.cache()            
        #2
        #Journal-Transcation Details
        df_Jetperiod=spark.sql("SELECT companyCode,accountNumber\
                                        ,'period'	AS periodDet\
                                        ,SUM(IF (debitCreditIndicator='C',(-1.0*amountLC),amountLC)) AS amount\
                                        ,SUM(IF (debitCreditIndicator='C',(-1.0*amountLC),0)) AS creditBalance\
                                        ,SUM(IF (debitCreditIndicator='C',0,amountLC)) AS debitBalance \
                                    FROM  fin_L1_TD_Journal\
                                    WHERE	IF (fiscalYear IS NULL,0,fiscalYear) = '" + finYear + \
                                  "' AND	postingDate		BETWEEN '" + startDate + "'	 AND  '" + endDate + \
                                  "' AND	financialPeriod		BETWEEN " + periodStart + "	 AND  " + periodEnd + \
                                  "  AND	IF (documentStatus IS NULL,'',documentStatus) ='N' \
                                   GROUP BY companyCode,accountNumber")

        df_Jetperiod.cache()
        new_col_TotalDebits=col('jet.debitBalance')
        new_col_TotalCredits=col('jet.creditBalance')

        new_col_ClosingBalance=when(col('jet.amount').isNull(), col('glab.openingBalance') ) \
                                              .otherwise(col('jet.amount')+(when(col('glab.openingBalance').isNull(),lit(0))\
                                                                                .otherwise(col('glab.openingBalance'))))

        df_GlaOpeningwithaccountName.alias('glab')\
                                    .join(df_Jetperiod.alias('jet'),on=['companyCode','accountNumber'],how='left')\
                                    .select(col('glab.companyCode').alias('companyCode'),col('glab.accountNumber').alias('accountNumber')\
                                       ,col('glab.accountName').alias('accountName'),col('glab.accountType').alias('accountType')\
                                       ,col('glab.openingBalance').alias('openingBalance')\
                                       ,lit(new_col_TotalDebits).alias('amountDR')\
                                       ,lit(new_col_TotalCredits).alias('amountCR')\
                                       ,lit(new_col_ClosingBalance).alias('closingBalance')\
                                       ,col('glab.closingGLBalance').alias('closingGLBalance')\
                                           ).createOrReplaceTempView("fin_L1_STG_TrialBalance")

        fin_L1_STG_TrialBalance=spark.sql("SELECT companyCode,accountNumber,accountName,accountType"
                                  ",CAST(openingBalance AS  NUMERIC(32,6)) AS openingBalance"
                                  ",CAST(amountDR AS NUMERIC(32,6)) AS amountDR "
                                   ",CAST(amountCR AS NUMERIC(32,6)) AS amountCR "
                                   ",CAST(closingBalance AS NUMERIC(32,6)) AS closingBalance "
                                   ",CAST(closingGLBalance AS NUMERIC(32,6)) AS closingGLBalance"
                                   " FROM fin_L1_STG_TrialBalance")
            
        fin_L1_STG_TrialBalance.persist()
        executionStatus = "Trialbalance report created successfully."
        objGenHelper.gen_writeSingleCsvFile_perform(df = fin_L1_STG_TrialBalance,targetFile = reportPath)
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
    except Exception as e:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)                
    finally:
        print(executionStatus)
        if df_GlaOpeningwithaccountName is not None:
            df_GlaOpeningwithaccountName.unpersist()
        if df_Jetperiod is not None:
            df_Jetperiod.unpersist()
        if fin_L1_STG_TrialBalance is not None:
            fin_L1_STG_TrialBalance.unpersist()

        return [executionStatusID,executionStatus]
            


