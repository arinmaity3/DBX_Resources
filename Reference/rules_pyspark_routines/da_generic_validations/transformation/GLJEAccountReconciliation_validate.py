# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import *
from pyspark.sql.functions import col, asc, lit, when,regexp_extract,asc,row_number
from pyspark.sql import Row
from functools import reduce
from pyspark.sql import DataFrame
import sys
import traceback
import uuid

def fin_GLJEAccountReconciliation_validate(fileType):
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.CALC_VS_IMPORTED_BALANCES)
        dfResult_Detail = None
        FIN_YEAR                = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        PERIOD_START            = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START')
        PERIOD_END              = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END')
        START_DATE              = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date())
        END_DATE                = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date())
        accumulatedBalanceFlag  = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ACCUMULATED_BALANCE')  
     
        if accumulatedBalanceFlag is None:
            accumulatedBalanceFlag = "1"
        
        if accumulatedBalanceFlag == "0":
            balance_table = 'fin_L1_TD_GLBalance'
        else:
            balance_table = 'fin_L1_STG_GLBalanceAccumulated'
        
        L1_TMP_AccountwiseBalanceFromTrialBalance = spark.sql("SELECT  companyCode,accountNumber,fiscalYear, financialPeriod, "
                                                            " SUM(CASE WHEN financialPeriod in (-1,0) THEN -endingBalanceLC ELSE endingBalanceLC END ) AS GLAmountInRC "
                                                            "FROM  " + balance_table + " WHERE fiscalYear =" +FIN_YEAR+ 
                                                            " AND (financialPeriod between "+PERIOD_START+ " and " +PERIOD_END +")"
                                                            " GROUP BY companyCode"
                                                            ",accountNumber"
                                                            ",fiscalYear"	
                                                            ",financialPeriod")
        L1_TMP_AccountwiseBalanceFromTrialBalance.createOrReplaceTempView('L1_TMP_AccountwiseBalanceFromTrialBalance')
          
        debitCreditIndicatorChange = objDataTransformation.gen_usf_DebitCreditIndicator_Change('JET')
        L1_TMP_AccoutwiseBalanceFromJELine = spark.sql("SELECT companyCode,accountNumber,fiscalYear,financialPeriod, "
                            " SUM(CASE WHEN " + debitCreditIndicatorChange + " = 1 "
                                       " THEN CASE WHEN debitCreditIndicator = 'C' "
                                                   " THEN -1 * amountLC "
                                                    " ELSE amountLC END "
                                        " ELSE amountLC END) AS JEAmountInRC " 
                            " FROM	fin_L1_TD_Journal "
                            " WHERE CAST(postingDate AS DATE) BETWEEN '"+START_DATE+ "' and '" +END_DATE+
                            "' AND documentStatus ='N' "
                            " GROUP BY companyCode,accountNumber,fiscalYear,financialPeriod ")	

        L1_TMP_AccoutwiseBalanceFromJELine.createOrReplaceTempView('L1_TMP_AccoutwiseBalanceFromJELine')
        sqlContext.cacheTable('L1_TMP_AccoutwiseBalanceFromJELine')    
      
        L1_TMP_GLJEAccountwiseDifference = spark.sql("SELECT IF (GL.companyCode IS NULL,JE.companyCode,GL.companyCode) AS companyCode,"
                                                   "IF (GL.accountNumber IS NULL,JE.accountNumber,GL.accountNumber) AS accountNumber, "
                                                   "IF (GL.fiscalYear IS NULL,JE.fiscalYear,GL.fiscalYear) AS fiscalYear, "
                                                   "SUM(IF(GL.GLAmountInRC IS NULL,0,GL.GLAmountInRC)) AS GLAmountInRC, "
                                                   "SUM(IF(JE.JEAmountInRC IS NULL,0,JE.JEAmountInRC)) AS JEAmountInRC, "
                                                   "ROUND(ABS(ABS(SUM(IF(GL.GLAmountInRC IS NULL,0,GL.GLAmountInRC))) - ABS(SUM(IF(JE.JEAmountInRC IS NULL,0,JE.JEAmountInRC)))),0) AS GLJEDifference "
                                                   " FROM L1_TMP_AccountwiseBalanceFromTrialBalance GL "
                                                   " FULL OUTER JOIN L1_TMP_AccoutwiseBalanceFromJELine JE "
                                                   " ON GL.accountNumber = JE.accountNumber "
                                                   " AND IF(GL.companyCode IS NULL,'',GL.companyCode)= IF(JE.companyCode IS NULL,'',JE.companyCode) "
												   " AND GL.fiscalYear			= JE.fiscalYear "
												   " AND GL.financialPeriod		= JE.financialPeriod "
                                                   " GROUP BY IF (GL.companyCode IS NULL,JE.companyCode,GL.companyCode), "
                                                   " IF (GL.accountNumber IS NULL,JE.accountNumber,GL.accountNumber), "
                                                   " IF (GL.fiscalYear IS NULL,JE.fiscalYear,GL.fiscalYear)")
      
        L1_TMP_GLJEAccountwiseDifference.createOrReplaceTempView('L1_TMP_GLJEAccountwiseDifference')
        
        L1_TMP_GLJEDiffAccountReconcilation = spark.sql(" select accountNumber,companyCode,cast(fiscalYear as string)fiscalYear,accountNumber,cast(GLAmountInRC as string)GLAmountInRC ,cast(JEAmountInRC as string) JEAmountInRC from L1_TMP_GLJEAccountwiseDifference WHERE GLJEDifference > 0 ")
        L1_TMP_GLJEDiffAccountReconcilation.limit(gl_maximumNumberOfValidationDetails)
        dfresult = L1_TMP_GLJEDiffAccountReconcilation.count() 
        
        if dfresult >0:
          executionStatus = 'Calculated closing balances versus imported closing balance verification failed.'
          executionStatusID = LOG_EXECUTION_STATUS.WARNING

          dfResult_Detail= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.CALC_VS_IMPORTED_BALANCES.value,
                                      orderBycolumnName1='accountNumber',
                                      pivotColumn1='companyCode',
                                      pivotColumn2='fiscalYear',
                                      pivotColumn3='accountNumber',
                                      pivotColumn4='GLAmountInRC',
                                      pivotColumn5='JEAmountInRC',
                                      dfToPivot=L1_TMP_GLJEDiffAccountReconcilation)
        else:
          executionStatus = 'Calculated closing balances versus imported closing balance verification succeeded'
          executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
      
    except Exception as e:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
             
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail)
        spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_AccoutwiseBalanceFromJELine")
        return [executionStatusID,executionStatus,dfResult_Detail]
        


