# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,lit,col,when
import sys
import traceback

def fin_L3_STG_GLAnnualizedBalance_populate(): 
  """Populate fin_L3_STG_GLAnnualizedBalance """
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    global fin_L3_STG_GLAnnualizedBalance
    window = Window().orderBy(lit("1"))
    
    noOfPeriod=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'NO_OF_PERIODS') 
    noOfAdjustment=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'NO_OF_ADJUSTMENTS')
    finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
    periodStart = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_START')
    periodEnd = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_End')
    noOfAdjustment = "0" if noOfAdjustment is None else noOfAdjustment
    noOfPeriod = "NULL" if noOfPeriod is None else noOfPeriod
    
    if not (periodStart is None):
        interimPeriodEnd = str(int(periodStart) - 1)

    fin_L3_TMP_GLAnnualizedBalance =spark.sql("SELECT DISTINCT "\
                                 " glBal2.organizationUnitSurrogateKey"\
                                 " ,glBal2.glAccountSurrogateKey"\
                                 " ,glBal2.annualizedBalanceDr"\
                                 " ,glBal2.annualizedBalanceCr"\
                                 " ,currencyCode"\
                               " FROM	   fin_L2_FACT_GLBalance glBal2"\
                               " INNER JOIN fin_L2_DIM_FinancialPeriod	ddt  ON"\
                                 " ddt.organizationUnitSurrogateKey	= glBal2.organizationUnitSurrogateKey"\
                                 " AND ddt.financialPeriodSurrogateKey		= glBal2.dateSurrogateKey"\
                                
                               " INNER JOIN fin_L2_DIM_GLAccount  glAcc2  ON "\
                                 " glAcc2.organizationUnitSurrogateKey	= glBal2.organizationUnitSurrogateKey"\
                                 " AND glAcc2.glAccountSurrogateKey		= glBal2.glAccountSurrogateKey"\
                                 " AND glAcc2.accountCategory				= '#NA#' "\
                                 " WHERE	ddt.fiscalYear ="+finYear+""\
                                
                               " AND		ddt.financialPeriod  BETWEEN   CASE	WHEN "+ periodStart+"  = 0 "\
                                   " THEN ddt.financialPeriod"\
                                   " ELSE "+periodStart+""\
                                   " END"\
                                   " AND	"\
                                   " CASE	WHEN "+periodEnd+" = 0"\
                                   " THEN ddt.financialPeriod"\
                                   " ELSE "+periodEnd+""\
                                   " END"\
                               " Union All"\
                                 " SELECT	 glBal2.organizationUnitSurrogateKey"\
                                   " ,glBal2.glAccountSurrogateKey"\
                                   " ,CASE debitCreditIndicator WHEN 'D' "\
                                   "  THEN (endingBalanceTrial/("+ periodEnd +"-"+noOfAdjustment+"))* "+noOfPeriod+"\
                                                  ELSE 0.00 END annualizedBalanceDr "\
                                   " ,CASE debitCreditIndicator WHEN 'C' "\
                                   "  THEN (endingBalanceTrial/("+periodEnd+" -"+ noOfAdjustment+"))* "+noOfPeriod+" \
                                                   ELSE 0.00 END annualizedBalanceCr"\
                                   " ,currencyCode"\
                                   " FROM	fin_L2_FACT_GLBalance glBal2"\
                                 " INNER JOIN fin_L2_DIM_FinancialPeriod	ddt  ON"\
                               
                                 " ddt.organizationUnitSurrogateKey	= glBal2.organizationUnitSurrogateKey"\
                                 " AND ddt.financialPeriodSurrogateKey		= glBal2.dateSurrogateKey"\
                               
                                 " INNER JOIN fin_L2_DIM_GLAccount  glAcc2  ON"\
                               
                                 " glAcc2.organizationUnitSurrogateKey		= glBal2.organizationUnitSurrogateKey"\
                                 " AND glAcc2.glAccountSurrogateKey			= glBal2.glAccountSurrogateKey"\
                                 " AND glAcc2.accountCategory					= '#NA#'"\
                               
                                 " WHERE	ddt.fiscalYear = "+finYear+" "\
                                 " AND ddt.financialPeriod   between  -1 and " + interimPeriodEnd  +" "\
                                   )
    
    fin_L3_TMP_GLAnnualizedBalance.createOrReplaceTempView("fin_L3_TMP_GLAnnualizedBalance") 
    sqlContext.cacheTable("fin_L3_TMP_GLAnnualizedBalance")
    fin_L3_STG_GLAnnualizedBalance =spark.sql("SELECT"\
                     " gbal.organizationUnitSurrogateKey"\
                     " ,gbal.glAccountSurrogateKey"\
                     " ,sum(case when ISNULL(gbal.annualizedBalanceDr) then 0.00 \
                                           else gbal.annualizedBalanceDr end  ) annualizedBalanceDr"\
                     " ,sum(case when ISNULL(gbal.annualizedBalanceCr) then 0.00 \
                                           else gbal.annualizedBalanceCr end  ) annualizedBalanceCr"\
                     " ,sum(case when ISNULL(gbal.annualizedBalanceDr) then 0.00 \
                                           else gbal.annualizedBalanceDr end )"\
                      " + sum(case when ISNULL(gbal.annualizedBalanceCr) then 0.00 \
                                           else gbal.annualizedBalanceCr end ) annualizedBalanceDrCr"\
                     " FROM"\
                     " fin_L3_TMP_GLAnnualizedBalance gbal"\
                     " GROUP BY gbal.organizationUnitSurrogateKey"\
                     " ,gbal.glAccountSurrogateKey"
                     )
    fin_L3_STG_GLAnnualizedBalance=fin_L3_STG_GLAnnualizedBalance.withColumn("annulizedBalanceID",row_number().over(window)) 
 
    fin_L3_STG_GLAnnualizedBalance = objDataTransformation.gen_convertToCDMandCache \
        (fin_L3_STG_GLAnnualizedBalance,'fin','L3_STG_GLAnnualizedBalance',True)

    #Writing to parquet file in dwh schema format
    dwh_vw_FACT_GLAnnualizedBalance = objDataTransformation.gen_convertToCDMStructure_generate(\
                             fin_L3_STG_GLAnnualizedBalance,'dwh','vw_FACT_GLAnnualizedBalance',False)[0]               
    objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_GLAnnualizedBalance,\
                             gl_CDMLayer2Path + "fin_L2_FACT_GLAnnualizedBalance.parquet" ) 

    executionStatus="fin_L3_STG_GLAnnualizedBalance populated successfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  finally:
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L3_TMP_GLAnnualizedBalance")
        
    

