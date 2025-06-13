# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType
import sys
import traceback

def fin_L2_FACT_JEBifurcationBalance_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    finYear =  objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
    periodStart_1 = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START'))-1
    global fin_L2_FACT_JEBifurcationBalance
    
    L2_TMP_BifurcationWithEndingBalance=fin_L2_FACT_GLBalance.alias("glbl2")\
                  .join(gen_L2_DIM_Organization.alias("orgu2")\
                          ,(col('glbl2.organizationUnitSurrogateKey')==col('orgu2.organizationUnitSurrogateKey')),'inner')\
                  .join(fin_L2_DIM_FinancialPeriod.alias("ddt2")\
                          ,((col('glbl2.dateSurrogateKey')==col('ddt2.financialPeriodSurrogateKey')) &\
                           (col('orgu2.organizationUnitSurrogateKey')==col('ddt2.organizationUnitSurrogateKey'))),'inner')\
                  .join(fin_L2_DIM_GLAccount.alias("glacc2")\
                          ,((col('glbl2.glAccountSurrogateKey')==col('glacc2.glAccountSurrogateKey')) &\
                           (col('glacc2.accountCategory')== '#NA#' )),'inner')\
                          .filter((col('ddt2.fiscalYear')==finYear)\
                                  & (col('glbl2.endingBalanceTrial') != lit(0.00))\
                                   & (col('ddt2.financialPeriod').between(-1,periodStart_1))\
                                 )\
                   .groupBy('glbl2.organizationUnitSurrogateKey','ddt2.financialPeriodSurrogateKey'\
                            ,'glacc2.glAccountSurrogateKey','glbl2.debitCreditIndicator'\
                            ,'ddt2.fiscalYear','ddt2.fiscalYear')\
                   .agg( sum('glbl2.endingBalanceTrial').alias("endingBalanceTrial")\
                       )
    
    fin_L2_FACT_JEBifurcationBalance=L2_TMP_BifurcationWithEndingBalance.alias("fb").\
                    select(col('fb.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                           ,col('fb.financialPeriodSurrogateKey').alias('dateSurrogateKey')\
                           ,col('fb.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
                           ,col('fb.debitCreditIndicator').alias('debitCreditIndicator')\
                           ,col('fb.endingBalanceTrial').alias('accountBalance')\
                           ,col('fb.endingBalanceTrial').alias('beginingBalance')\
                           ,when(col("fb.debitCreditIndicator")== 'C', col('fb.endingBalanceTrial'))\
                                      .otherwise(lit(0)).alias("accountBalanceCr")\
                           ,when(col("fb.debitCreditIndicator")== 'D', col('fb.endingBalanceTrial'))\
                                      .otherwise(lit(0)).alias("accountBalanceDr")\
                           ,lit(0).alias('businessProcessScenarioID')\
                                                                             
                                                                           )
    fin_L2_FACT_JEBifurcationBalance = objDataTransformation.gen_convertToCDMStructure_generate(fin_L2_FACT_JEBifurcationBalance,\
				'fin','L2_FACT_JEBifurcationBalance',False)[0]

    dwh_vw_FACT_JEBifurcationBalance = fin_L2_FACT_JEBifurcationBalance.alias('jbb').\
        select(col('jbb.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
        ,col('jbb.dateSurrogateKey').alias('dateSurrogateKey')\
        ,col('jbb.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
        ,col('jbb.accountBalance').alias('accountBalance')\
        ,col('jbb.beginingBalance').alias('beginingBalance'))

    dwh_vw_FACT_JEBifurcationBalance = objDataTransformation.gen_convertToCDMStructure_generate(dwh_vw_FACT_JEBifurcationBalance,\
				'dwh','vw_FACT_JEBifurcationBalance',True)[0]
    objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_JEBifurcationBalance,gl_CDMLayer2Path + "fin_L2_FACT_JEBifurcationBalance.parquet" )
    
    executionStatus = "fin_L2_FACT_JEBifurcationBalance populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
