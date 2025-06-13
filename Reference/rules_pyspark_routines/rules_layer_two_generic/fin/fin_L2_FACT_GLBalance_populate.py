# Databricks notebook source
from pyspark.sql.window import *
from pyspark.sql.functions import col, asc, lit, when,asc,row_number
from pyspark.sql import Row
import sys
import traceback


def fin_L2_FACT_GLBalance_populate():
    try:
      
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global fin_L2_FACT_GLBalance
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      
      # Set parameters and variables
      accumulatedBalanceFlag = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ACCUMULATED_BALANCE')
      periodStart = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_START')
      periodEnd = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_End')
      finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR') 
      
      if objGenHelper.gen_lk_cd_parameter_get('GLOBAL','NO_OF_PERIODS') is not None:
        noOfPeriod	= objGenHelper.gen_lk_cd_parameter_get('GLOBAL','NO_OF_PERIODS')
      else:
        noOfPeriod ='12'

      if objGenHelper.gen_lk_cd_parameter_get('GLOBAL','NO_OF_ADJUSTMENTS') is not None:
         noOfAdjustmentPeriod = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','NO_OF_ADJUSTMENTS')
      elif objGenHelper.gen_lk_cd_parameter_get('GLOBAL','NO_OF_ADJUSTMENTPERIODS') is not None:
         noOfAdjustmentPeriod = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','NO_OF_ADJUSTMENTPERIODS')
      else:
         noOfAdjustmentPeriod = '0'
      
  

      TMP_DateMonthWise = fin_L2_DIM_FinancialPeriod.alias("fin")\
                        .join(gen_L2_DIM_Organization.alias("org"),((col('fin.organizationUnitSurrogateKey')==col('org.organizationUnitSurrogateKey'))),how ="inner")\
                        .groupBy(col("fiscalYear"),col('org.companyCode'),col("financialPeriod"))\
                        .agg(max("postingDate").alias("calendarDate"))
      TMP_DateMonthWise.createOrReplaceTempView("TMP_DateMonthWise")
      sqlContext.cacheTable('TMP_DateMonthWise')
      

      L1_TMP_DateMonthWise = TMP_DateMonthWise.alias("tmp1")\
                      .join(gen_L2_DIM_Organization.alias("org"),((col('tmp1.companyCode')==col('org.companyCode'))),how ="inner")\
                      .join(fin_L2_DIM_FinancialPeriod.alias("fin"),((col('tmp1.calendarDate')==col('fin.postingDate')) \
                      & (col('tmp1.fiscalYear')==col('fin.fiscalYear')) & (col('tmp1.financialPeriod')==col('fin.financialPeriod')) \
                      & (col('fin.organizationUnitSurrogateKey')==col('org.organizationUnitSurrogateKey'))),how="inner") \
                      .select(col('fin.financialPeriodSurrogateKey').alias("dateSurrogateKey"),col('tmp1.companyCode'),col('tmp1.calendarDate'),\
                       col("tmp1.fiscalYear"),col("tmp1.financialPeriod"))
      L1_TMP_DateMonthWise.createOrReplaceTempView("L1_TMP_DateMonthWise")
      sqlContext.cacheTable('L1_TMP_DateMonthWise')
      
      if accumulatedBalanceFlag is None:
        accumulatedBalanceFlag = "1"
        
       
      if accumulatedBalanceFlag == "0":
          L1_TMP_GLBalanceInL2 = fin_L1_TD_GLBalance.select(col("companyCode"),col('accountNumber'),col('fiscalYear'), \
                                                   col('financialPeriod'),col('debitCreditIndicator'),col('localCurrency'), \
                                                   col('documentCurrency'), \
                                                   col('endingBalanceLC').alias("endingBalanceLC"), \
                                                   col('endingBalanceDC').alias("endingBalanceDC")).createOrReplaceTempView("L1_TMP_GLBalanceInL2")
          sqlContext.cacheTable('L1_TMP_GLBalanceInL2')

      else:
          L1_TMP_GLBalanceInL2 = fin_L1_STG_GLBalanceAccumulated.select(col("companyCode"),col('accountNumber'),col('fiscalYear'), \
                                                   col('financialPeriod'),col('debitCreditIndicator'),col('localCurrency'), \
                                                   col('documentCurrency'),col('endingBalanceLC')).sort(lit('1,2,3'))\
                                                   .createOrReplaceTempView("L1_TMP_GLBalanceInL2")
          sqlContext.cacheTable('L1_TMP_GLBalanceInL2')

          
          
          
      L1_TMP_GLAnnualizedBalance = spark.sql("WITH CTE_GLAnnualizedBalance "\
                      "AS "\
                      "( "\
                      "SELECT  companyCode "\
                      ",fiscalYear "\
                      ",debitCreditIndicator "\
                      ",accountNumber "\
                      ",localCurrency  "\
                      ",documentCurrency "\
                      ",IF(debitCreditIndicator='C',((SUM(endingBalanceLC)/("+periodEnd+" - "+noOfAdjustmentPeriod+"))*"+noOfPeriod+"),0) as annualizedCRLC "\
                      ",IF(debitCreditIndicator='D',((SUM(endingBalanceLC)/("+periodEnd+" - "+noOfAdjustmentPeriod+"))*"+noOfPeriod+"),0) as annualizedDRLC "\
                      " FROM   L1_TMP_GLBalanceInL2 "\
                      " WHERE  fiscalYear = "+finYear+ " AND "\
                      " financialPeriod BETWEEN "+ periodStart +" AND "+ periodEnd+ " "\
                      " AND financialPeriod NOT IN (0,-1)"\
                      " GROUP BY	companyCode "\
                      " ,fiscalYear "\
                      " ,debitCreditIndicator "\
                      " ,accountNumber "\
                      " ,localCurrency  "\
                      " ,documentCurrency "\
                      ") "\
                      " select companyCode,fiscalYear,accountNumber,localCurrency,documentCurrency,sum(annualizedCRLC) as annualizedCRLC,sum(annualizedDRLC) as annualizedDRLC from CTE_GLAnnualizedBalance" \
                      " GROUP BY	companyCode,fiscalYear,accountNumber,localCurrency,documentCurrency")
      
      L1_TMP_GLAnnualizedBalance.createOrReplaceTempView("L1_TMP_GLAnnualizedBalance")   
      sqlContext.cacheTable('L1_TMP_GLAnnualizedBalance')
      

         
      FACT_GLBalance1 = spark.sql("select dou2.organizationUnitSurrogateKey \
                       ,gla2.glAccountSurrogateKey \
                       ,ddt2.dateSurrogateKey \
                       ,IF ((tgl1.documentCurrency IS NULL OR tgl1.documentCurrency ='')  ,dou2.localCurrency,tgl1.documentCurrency) as currencyCode \
                       ,tgl1.debitCreditIndicator \
                       ,tgl1.endingBalanceLC as endingBalanceTrial \
                       ,glab.annualizedDRLC as annualizedBalanceDr \
                       ,glab.annualizedCRLC as annualizedBalanceCr \
         FROM	L1_TMP_GLBalanceInL2 tgl1\
         INNER JOIN gen_L2_DIM_Organization dou2 \
                                               ON \
                                               ( \
                                                 dou2.companyCode	= tgl1.companyCode \
                                               AND dou2.companyCode	<> '#NA#') \
         INNER JOIN fin_L2_DIM_GLAccount gla2 \
                                               ON \
                                               ( \
                                                 gla2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey	\
                                               AND gla2.accountNumber = case when isnull(nullif(tgl1.accountNumber,'')) \
                                                   then '#NA#' else tgl1.accountNumber end ) \
         INNER JOIN L1_TMP_DateMonthWise ddt2 \
                                               ON \
                                               ( \
                                                ddt2.fiscalYear = tgl1.fiscalYear \
                                               AND ddt2.financialPeriod = case when tgl1.financialPeriod  = '0' then '-1' \
                                                    else tgl1.financialPeriod end \
                                               AND ddt2.companyCode = dou2.companyCode ) \
         LEFT JOIN L1_TMP_GLAnnualizedBalance glab \
                                               ON \
                                               ( \
                                               tgl1.companyCode = glab.companyCode \
                                               AND tgl1.fiscalYear = glab.fiscalYear \
                                               AND tgl1.accountNumber = glab.accountNumber \
                                               AND IF (tgl1.localCurrency IS NULL ,0,tgl1.localCurrency) = IF (\
                                                    glab.localCurrency IS NULL ,0,glab.localCurrency) \
                                               AND IF (tgl1.documentCurrency IS NULL ,0,tgl1.documentCurrency) = IF (\
                                                    glab.documentCurrency IS NULL ,0,glab.documentCurrency)) \
                                               WHERE case when isnull(nullif(tgl1.documentCurrency,'')) \
                                                    then '#NA#' else tgl1.documentCurrency end \
                                               = case when isnull(nullif(tgl1.localCurrency,'')) \
                                                    then '#NA#' else tgl1.localCurrency end")

      FACT_GLBalance1.createOrReplaceTempView("FACT_GLBalance1")
      sqlContext.cacheTable('FACT_GLBalance1')
      
      FACT_GLBalance2 = spark.sql("select dou2.organizationUnitSurrogateKey \
                    ,gla2.glAccountSurrogateKey \
                    ,ddt2.dateSurrogateKey \
                    ,IF ((tgl1.documentCurrency IS NULL OR tgl1.documentCurrency = '') ,dou2.localCurrency,tgl1.documentCurrency) as currencyCode \
                    ,tgl1.debitCreditIndicator \
                    ,tgl1.endingBalanceLC as endingBalanceTrial \
                    ,glab.annualizedDRLC as annualizedBalanceDr \
                    ,glab.annualizedCRLC as annualizedBalanceCr \
             FROM	L1_TMP_GLBalanceInL2 tgl1\
             INNER JOIN gen_L2_DIM_Organization dou2 \
                                                   ON \
                                                   ( \
                                                     dou2.companyCode	= tgl1.companyCode \
                                                   AND dou2.companyCode	<> '#NA#') \
             INNER JOIN fin_L2_DIM_GLAccount gla2 \
                                                   ON \
                                                   ( \
                                                     gla2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey	\
                                                   AND gla2.accountNumber = case when isnull(nullif(tgl1.accountNumber,'')) \
                                                      then '#NA#' else tgl1.accountNumber end ) \
             INNER JOIN L1_TMP_DateMonthWise ddt2 \
                                                   ON \
                                                   ( \
                                                    ddt2.fiscalYear = tgl1.fiscalYear \
                                                   AND ddt2.financialPeriod = case when tgl1.financialPeriod  = '0' then\
                                                           '-1' else tgl1.financialPeriod end \
                                                   AND ddt2.companyCode = dou2.companyCode ) \
             LEFT JOIN L1_TMP_GLAnnualizedBalance glab \
                                                   ON \
                                                   ( \
                                                   tgl1.companyCode = glab.companyCode \
                                                   AND tgl1.fiscalYear = glab.fiscalYear \
                                                   AND tgl1.accountNumber = glab.accountNumber \
                                                   AND IF (tgl1.localCurrency IS NULL ,0,tgl1.localCurrency) = IF (\
                                                          glab.localCurrency IS NULL ,0,glab.localCurrency) \
                                                   AND IF (tgl1.documentCurrency IS NULL ,0,tgl1.documentCurrency) = IF (\
                                                          glab.documentCurrency IS NULL ,0,glab.documentCurrency)) \
                                                   WHERE case when isnull(nullif(tgl1.documentCurrency,'')) then '#NA#' \
                                                          else tgl1.documentCurrency end \
                                                   <> case when isnull(nullif(tgl1.localCurrency,'')) then '#NA#' else tgl1.localCurrency end")

      FACT_GLBalance2.createOrReplaceTempView("FACT_GLBalance2")
      sqlContext.cacheTable('FACT_GLBalance2')
      
      fin_L2_FACT_GLBalance = spark.sql("select * from FACT_GLBalance1 union All select * from FACT_GLBalance2")
      w = Window().orderBy(lit('glBalanceSurrogateKey'))
      fin_L2_FACT_GLBalance = fin_L2_FACT_GLBalance.withColumn("glBalanceSurrogateKey", row_number().over(w))
            
      fin_L2_FACT_GLBalance = objDataTransformation.gen_convertToCDMandCache \
          (fin_L2_FACT_GLBalance,'fin','L2_FACT_GLBalance',True)
      
      executionStatus = "fin_L2_FACT_GLBalance populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]    
      
    except Exception as err:
       executionStatus = objGenHelper.gen_exceptionDetails_log()       
       executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
       return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    finally:
      spark.sql("UNCACHE TABLE  IF EXISTS TMP_DateMonthWise")
      spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_DateMonthWise")
      spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_GLBalanceInL2")
      spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_GLAnnualizedBalance")
      spark.sql("UNCACHE TABLE  IF EXISTS FACT_GLBalance1")
      spark.sql("UNCACHE TABLE  IF EXISTS FACT_GLBalance2")
      
      
