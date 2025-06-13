# Databricks notebook source
import sys
import traceback
import warnings
from pyspark.sql.functions import lit,col

def fin_L2_FACT_Journal_02_prepare():     
  """Populate fin_L2_STG_Journal_02_prepare"""
  
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    global fin_L2_STG_Journal_02_prepare
    lstOfBracketType = ['JE_LINE_RANGE','JE_AMOUNT_RANGE']
      
    naDate = str(gen_L2_DIM_CalendarDate.select(col('dateSurrogateKey'))\
                        .filter(col('calendarDate') == lit('1900-01-01')).rdd.first()[0])
    
    fin_L1_TMP_JEAggregation = fin_L1_TD_Journal.groupBy("companyCode","fiscalYear","documentNumber")\
                          .agg(count("lineItem").alias("numberOfLineItem")\
                               ,sum(when(col("debitCreditIndicator")==lit("D") ,col("amountLC"))\
                                   .otherwise(0.00)).alias("amountLC")\
                           )
    fin_L1_TMP_JEAggregation.createOrReplaceTempView("fin_L1_TMP_JEAggregation")
    sqlContext.cacheTable('fin_L1_TMP_JEAggregation')
    
    fin_L2_TMP_BracketDetail =  gen_L2_DIM_Bracket.alias("brck")\
                           .join(gen_L2_DIM_BracketText.alias("bkt2"),on = "bracketSurrogateKey",\
                                how = "inner")\
                           .select(('brck.bracketSurrogateKey')\
                                   ,('brck.lowerLimit')\
                                   ,('brck.upperLimit')\
                                   ,('bkt2.bracketType')\
                                   ,('bkt2.bracketText'))\
                           .filter(col("bkt2.bracketType").isin(lstOfBracketType))
    fin_L2_TMP_BracketDetail.createOrReplaceTempView("fin_L2_TMP_BracketDetail")
    sqlContext.cacheTable('fin_L2_TMP_BracketDetail') 

    fin_L2_STG_Journal_02_prepare = spark.sql("	select    dou2.organizationUnitSurrogateKey			AS organizationUnitSurrogateKey	\
				,jou1.journalSurrogateKey					        AS journalSurrogateKey \
				,lrge.bracketSurrogateKey					        AS lineRangeSurrogateKey \
				,lrge.bracketText							        AS lineRangebracketText \
				,arge.bracketSurrogateKey					        AS amountRangeSurrogateKey \
				,arge.bracketText							        AS amountRangebracketText \
                ,IF(hol2.dateSurrogateKey IS NULL,"+naDate+",hol2.dateSurrogateKey) AS holidaySurrogateKey \
                ,0                                                  AS je_LinePatternID \
                ,IF(footPrint.PatternID IS NULL,0,footPrint.PatternID) AS je_footPrintPatternID \
             FROM	fin_L1_TD_Journal	jou1 \
             INNER JOIN fin_L1_TMP_JEAggregation jeAggr ON \
                                            ( \
                                            jou1.companyCode			= jeAggr.companyCode \
                                        AND jou1.fiscalYear			= jeAggr.fiscalYear \
                                        AND jou1.documentNumber		= jeAggr.documentNumber ) \
             INNER JOIN gen_L2_DIM_Organization	dou2	ON \
														( \
														dou2.companyCode			= jou1.companyCode \
                                                        AND dou2.companyCode		<> '#NA#') \
             LEFT JOIN fin_L2_TMP_BracketDetail lrge	ON \
																( \
																	jeAggr.numberOfLineItem	BETWEEN lrge.lowerLimit AND lrge.upperLimit \
                                                                    AND lrge.bracketType		='JE_LINE_RANGE' ) \
             LEFT JOIN fin_L2_TMP_BracketDetail arge	ON \
																( \
																	abs(jeAggr.amountLC)	   >= arge.lowerLimit \
																	AND abs(jeAggr.amountLC) < arge.upperLimit \
																	AND arge.bracketType		   ='JE_AMOUNT_RANGE' ) \
             LEFT JOIN gen_L2_DIM_CalendarDate	hol2 ON \
																( \
														 hol2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey \
                                                         AND hol2.calendarDate		= IF(jou1.creationDate IS NULL,'01-01-1900',jou1.creationDate) \
														AND hol2.isHoliday			= 1 ) \
             LEFT JOIN fin_L1_STG_JEAccountFootPrint footPrint	ON \
																				( \
																					footPrint.companyCode	= jou1.companyCode \
																					AND footPrint.fiscalYear	= jou1.fiscalYear \
																					AND footPrint.documentNumber = jou1.documentNumber )")
    
    fin_L2_STG_Journal_02_prepare = objDataTransformation.gen_convertToCDMandCache \
    (fin_L2_STG_Journal_02_prepare,'fin','L2_STG_Journal_02_prepare',False)

    recordCount2 = fin_L2_STG_Journal_02_prepare.count()
    
    if gl_countJET != recordCount2: 
        executionStatus="Number of records in fin_L2_FACT_Journal_02_prepare are \
        not reconciled with number of records in [fin].[L1_TD_Journal]"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
                
    executionStatus = "fin_L2_STG_Journal_02_prepare populated successfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    print(executionStatus)
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  finally:        
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L2_TMP_BracketDetail")
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_JEAggregation")
