# Databricks notebook source
import sys
import traceback
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit,col,when

def fin_L3_STG_GLPPYCumulativeBalance_populate():
    """Populate fin_L3_STG_GLPPYCumulativeBalance """
    try:
      global fin_L3_STG_GLPPYCumulativeBalance
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      window = Window().orderBy(lit("1")) 
      periodStart = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START')
      periodEnd = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END')
      finYear = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR'))
      finYear = 0 if finYear is None else finYear
      prvfinYear1	= str(finYear-1)
      prvfinYear2	= str(finYear-2)
      syfinYear	= str(finYear+1)
      finYear = str(finYear)
      
      fin_L3_TMP_DistinctGLAKeyDetail=spark.sql("SELECT DISTINCT glAccountSurrogateKey"\
                            " FROM fin_L2_FACT_GLBalance"\
                            " INNER JOIN fin_L2_DIM_FinancialPeriod	ddt2 ON"\
                              " dateSurrogateKey =	ddt2.financialPeriodSurrogateKey"\
                            " WHERE (ddt2.fiscalYear BETWEEN "+prvfinYear2+" AND "+finYear+")"\
         )

      fin_L3_TMP_DistinctGLAKeyDetail.createOrReplaceTempView("fin_L3_TMP_DistinctGLAKeyDetail") 
      fin_L3_TMP_PeriodWiseMaxDateKey=spark.sql("SELECT	 MAX(financialPeriodSurrogateKey) financialPeriodSurrogateKey"\
            " ,fiscalYear"\
            " ,financialPeriod "\
            " FROM fin_L2_DIM_FinancialPeriod	"\
            " GROUP BY fiscalYear,financialPeriod"\
         )

      fin_L3_TMP_PeriodWiseMaxDateKey.createOrReplaceTempView("fin_L3_TMP_PeriodWiseMaxDateKey") 


      fin_L3_TMP_EndingBalanceTrial=spark.sql("SELECT "\
                "  orga2.organizationUnitSurrogateKey			AS	organizationUnitSurrogateKey	"\
                " ,glba2.glAccountSurrogateKey				AS	glAccountSurrogateKey"\
                " ,ddt2.fiscalYear							AS	fiscalYear"\
                " ,ddt2.financialPeriod						AS	financialPeriod"\
                " ,glac2.accountType							AS	accountType"\
                " ,SUM(glba2.endingBalanceTrial)				AS	endingBalanceTrial"\
                " ,MAX(ddt2.financialPeriodSurrogateKey)		AS	dateSurrogateKey"\
                " ,SUM( CASE glba2.debitCreditIndicator WHEN 'D' THEN glba2.endingBalanceTrial ELSE 0.00 END)\
                                                                AS debitBalanceTrial"\
                " ,SUM( CASE glba2.debitCreditIndicator WHEN 'C' THEN glba2.endingBalanceTrial ELSE 0.00 END) \
                                                                AS creditBalanceTrial"\
                " FROM		fin_L2_FACT_GLBalance	glba2"\
                " INNER JOIN	gen_L2_DIM_Organization		orga2	ON"\
                  " glba2.organizationUnitSurrogateKey	=	orga2.organizationUnitSurrogateKey"\
                " INNER JOIN	fin_L2_DIM_FinancialPeriod	ddt2	ON"\
                  " glba2.dateSurrogateKey				=	ddt2.financialPeriodSurrogateKey "\
                " INNER JOIN	fin_L2_DIM_GLAccount	glac2		ON "\
                  " glba2.glAccountSurrogateKey			=	glac2.glAccountSurrogateKey"\
                " AND glac2.accountCategory				=	'#NA#' "\
                  " WHERE	((ddt2.fiscalYear BETWEEN "+prvfinYear2+" AND " +prvfinYear1+")"\
                  " OR  ddt2.fiscalYear = "+syfinYear+""\
                  " OR (ddt2.fiscalYear = "+finYear +""\
                  " AND( ddt2.financialPeriod between  -1 and ("+periodStart+" -1)"\
                  " OR cast(ddt2.financialPeriod  as int) "\
                  " BETWEEN CASE WHEN "+periodStart+"  = 0   THEN ddt2.financialPeriod "\
                            " ELSE "+periodStart+""\
                            " END"\
                  " AND		CASE WHEN "+periodEnd +" = 0  THEN ddt2.financialPeriod "\
                            " ELSE "+periodEnd+""\
                            " END"\
                " ))"\
                " )"\
                " AND glba2.endingBalanceTrial <> 0.00 "\
                " GROUP BY "\
                " orga2.organizationUnitSurrogateKey"\
                " ,glba2.glAccountSurrogateKey"\
                " ,ddt2.fiscalYear"\
                " ,ddt2.financialPeriod"\
                " ,glac2.accountType"\
                )
      
      
      fin_L3_TMP_EndingBalanceTrial.createOrReplaceTempView("fin_L3_TMP_EndingBalanceTrial") 
      fin_L3_TMP_MaxFinPeriodDate=spark.sql("SELECT "\
           " glac2.organizationUnitSurrogateKey"\
           " ,glac2.glAccountSurrogateKey"\
           " ,fiscalYear"\
           " ,financialPeriod"\
           " ,accountType"\
           " ,MAX(financialPeriodSurrogateKey) as dateSurrogateKey"\
           " FROM	fin_L2_DIM_GLAccount	glac2	"\
           " INNER JOIN fin_L3_TMP_DistinctGLAKeyDetail dgkd2 ON "\
                                                                 " dgkd2.glAccountSurrogateKey			=	glac2.glAccountSurrogateKey "\
           " CROSS JOIN fin_L3_TMP_PeriodWiseMaxDateKey pmk2	"\
           " WHERE "\
           " glac2.accountCategory = '#NA#'"\
           " AND ((pmk2.fiscalYear BETWEEN "+prvfinYear2 +" AND "+prvfinYear1+")"\
                  "  OR pmk2.fiscalYear = "+syfinYear+""\
                  "  OR (pmk2.fiscalYear = "+finYear+" "\
                         " AND( cast(pmk2.financialPeriod as int) between  -1 and ("+periodStart+" -1)"\
                             " OR cast(pmk2.financialPeriod as int)"\
                                "  BETWEEN CASE WHEN "+ periodStart +" = 0"\
                                         " THEN pmk2.financialPeriod "\
                                         " ELSE "+ periodStart +""\
                                         " END "\
                                 " AND		CASE WHEN "+ periodEnd+" = 0"\
                                         " THEN pmk2.financialPeriod "\
                                    " ELSE "+ periodEnd+""\
                                    " END"\
                            " ))"\
                        " )"\
           " GROUP BY"\
           " glac2.organizationUnitSurrogateKey"\
           " ,glac2.glAccountSurrogateKey"\
           " ,fiscalYear"\
           " ,financialPeriod"\
           " ,accountType"\
                                           )

      fin_L3_TMP_MaxFinPeriodDate.createOrReplaceTempView("fin_L3_TMP_MaxFinPeriodDate") 

      fin_L3_TMP_GLPPYCumulativeBalance=spark.sql("SELECT "\
            "  case when ISNULL(ebl2.organizationUnitSurrogateKey)then mpd2.organizationUnitSurrogateKey \
                    else  ebl2.organizationUnitSurrogateKey end   	organizationUnitSurrogateKey"\
            " ,case when ISNULL(ebl2.glAccountSurrogateKey)then  mpd2.glAccountSurrogateKey \
                    else 	ebl2.glAccountSurrogateKey	 end glAccountSurrogateKey"\
            " ,case when ISNULL(ebl2.fiscalYear)then mpd2.fiscalYear else ebl2.fiscalYear end  	fiscalYear"\
            " ,case when ISNULL(ebl2.financialPeriod)then mpd2.financialPeriod  else ebl2.financialPeriod end financialPeriod"\
            " ,case when ISNULL(ebl2.accountType)then mpd2.accountType else ebl2.accountType end accountType"\
            " ,case when ISNULL(ebl2.endingBalanceTrial)then 0 else ebl2.endingBalanceTrial end	endingBalanceTrial"\
            " ,case when ISNULL(ebl2.dateSurrogateKey)then mpd2.dateSurrogateKey else mpd2.dateSurrogateKey end 	 dateSurrogateKey	"\
            " ,case when ISNULL(ebl2.debitBalanceTrial)then 0	else ebl2.debitBalanceTrial end debitBalanceTrial" \
            " ,case when ISNULL(ebl2.creditBalanceTrial)then 0	else ebl2.creditBalanceTrial end  creditBalanceTrial"\
            " FROM	fin_L3_TMP_EndingBalanceTrial	ebl2"\
            " RIGHT JOIN fin_L3_TMP_MaxFinPeriodDate mpd2 ON"\
            " ebl2.organizationUnitSurrogateKey		= mpd2.organizationUnitSurrogateKey"\
            " AND ebl2.glAccountSurrogateKey	    = mpd2.glAccountSurrogateKey"\
            " AND ebl2.fiscalYear				    = mpd2.fiscalYear"\
            " AND ebl2.financialPeriod				= mpd2.financialPeriod"\
            )



      fin_L3_TMP_GLPPYCumulativeBalance.createOrReplaceTempView("fin_L3_TMP_GLPPYCumulativeBalance") 


      fin_L3_STG_GLPPYCumulativeBalance=spark.sql("SELECT "\
                  "  organizationUnitSurrogateKey"\
                  " ,glAccountSurrogateKey"\
                  " ,dateSurrogateKey"\
                  " ,endingBalanceTrial	"\
                  " ,fiscalYear"\
                  " ,financialPeriod"\
                  " ,SUM(endingBalanceTrial) OVER (PARTITION BY glAccountSurrogateKey,organizationUnitSurrogateKey,fiscalYear \
                               ORDER BY financialPeriod ROWS UNBOUNDED PRECEDING) cumulativeBalance "\
                  " , SUM(debitBalanceTrial) OVER (PARTITION BY glAccountSurrogateKey,organizationUnitSurrogateKey,fiscalYear \
                               ORDER BY financialPeriod ROWS UNBOUNDED PRECEDING) cumulativeDebitBalance"\
                   " ,SUM(creditBalanceTrial) OVER (PARTITION BY glAccountSurrogateKey,organizationUnitSurrogateKey,fiscalYear \
                               ORDER BY financialPeriod ROWS UNBOUNDED PRECEDING) cumulativeCreditBalance "\
              "FROM	fin_L3_TMP_GLPPYCumulativeBalance"\
                                         ) 
      fin_L3_STG_GLPPYCumulativeBalance=fin_L3_STG_GLPPYCumulativeBalance.withColumn("balanceID",row_number().over(window))
      
      fin_L3_STG_GLPPYCumulativeBalance = objDataTransformation.gen_convertToCDMandCache \
          (fin_L3_STG_GLPPYCumulativeBalance,'fin','L3_STG_GLPPYCumulativeBalance',True)
      
      vw_FACT_GLPPYCumulativeBalance = fin_L3_STG_GLPPYCumulativeBalance.alias('CBalance').\
			                   select (col('analysisID').alias('analysisID') ,\
                               col('organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey') ,\
                               col('glAccountSurrogateKey').alias('glAccountSurrogateKey') ,\
                               col('dateSurrogateKey').alias('dateSurrogateKey') ,\
                               col('endingBalanceTrial').alias('endingBalanceTrial') ,\
                               col('cumulativeBalance').alias('cumulativeBalance') ,\
                               col('cumulativeDebitBalance').alias('cumulativeDebitBalance') ,\
                               col('cumulativeCreditBalance').alias('cumulativeCreditBalance') )

      dwh_vw_FACT_GLPPYCumulativeBalance = objDataTransformation.gen_convertToCDMStructure_generate(\
                         vw_FACT_GLPPYCumulativeBalance,'dwh','vw_FACT_GLPPYCumulativeBalance',False)[0]
      objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_GLPPYCumulativeBalance,\
                         gl_CDMLayer2Path + "fin_L2_FACT_GLCummulativeBalance.parquet" )
        
      executionStatus = "fin_L3_STG_GLPPYCumulativeBalance populated successfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception:
      executionStatus = objGenHelper.gen_exceptionDetails_log()
      print(executionStatus)
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
