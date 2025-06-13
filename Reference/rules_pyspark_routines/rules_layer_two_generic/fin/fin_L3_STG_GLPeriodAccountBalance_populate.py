# Databricks notebook source
import sys
import traceback
def fin_L3_STG_GLPeriodAccountBalance_populate():
	""" Populate global dataframe and SQL temp view fin_L3_STG_GLPeriodAccountBalance"""
	try:
		logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
		objGenHelper = gen_genericHelper()
		objDataTransformation = gen_dataTransformation()
		global fin_L3_STG_GLPeriodAccountBalance

		finYear     = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR') 
		finYear = '0' if finYear is None else finYear
		periodStart = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START')
		periodStart = '0' if periodStart is None else periodStart
		periodEnd   = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END')
		periodEnd = '0' if periodEnd is None else periodEnd
		prvfinYear = str(int(finYear)-1) 
		syfinYear = str(int(finYear)+1) 
		periodStartOrFinPeriod = 'ddt2.financialPeriod' if periodStart == '0' else periodStart
		periodEndOrFinPeriod = 'ddt2.financialPeriod' if periodEnd == '0' else periodEnd
		periodStartMinus1 = str(int(periodStart)-1)

		fin_L3_STG_GLPeriodAccountBalance = spark.sql("\
			             select glba2.glBalanceSurrogateKey\
						       ,orga2.organizationUnitSurrogateKey\
							   ,ddt2.financialPeriodSurrogateKey as dateSurrogateKey\
					           ,glba2.glAccountSurrogateKey\
							   ,glba2.debitCreditIndicator\
							   ,glba2.endingBalanceTrial\
							   ,ddt2.fiscalYear\
							   ,ddt2.financialPeriod\
					           ,glba2.annualizedBalanceDr\
							   ,glba2.annualizedBalanceCr\
			             from fin_L2_FACT_GLBalance	glba2\
			             inner join	gen_L2_DIM_Organization	orga2	\
						       on glba2.organizationUnitSurrogateKey=orga2.organizationUnitSurrogateKey \
			             inner join	fin_L2_DIM_FinancialPeriod	ddt2 \
			                   on glba2.dateSurrogateKey=ddt2.financialPeriodSurrogateKey \
			             inner join	fin_L2_DIM_GLAccount glac2	\
	                           on glba2.glAccountSurrogateKey=glac2.glAccountSurrogateKey and glac2.accountCategory= '#NA#'\
			              where ddt2.fiscalYear = '"+prvfinYear+"' OR ddt2.fiscalYear = '"+syfinYear+"'\
			                   or (ddt2.fiscalYear = '"+finYear+"' \
				              and (  ddt2.financialPeriod between -1 and "+periodStartMinus1+" \
				               or ddt2.financialPeriod between "+periodStartOrFinPeriod+" and "+periodEndOrFinPeriod+"))")
				  
		fin_L3_STG_GLPeriodAccountBalance = objDataTransformation.gen_convertToCDMandCache \
			(fin_L3_STG_GLPeriodAccountBalance,'fin','L3_STG_GLPeriodAccountBalance',True)

		vw_FACT_GLPeriodAccountBalance = fin_L3_STG_GLPeriodAccountBalance.alias('GLPBalance').\
			                 select (col('GLPBalance.analysisID').alias('analysisID') ,\
							 col('GLPBalance.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey'), \
							 col('GLPBalance.dateSurrogateKey').alias('dateSurrogateKey'), \
							 col('GLPBalance.glAccountSurrogateKey').alias('glAccountSurrogateKey'), \
							 col('GLPBalance.debitCreditIndicator').alias('debitCreditIndicator'), \
							 col('GLPBalance.endingBalanceTrial').alias('endingBalanceTrial'), \
							 col('GLPBalance.annualizedBalanceDr').alias('annualizedBalanceDr'), \
							 col('GLPBalance.annualizedBalanceCr').alias('annualizedBalanceCr'))

		dwh_vw_FACT_GLPeriodAccountBalance = objDataTransformation.gen_convertToCDMStructure_generate(\
			                              vw_FACT_GLPeriodAccountBalance,'dwh','vw_FACT_GLPeriodAccountBalance',False)[0]
		objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_GLPeriodAccountBalance,\
			                              gl_CDMLayer2Path + "fin_L2_FACT_GLBalance.parquet" )

		executionStatus = "fin_L3_STG_GLPeriodAccountBalance populated successfully"
		executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
		return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

	except Exception:
		executionStatus = objGenHelper.gen_exceptionDetails_log()       
		executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)  
		return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
		

