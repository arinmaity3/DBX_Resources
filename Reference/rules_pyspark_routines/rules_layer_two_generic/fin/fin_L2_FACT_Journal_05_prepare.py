# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,lit,col,when
import sys
import traceback

def fin_L2_FACT_Journal_05_prepare(): 
  """Populate Journal_05 """
  try:
    
    global fin_L2_STG_Journal_05_prepare
  
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    
    fin_L2_STG_Journal_05_prepare =spark.sql(
                "select  "\
                "  jou1.journalSurrogateKey							 journalSurrogateKey"\
                " ,dat2.financialPeriodSurrogateKey					 postingDateSurrogateKey"\
                " ,cre2.dateSurrogateKey								 documentCreationDateSurrogateKey"\
                " ,clr2.dateSurrogateKey								 clearingDateSurrogateKey"\
                " ,docDate.dateSurrogateKey							 documentDateSurrogateKey"\
                " ,jou1.reversalFiscalYear							 documentReversalFiscalYear"\
                " ,dou2.organizationUnitSurrogateKey					 organizationUnitSurrogateKey"\
                " ,appr2.dateSurrogateKey							 approvalDateSurrogateKey"\
                "  FROM	fin_L1_TD_Journal jou1 "\
               " INNER JOIN gen_L2_DIM_Organization	dou2	ON "\
               " dou2.companyCode					= jou1.companyCode	 AND"\
               " dou2.companyCode					<> '#NA#'"\
                  
               " INNER JOIN fin_L2_DIM_FinancialPeriod dat2	ON "\
               " dat2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey		"\
               " AND dat2.postingDate				= case when isnull(nullif(jou1.postingDate,'')) \
                                                           then '1900-01-01' else jou1.postingDate end 	"\
               " AND dat2.financialPeriod			= jou1.financialPeriod "\
                  
               " INNER JOIN  gen_L2_DIM_CalendarDate  docDate  ON "\
               " dou2.organizationUnitSurrogateKey    = docDate.organizationUnitSurrogateKey"\
               " AND docDate.calendarDate			 = case when isnull(nullif(jou1.documentDate,''))\
                                                       then '1900-01-01' else jou1.documentDate 	end"\

               " INNER JOIN gen_L2_DIM_CalendarDate cre2	ON "\
               " cre2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey				"\
               " AND cre2.calendarDate				= case when isnull(nullif(jou1.creationDate,''))\
                                                        then '1900-01-01' else jou1.creationDate end 	"\

               " INNER JOIN gen_L2_DIM_CalendarDate	clr2	ON "\
                                 " clr2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey			"\
                                 " AND clr2.calendarDate				= case when isnull(nullif(jou1.clearingDate,'')) then '1900-01-01' else jou1.clearingDate end 	"\

                                 " INNER JOIN gen_L2_DIM_CalendarDate	appr2	ON "\
                                 " appr2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey					"\
                                 " AND appr2.calendarDate				= case when isnull(nullif(jou1.approvalDate,'')) then '1900-01-01' else jou1.approvalDate end	"\

                                   )
                                             
    fin_L2_STG_Journal_05_prepare = objDataTransformation.gen_convertToCDMandCache \
        (fin_L2_STG_Journal_05_prepare,'fin','L2_STG_Journal_05_prepare',True)

    journal05Count = fin_L2_STG_Journal_05_prepare.count()
    
    if journal05Count != gl_countJET:
      executionStatus="Number of records in fin_L2_FACT_Journal_05_prepare are \
      not reconciled with number of records in [fin].[L1_TD_Journal]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      warnings.warn(executionStatus)
      return [False,"fin_L2_STG_Journal_05_prepare population failed"]  

    executionStatus = "fin_L2_STG_Journal_05 population sucessfull"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
