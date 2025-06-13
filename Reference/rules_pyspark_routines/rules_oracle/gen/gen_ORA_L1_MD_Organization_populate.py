# Databricks notebook source
import sys
import traceback

def gen_ORA_L1_MD_Organization_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    global gen_L1_MD_Organization

    analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))
    clientid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'CLIENT_ID'))

    df_gen_L1_MD_Organization = fin_L0_STG_ClientBusinessStructure.alias('CBS')\
                                        .join(knw_LK_CD_ReportingSetup.alias('LRS'),(col("CBS.KAAP_COMPANY_CODE") == col("LRS.companyCode")),how="inner")\
                                        .select(col('LRS.COMPANYCODE').alias('CompanyCode'),col('CBS.DESCRIPTION').alias('CompanyCodeDescription'),\
                                               when(col('CBS.countryCode').isNull(),lit(''))\
                                               .otherwise(col('CBS.countryCode')).alias('countryCode'),\
                                               when(col('CBS.COUNTRY').isNull(),lit('#NA#'))\
                                               .otherwise(col('CBS.COUNTRY')).alias('CountryName'),\
                                               when(col('CBS.City').isNull(),lit('#NA#'))\
                                               .otherwise(col('CBS.City')).alias('City'),\
                                               col('CBS.LEDGER_CURRENCY').alias('LocalCurrency'),col('CBS.CHART_OF_ACCOUNTS_ID').alias('ChartOfAccounts'),\
                                               col('lrs.reportingGroup').alias('ReportingGroup'),col('LRS.reportingLanguageCode').alias('ReportingLanguage'),\
                                               col('CBS.LEDGER_CURRENCY').alias('ReportingCurrency'),lit(analysisid).alias('analysisid'),lit(clientid).alias('clientid'),\
                                               when(col('LRS.clientName').isNull(),lit('#NA#'))\
                                               .otherwise(col('LRS.clientName')).alias('ClientName'),col('CBS.BALANCING_SEGMENT').alias('Segment01'),\
                                              lit('#NA#').alias('segment02'),lit('#NA#').alias('segment03'),lit('#NA#').alias('segment04'),lit('#NA#').alias('segment05')).distinct()

    gen_L1_MD_Organization = objDataTransformation.gen_convertToCDMandCache \
        (df_gen_L1_MD_Organization,'gen','L1_MD_Organization',targetPath=gl_CDMLayer1Path)
 
    executionStatus = "gen_L1_MD_Organization populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
														
