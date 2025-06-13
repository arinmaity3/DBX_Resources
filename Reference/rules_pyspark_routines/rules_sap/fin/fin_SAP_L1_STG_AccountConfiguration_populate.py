# Databricks notebook source
import sys
import traceback

def fin_SAP_L1_STG_AccountConfiguration_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    global fin_L1_STG_AccountConfiguration
    
    fin_L1_STG_AccountConfiguration = erp_T012K.alias('t012k_erp')\
                                        .join(knw_LK_CD_ReportingSetup.alias('repsetup'),((col("t012k_erp.MANDT") == col("repsetup.clientCode"))\
                                            & (col("t012k_erp.BUKRS") == col("repsetup.companyCode") )),"inner")\
                                        .select(col('t012k_erp.BUKRS').alias('companyCode'),col('repsetup.chartOfAccount').alias('chartofAccount')\
                                               ,col('t012k_erp.HKONT').alias('accountNumber'),lit('HouseBankAccounts').alias('configurationName'))\
                                        .union(erp_T030.alias('t030_erp')\
                                        .join(knw_LK_CD_ReportingSetup.alias('repsetup'),((col("t030_erp.MANDT") == col("repsetup.clientCode"))\
                                            & (col("t030_erp.KTOPL") == col("repsetup.chartOfAccount") )),"inner")\
                                        .select(col('repsetup.companyCode').alias('companyCode'),col('t030_erp.KTOPL').alias('chartofAccount')\
                                               ,col('t030_erp.KONTS').alias('accountNumber'),lit('StandardAccountsBSP').alias('configurationName'))\
                                        .filter(col("t030_erp.KTOSL") == 'BSP'))\
                                        .union(erp_T030.alias('t030_erp')\
                                        .join(knw_LK_CD_ReportingSetup.alias('repsetup'),((col("t030_erp.MANDT") == col("repsetup.clientCode"))\
                                            & (col("t030_erp.KTOPL") == col("repsetup.chartOfAccount") )),"inner")\
                                        .select(col('repsetup.companyCode').alias('companyCode'),col('t030_erp.KTOPL').alias('chartofAccount')\
                                               ,col('t030_erp.KONTH').alias('accountNumber'),lit('StandardAccountsBSP').alias('configurationName'))\
                                        .filter(col("t030_erp.KTOSL") == 'BSP'))\
                                        .union(erp_T030.alias('t030_erp')\
                                        .join(knw_LK_CD_ReportingSetup.alias('repsetup'),((col("t030_erp.MANDT") == col("repsetup.clientCode"))\
                                            & (col("t030_erp.KTOPL") == col("repsetup.chartOfAccount") )),"inner")\
                                        .select(col('repsetup.companyCode').alias('companyCode'),col('t030_erp.KTOPL').alias('chartofAccount')\
                                               ,col('t030_erp.KONTS').alias('accountNumber'),lit('StandardAccountsKDF').alias('configurationName'))\
                                        .filter(col("t030_erp.KTOSL") == 'KDF'))\
                                        .union(erp_T030.alias('t030_erp')\
                                        .join(knw_LK_CD_ReportingSetup.alias('repsetup'),((col("t030_erp.MANDT") == col("repsetup.clientCode"))\
                                            & (col("t030_erp.KTOPL") == col("repsetup.chartOfAccount") )),"inner")\
                                        .select(col('repsetup.companyCode').alias('companyCode'),col('t030_erp.KTOPL').alias('chartofAccount')\
                                               ,col('t030_erp.KONTH').alias('accountNumber'),lit('StandardAccountsKDF').alias('configurationName'))\
                                        .filter(col("t030_erp.KTOSL") == 'KDF'))\
                                        .distinct()

    fin_L1_STG_AccountConfiguration = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_AccountConfiguration,'fin','L1_STG_AccountConfiguration',targetPath=gl_CDMLayer1Path)
		
    executionStatus = "fin_L1_STG_AccountConfiguration populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

                           
