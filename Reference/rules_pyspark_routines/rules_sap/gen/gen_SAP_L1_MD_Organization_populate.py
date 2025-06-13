# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when
def gen_SAP_L1_MD_Organization_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))
    clientid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'CLIENT_ID'))
    
    global gen_L1_MD_Organization    
    #global erp_T001_NotExists

    df_OrganizationClientDataReportingLanguage=erp_T001.alias("t001")\
        .join (erp_T005T.alias("t005t")\
            ,((col("t001.MANDT")  == col("t005t.MANDT"))\
              & (col("t001.LAND1") == col("t005t.LAND1"))), "inner")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
            ,(col("lngkey.sourceSystemValue") == col("t005t.SPRAS")), "inner")\
        .join (knw_clientDataReportingLanguage.alias("rlg1")\
            ,((col("rlg1.clientCode")   == col("t001.MANDT"))\
              & (col("rlg1.companyCode") == col("t001.BUKRS") )\
              & (col("rlg1.languageCode") == col("lngkey.targetSystemValue") )\
              ), "inner")\
        .select(col('t001.MANDT').alias('clientCode')\
             ,col('t001.BUKRS').alias('companyCode')\
             ,col('t001.LAND1').alias('countryCode')\
             ,col('t005t.LANDX').alias('countryName')\
             ,col('rlg1.languageCode'))\
        .distinct()
    
    erp_T001_NotExists = erp_T001.alias('t001')\
        .join(df_OrganizationClientDataReportingLanguage.alias('org')\
           ,((col("t001.MANDT")   == col("org.clientCode"))\
             & (col("t001.BUKRS")   == col("org.companyCode") )\
             & (col("t001.LAND1")   == col("org.countryCode") )\
          ), 'leftanti')\
        .select(col("t001.MANDT"),col("t001.BUKRS"),col("t001.LAND1"))
      
    df_OrganizationClientDataSystemReportingLanguage=erp_T001_NotExists.alias("t001")\
        .join (knw_clientDataSystemReportingLanguage.alias("rlg1")\
          ,((col("rlg1.clientCode")   == col("t001.MANDT"))\
            & (col("rlg1.companyCode")   == col("t001.BUKRS") )), "inner")\
        .join (erp_T005T.alias("t005t")\
          ,((col("t001.MANDT").eqNullSafe(col("t005t.MANDT")))\
            & (col("t001.LAND1").eqNullSafe(col("t005t.LAND1")))\
            ), "left")\
        .join (knw_LK_ISOLanguageKey.alias("lngkey")\
          ,((col("lngkey.sourceSystemValue")    == col("t005t.SPRAS"))\
            & (col("lngkey.targetSystemValue")  == col("rlg1.languageCode") )\
           ), "inner")\
        .select(col('t001.MANDT').alias('clientCode')\
          ,col('t001.BUKRS').alias('companyCode')\
          ,col('t001.LAND1').alias('countryCode')\
          ,when((col('t005t.LANDX').isNull()),'#NA#')\
            .otherwise(col('t005t.LANDX')).alias('countryName')\
          ,when((col('rlg1.languageCode').isNull()),'#NA#')\
            .otherwise(col('rlg1.languageCode')).alias('languageCode')\
               )                       
    df_OrganizationcountryName=df_OrganizationClientDataReportingLanguage\
        .union(df_OrganizationClientDataSystemReportingLanguage)   
       
    gen_L1_MD_Organization=erp_T001.alias("t001")\
       .join (knw_LK_CD_ReportingSetup.alias("lars")\
         ,((col("lars.clientCode")   == col("t001.MANDT"))\
           & (col("lars.companyCode")   == col("t001.BUKRS") )), "inner")\
       .join (df_OrganizationcountryName.alias("cntry")\
         ,((col("cntry.clientCode").eqNullSafe(col("lars.clientCode")))\
           & (col("cntry.companyCode").eqNullSafe(col("lars.companyCode")))\
           & (col("cntry.countryCode").eqNullSafe(col("t001.LAND1")))\
          ), "left")\
       .select( col('t001.BUKRS').alias("companyCode")\
          ,col('t001.BUTXT').alias("companyCodeDescription")\
          ,col('t001.LAND1').alias("countryCode")\
          ,col('cntry.countryName').alias("countryName")\
          ,col('t001.ORT01').alias("city")\
          ,col('t001.WAERS').alias("localCurrency")\
          ,col('t001.KTOPL').alias("chartOfAccounts")\
          ,col('lars.reportingGroup').alias("reportingGroup")\
          ,col('lars.reportingLanguageCode').alias("reportingLanguage")\
          ,col('lars.reportingCurrencyCode').alias("reportingCurrency")\
          ,lit(analysisid).alias('analysisID')\
          ,lit(clientid).alias('clientID')\
          ,col('lars.clientName').alias("clientName")\
          ,lit(None).alias("segment01")\
          ,lit(None).alias("segment02")\
          ,lit(None).alias("segment03")\
          ,lit(None).alias("segment04")\
          ,lit(None).alias("segment05")\
         )\
        .distinct()
    
    gen_L1_MD_Organization =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_Organization,'gen','L1_MD_Organization',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_MD_Organization populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
