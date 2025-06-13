# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,row_number,coalesce

def otc_SAP_L1_MD_InternationalCommercialterm_populate(): 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    global otc_L1_MD_InternationalCommercialterm
             
    w = Window().orderBy(asc(col('value')))                        
    erp_TINCT_DistinctCount  = erp_TINCT.count()      
    
    if (erp_TINCT_DistinctCount != 0):
      TMP_InternationalCommClientDataReportingLanguage = erp_TINCT.alias('tinct_erp')\
               .join(knw_LK_ISOLanguageKey.alias('lngkey'),\
                      ((col('lngkey.sourceSystemValue'))==(col('tinct_erp.SPRAS'))),how="inner")\
               .join(knw_clientDataReportingLanguage.alias('rlg1')\
                      ,((col('rlg1.clientCode')==(col('tinct_erp.MANDT')))\
                      &(col('rlg1.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
               .select(col('rlg1.clientCode').alias('clientCode')\
                      ,col('tinct_erp.INCO1').alias('value')\
                      ,col('tinct_erp.BEZEI').alias('description')\
                      ,col('rlg1.languageCode').alias('languageCode'))\
               .distinct()
        
      InClitDtReportingLanguageNotExist = erp_TINCT.alias('tinct_erp')\
              .join(TMP_InternationalCommClientDataReportingLanguage.alias('rpt')\
                    ,((col('rpt.clientCode') == col('tinct_erp.MANDT'))\
                    &(col('rpt.value')==(col('tinct_erp.INCO1')))) ,how='leftanti')\
              .select(col('tinct_erp.MANDT'),col('tinct_erp.SPRAS')\
                    ,col('tinct_erp.INCO1'),col('tinct_erp.BEZEI'))
  
      InternationalCommercialTerm = InClitDtReportingLanguageNotExist.alias('tinct_erp')\
              .join (knw_LK_ISOLanguageKey.alias('lngkey'),\
                    (col('lngkey.sourceSystemValue').eqNullSafe(col('tinct_erp.SPRAS'))), 'left')\
              .join(knw_clientDataSystemReportingLanguage.alias('rlg2')\
                  ,((col('rlg2.clientCode').eqNullSafe(col('tinct_erp.MANDT')))\
                  &(col('rlg2.languageCode').eqNullSafe(col('lngkey.targetSystemValue')))),'left')\
              .select(col('tinct_erp.MANDT').alias('clientCode')\
                  ,col('tinct_erp.INCO1').alias('value')\
                  ,col('tinct_erp.BEZEI').alias('description')\
                  ,coalesce(col('rlg2.languageCode'),lit('NA')).alias('languageCode'))\
              .distinct()
        
      InternationalCommercialTerm = InternationalCommercialTerm\
                                        .union(TMP_InternationalCommClientDataReportingLanguage)
      
      otc_L1_MD_InternationalCommercialterm = InternationalCommercialTerm\
                                                .withColumn("id", row_number().over(w))
      
        
    else:
      ReportingSetup = knw_LK_CD_ReportingSetup.select(col('clientCode').alias('clientCode')\
                         ,col('KPMGDataReportingLanguage').alias('languageCode')).distinct()
   
      otc_L1_MD_InternationalCommercialterm = knw_LK_GD_InternationalCommercialTerm.alias('gdict')\
                         .crossJoin(ReportingSetup.alias('rps'))\
                         .select(col('gdict.salesDocumentIncoTermId').alias('id')\
                                ,col('rps.clientCode').alias('clientCode')\
                                ,col('gdict.salesDocumentIncoTermValue').alias('value')\
                                ,col('gdict.salesDocumentIncoTermDescription').alias('description')\
                                ,col('rps.languageCode').alias('languageCode'))\
                         .distinct()
       
      
    otc_L1_MD_InternationalCommercialterm =  objDataTransformation.gen_convertToCDMandCache \
         (otc_L1_MD_InternationalCommercialterm,'otc','L1_MD_InternationalCommercialterm',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "L1_MD_InternationalCommercialterm populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
