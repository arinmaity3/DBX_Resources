# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,coalesce

def ptp_SAP_L1_MD_PurchaseOrderSubType_populate(): 
    try:
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

      global ptp_L1_MD_PurchaseOrderSubType
      
      ptp_SAP_L0_TMP_PurchaseOrderSubTypeReportingLanguage = erp_T161T.alias('t161t_erp')\
              .join(knw_LK_CD_ReportingSetup.alias('lkrs'),\
                        (col('lkrs.clientCode')==(col('t161t_erp.MANDT'))),how='inner')\
              .join(knw_clientDataReportingLanguage.alias('rlg1')\
                   ,((col('t161t_erp.MANDT')==(col('rlg1.clientCode')))\
                   &(col('lkrs.companyCode')==(col('rlg1.companyCode')))),how='inner')\
              .join(knw_LK_ISOLanguageKey.alias('isolk')\
                   ,(col('isolk.sourceSystemValue')==(col('t161t_erp.SPRAS'))),how='inner')\
              .select(col('t161t_erp.MANDT').alias('purchaseOrderSubTypeClientCode')\
                     ,col('lkrs.companyCode').alias('purchaseOrderSubTypeCompanyCode')\
                     ,col('t161t_erp.BSTYP').alias('purchaseOrderType')\
                     ,col('t161t_erp.BSART').alias('purchaseOrderSubType')\
                     ,col('t161t_erp.BATXT').alias('purchaseOrderSubTypeDescription')\
                     ,col('rlg1.languageCode').alias('purchaseOrderSubTypeLanguageCode'))\
              .distinct()

      
      PurchaseOrderSubTypeReportingLanguage_Notexist = erp_T161T.alias('t161t_erp')\
             .join(knw_LK_CD_ReportingSetup.alias('lkrs')\
                      ,(col('lkrs.clientCode')==(col('t161t_erp.MANDT'))),how='inner')\
             .join(ptp_SAP_L0_TMP_PurchaseOrderSubTypeReportingLanguage.alias('posc')\
                     ,((col('posc.purchaseOrderSubTypeClientCode') == col('t161t_erp.MANDT'))\
                     &(col('posc.purchaseOrderSubType')==(col('t161t_erp.BSART')))\
                     &(col('posc.purchaseOrderType')==(col('t161t_erp.BSTYP')))\
                     &(col('posc.purchaseOrderSubTypeCompanyCode')==(col('lkrs.companyCode')))) ,how='leftanti')
      
      PurchaseOrderSubTypeReportingLanguage_df1 = PurchaseOrderSubTypeReportingLanguage_Notexist.alias('t161t_erp')\
                .join(knw_LK_CD_ReportingSetup.alias('lkrs')\
                        ,(col('lkrs.clientCode')==(col('t161t_erp.MANDT'))),how='inner')\
                .join(knw_clientDataReportingLanguage.alias('rlg1')\
                         ,((col('t161t_erp.MANDT')==(col('rlg1.clientCode')))\
                         &(col('lkrs.companyCode')==(col('rlg1.companyCode')))),how='inner')\
                .join(knw_LK_ISOLanguageKey.alias('isolk')\
                        ,(col('isolk.sourceSystemValue')==(col('t161t_erp.SPRAS'))),how='inner')\
                .select(col('t161t_erp.MANDT').alias('purchaseOrderSubTypeClientCode')\
                       ,col('lkrs.companyCode').alias('purchaseOrderSubTypeCompanyCode')\
                       ,col('t161t_erp.BSTYP').alias('purchaseOrderType')
                       ,col('t161t_erp.BSART').alias('purchaseOrderSubType')
                       ,coalesce(col('t161t_erp.BATXT'),lit('#NA#')).alias('purchaseOrderSubTypeDescription')
                       ,coalesce(col('rlg1.languageCode'),lit('NA')).alias('purchaseOrderSubTypeLanguageCode'))\
                 .distinct()

      ptp_L1_MD_PurchaseOrderSubType = PurchaseOrderSubTypeReportingLanguage_df1.union(ptp_SAP_L0_TMP_PurchaseOrderSubTypeReportingLanguage)
              
      ptp_L1_MD_PurchaseOrderSubType =  objDataTransformation.gen_convertToCDMandCache \
         (ptp_L1_MD_PurchaseOrderSubType,'ptp','L1_MD_PurchaseOrderSubType',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "L1_MD_PurchaseOrderSubType populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
     
        

