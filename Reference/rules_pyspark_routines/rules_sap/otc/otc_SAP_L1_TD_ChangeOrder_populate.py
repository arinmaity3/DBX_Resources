# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when
from pyspark.sql import functions as F


def otc_SAP_L1_TD_ChangeOrder_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    
    global otc_L1_TD_ChangeOrder
    
    df_L1_TD_ChangeOrder = erp_CDPOS.alias('cdpos_erp')\
            .join(erp_CDHDR.alias('cdhdr_erp')\
                           ,(col('cdhdr_erp.MANDANT')==(col('cdpos_erp.MANDANT')))&\
                            (col('cdhdr_erp.OBJECTCLAS')==(col('cdpos_erp.OBJECTCLAS')))&\
                            (col('cdhdr_erp.OBJECTID')==(col('cdpos_erp.OBJECTID')))&\
                           (col('cdhdr_erp.CHANGENR')==(col('cdpos_erp.CHANGENR'))),how='inner') \
            .join(knw_KPMGDataReportingLanguage.alias('rpt'),\
                 (col('cdpos_erp.MANDANT').eqNullSafe(col('rpt.clientCode'))),how='left')\
            .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('odcf'),\
                 ((col('odcf.businessDatatype').eqNullSafe(lit('Ordering Document Change Field')))&\
                 (col('cdpos_erp.FNAME').eqNullSafe(col('odcf.sourceSystemValue')))&\
                 (col('odcf.targetLanguageCode').eqNullSafe(col('rpt.languageCode')))&\
                 (col('odcf.sourceERPSystemID').eqNullSafe(lit('erpGENSystemID')))),how='left')\
            .filter((col('cdpos_erp.TABNAME').isin('VBAK','VBAP'))\
                     &(col('cdpos_erp.FNAME').isin('MEINS','ZIEME','MATNR','UMVKN'\
                           ,'UMZIN','CMPRE_FLT','CMPRE','LFMNG','KWMENG','VRKME'
                           ,'NETPR','NETWR','SHKZG','UEBTK','UEBTO','UNTTO','ZWERT','ZMENG','UMZIZ','SMENG',
                           'ABLFZ','ABSFZ','KBVER','FMENG','LSMENG','KBMENG','KLMENG','UMVKZ','KPEIN','KMEIN')))\
            .select(substring(col('cdpos_erp.TABKEY'),4,10).alias('orderingChangedocumentnumber')\
                ,col('cdpos_erp.TABKEY').alias('TABKEY')\
				,expr("substring(TABKEY,-6,length(TABKEY))").alias('orderingchangeDocumentLineItem')\
				,col('cdpos_erp.CHANGENR').alias('orderingChangeDocumentChangeNumber')	\
				,col('cdhdr_erp.USERNAME').alias('orderingChangeDocumentChangeUser')\
				,col('cdhdr_erp.UDATE').alias('orderingChangeDocumentChangedate')\
				,col('cdhdr_erp.TCODE').alias('orderingChangeTransactionCode')\
				,col('cdpos_erp.CHNGIND').alias('orderingDocumentChangeIndicator')\
				,when(col('odcf.targetSystemValueDescription').isNull(),lit('Unknown'))\
                                      .otherwise(col('odcf.targetSystemValueDescription'))\
	                          .alias('orderingDocumentChangeField')\
				,col('cdpos_erp.VALUE_NEW').alias('orderingChangeValueNew')\
				,col('cdpos_erp.VALUE_OLD').alias('orderingdocumentChangeValueOld')\
                            ,col('cdhdr_erp.UTIME').alias('orderingChangeDocumentChangeTime_dmyCol'))

    otc_L1_TD_ChangeOrder = df_L1_TD_ChangeOrder.withColumn("orderingChangeDocumentChangeTime_",
        F.concat(F.expr("substring(orderingChangeDocumentChangeTime_dmyCol, 1, 2)"),F.lit(':'),\
                 F.expr("substring(orderingChangeDocumentChangeTime_dmyCol, 3,length(orderingChangeDocumentChangeTime_dmyCol))")))

    otc_L1_TD_ChangeOrder = otc_L1_TD_ChangeOrder.withColumn("orderingChangeDocumentChangeTime",
                        F.concat(F.expr("substring(orderingChangeDocumentChangeTime_, 1, 5)"),F.lit(':')\
                    ,F.expr("substring(orderingChangeDocumentChangeTime_dmyCol, 5,length(orderingChangeDocumentChangeTime_dmyCol))")))
    
    otc_L1_TD_ChangeOrder = otc_L1_TD_ChangeOrder.select('orderingChangedocumentnumber','orderingchangeDocumentLineItem'\
                           ,'orderingChangeDocumentChangeNumber','orderingChangeDocumentChangeUser'\
                           ,'orderingChangeDocumentChangedate','orderingChangeTransactionCode'\
                           ,'orderingDocumentChangeIndicator','orderingDocumentChangeField'\
                           ,'orderingChangeValueNew','orderingdocumentChangeValueOld'\
                           ,'orderingChangeDocumentChangeTime')
   
    otc_L1_TD_ChangeOrder =  objDataTransformation.gen_convertToCDMandCache \
           (otc_L1_TD_ChangeOrder,'otc','L1_TD_ChangeOrder',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_TD_ChangeOrder populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

