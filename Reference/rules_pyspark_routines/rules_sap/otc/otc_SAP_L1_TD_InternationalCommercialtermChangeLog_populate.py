# Databricks notebook source
import sys
import traceback
from pyspark.sql import Window
from pyspark.sql.functions import expr,lit,col,when,length

def otc_SAP_L1_TD_InternationalCommercialtermChangeLog_populate(): 
    try:
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

      global otc_L1_TD_InternationalCommercialtermChangeLog
      
      startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
      endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()


      incoTermChangeDocumentNumber = substring(col('cdp0.TABKEY'),4,10)
      incoTermChangeDateTime = concat(substring(col('cdh0.UDATE').cast("date"), 1, 10),lit(' '),substring(col('cdh0.UTIME'), 1, 2),lit(':'),\
                              substring(substring(col('cdh0.UTIME'), 3, 4),1,2),lit(':'),substring(col('cdh0.UTIME'),-2,2))

      VALUE_OLD_NULL = when(col('cdp0.VALUE_OLD').isNull(),lit(''))\
                            .otherwise(col('cdp0.VALUE_OLD'))

      w = Window().partitionBy('cdp0.MANDANT',lit(incoTermChangeDocumentNumber))
      
      L0_TMP_InternationalCommercialtermChangeLog_CTE = erp_CDPOS.alias('cdp0')\
                                              .join(erp_CDHDR.alias('cdh0'),((col("cdp0.MANDANT") == col("cdh0.MANDANT"))\
                                                  & (col("cdp0.OBJECTCLAS") == col("cdh0.OBJECTCLAS"))\
                                                  & (col("cdp0.OBJECTID") == col("cdh0.OBJECTID"))\
                                                  & (col("cdp0.CHANGENR") == col("cdh0.CHANGENR")) ),"inner")\
                                              .filter((col('cdh0.OBJECTCLAS').isin('FAKTBELEG','LIEFERUNG')) & (col('cdp0.TABNAME').isin('VBRK','LIKP')) & \
                                                      (col('cdp0.FNAME') == 'INCO1') & (col("cdh0.UDATE").cast("date").between(lit(startDate),lit(endDate))))\
                                              .select(col('cdp0.MANDANT').alias('incoTermChangeClient')\
                                                     ,min(concat(lit(incoTermChangeDateTime).cast("string"),lit(VALUE_OLD_NULL).cast("string"))).over(w).alias('incoTermInitialValue')\
                                                     ,col('cdp0.VALUE_NEW').alias('incoTermChangeValueNew')\
                                                     ,col('cdp0.VALUE_OLD').alias('incoTermChangeValueOld'),lit(incoTermChangeDateTime).alias('incoTermChangeDateTime')\
                                                     ,max(lit(incoTermChangeDateTime)).over(w).alias('incoTermLatestChangeDateTime')\
                                                     ,count('*').over(w).alias('incoTermTotalChangeNumber')\
                                                     ,lit(incoTermChangeDocumentNumber).alias('incoTermChangeDocumentNumber')
                                                     ,col('cdp0.CHANGENR').alias('incoTermChangeChangeNumber')\
                                                     ,col('cdh0.USERNAME').alias('incoTermChangeChangeUser'),col('cdp0.CHNGIND').alias('incoTermChangeChangeIndicator'))

      SAP_L0_TMP_InternationalCommercialtermChangeLog = L0_TMP_InternationalCommercialtermChangeLog_CTE\
                                                      .select(col('incoTermChangeClient')\
                                                       ,expr("substring(incoTermInitialValue,length(incoTermInitialValue),-23)").alias('incoTermInitialValue')\
                                                       ,col('incoTermChangeValueNew'),col('incoTermChangeValueOld')\
                                                       ,col('incoTermChangeDateTime'),col('incoTermLatestChangeDateTime')\
                                                       ,col('incoTermTotalChangeNumber'),col('incoTermChangeDocumentNumber'),col('incoTermChangeChangeNumber')\
                                                       ,col('incoTermChangeChangeUser'),col('incoTermChangeChangeIndicator'))

      otc_L1_TD_InternationalCommercialtermChangeLog = SAP_L0_TMP_InternationalCommercialtermChangeLog\
                                                         .filter(col('incoTermChangeDateTime') == col('incoTermLatestChangeDateTime'))\
                                                         .select(col('incoTermChangeClient'),col('incoTermInitialValue'),col('incoTermChangeValueNew'),col('incoTermChangeValueOld')\
                                                                ,col('incoTermLatestChangeDateTime'),col('incoTermTotalChangeNumber'),col('incoTermChangeDocumentNumber')\
                                                                ,col('incoTermChangeChangeUser'),col('incoTermChangeChangeIndicator'))
        
        
      otc_L1_TD_InternationalCommercialtermChangeLog =  objDataTransformation.gen_convertToCDMandCache \
         (otc_L1_TD_InternationalCommercialtermChangeLog,'otc','L1_TD_InternationalCommercialtermChangeLog',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "otc_L1_TD_InternationalCommercialtermChangeLog populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
