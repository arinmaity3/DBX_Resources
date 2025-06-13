# Databricks notebook source
#fin.usp_L2_DIM_RevenueClearingAttribute_Populate
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce,abs
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
def fin_L2_DIM_RevenueClearingAttribute_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

    global fin_L2_DIM_RevenueClearingAttribute
    startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
    defaultDate='1900-01-01'
    defaultChainCashType= 'No information available'
    v_surrogateKey = row_number().over(Window().orderBy(lit('')))

    df_L2_TMP_RevenueClearingAttribute=gen_L1_STG_ClearingBox_BaseData.alias("arbase")\
         .join(gen_L1_STG_ClearingBox_BaseDataByChain.alias('bc')\
          ,(col("arbase.idGroupChain") == col("bc.idGroupChain")),"left")\
        .select(\
            coalesce(col('bc.isChainFromPY'),lit(0)).alias('isChainFromPY')\
           ,coalesce(col('bc.isChainFromSubsequent'),lit(0)).alias('isChainFromSubsequent')\
           ,lit(0).alias('isExistsARClearing')\
           ,col('arbase.isInterCompanyAccount').alias('isInterCompanyAccount')\
           ,col('arbase.isInterCompanyAccountChain').alias('isInterCompanyAccountChain')\
           ,when(col('arbase.EBFItem').isNull(),lit(0)).otherwise(lit(1)).alias('isLinkEBFItem')\
           ,when(col('arbase.EBFKey').isNull(),lit(0)).otherwise(lit(1)).alias('isLInkEBFKey')\
           ,col('arbase.isMisinterpretedChain').alias('isMisinterpretedChain')\
           ,coalesce(col('bc.isMissingJE'),lit(0)).alias('isMissingJE')\
           ,col('arbase.isNotExistsTradeAR').alias('isNotExistsTradeAR')\
           ,col('arbase.isOtherAP').alias('isOtherAP')\
           ,col('arbase.isOtherAR').alias('isOtherAR')\
           ,col('arbase.isRevenue').alias('isRevenue')\
           ,col('arbase.isRevenueOpen').alias('isRevenueOpen')\
           ,col('arbase.isReversedOrReversal').alias('isReversal')\
           ,col('arbase.isRevisedByRevenue').alias('isRevisedByRevenue')\
           ,col('arbase.isTradeAP').alias('isTradeAP')\
           ,col('arbase.isTradeAR').alias('isTradeAR')\
           ,col('arbase.isTradeAROpen').alias('isTradeAROpen')\
           ,col('arbase.isVAT').alias('isVAT')\
           ,lit(0).alias('heuristicDifferenceReason')\
           ,lit('').alias('GLSpecialIndicator')\
           ,coalesce(col('bc.chainCashType'),lit(defaultChainCashType)).alias('chainCashType')\
           ,col('arbase.isInTimeFrame').alias('isInTimeFrame')\
           ,col('arbase.isRevenueFullyReversed').alias('isRevenueFullyReversed')\
           ,lit(0).alias('isRatioInvalid')\
           ,col('arbase.isOrigin').alias('isOrigin')\
           ,col('arbase.isClearing').alias('isClearing')\
           ,col('arbase.isBankCash').alias('isBankCash')\
           ,lit(0).alias('caseID')\
           ,lit(0).alias('flagBatchInput')\
           ,col('arbase.EBFKey').alias('EBFKey')\
           ,col('arbase.EBFItem').alias('EBFItem')\
           ,col('arbase.clearingBoxReGrouping').alias('reGrouping')\
               ).distinct()

    fin_L2_DIM_RevenueClearingAttribute=df_L2_TMP_RevenueClearingAttribute.alias('stg')\
      .select(\
            col('stg.isChainFromPY').alias('isChainFromPY')\
            ,col('stg.isChainFromSubsequent').alias('isChainFromSubsequent')\
            ,coalesce(col('stg.isExistsARClearing'),lit(0)).alias('isExistsARClearing')\
            ,coalesce(col('stg.isInterCompanyAccount'),lit(0)).alias('isInterCompanyAccount')\
            ,coalesce(col('stg.isInterCompanyAccountChain'),lit(0)).alias('isInterCompanyAccountChain')\
            ,coalesce(col('stg.isLinkEBFItem'),lit(0)).alias('isLinkEBFItem')\
            ,coalesce(col('stg.isLInkEBFKey'),lit(0)).alias('isLInkEBFKey')\
            ,coalesce(col('stg.isMisinterpretedChain'),lit(0)).alias('isMisinterpretedChain')\
            ,coalesce(col('stg.isMissingJE'),lit(0)).alias('isMissingJE')\
            ,coalesce(col('stg.isNotExistsTradeAR'),lit(0)).alias('isNotExistsTradeAR')\
            ,coalesce(col('stg.isOtherAP'),lit(0)).alias('isOtherAP')\
            ,coalesce(col('stg.isOtherAR'),lit(0)).alias('isOtherAR')\
            ,coalesce(col('stg.isRevenue'),lit(0)).alias('isRevenue')\
            ,coalesce(col('stg.isRevenueOpen'),lit(0)).alias('isRevenueOpen')\
            ,coalesce(col('stg.isReversal'),lit(0)).alias('isReversal')\
            ,coalesce(col('stg.isRevisedByRevenue'),lit(0)).alias('isRevisedByRevenue')\
            ,coalesce(col('stg.isTradeAP'),lit(0)).alias('isTradeAP')\
            ,coalesce(col('stg.isTradeAR'),lit(0)).alias('isTradeAR')\
            ,coalesce(col('stg.isTradeAROpen'),lit(0)).alias('isTradeAROpen')\
            ,coalesce(col('stg.isVAT'),lit(0)).alias('isVAT')\
            ,coalesce(col('stg.heuristicDifferenceReason'),lit(0)).alias('heuristicDifferenceReason')\
            ,coalesce(col('stg.glSpecialIndicator'),lit('')).alias('glSpecialIndicator')\
            ,coalesce(col('stg.chainCashType'),lit(defaultChainCashType)).alias('chainCashType')\
            ,coalesce(col('stg.isInTimeFrame'),lit(0)).alias('isInTimeFrame')\
            ,coalesce(col('stg.isRevenueFullyReversed'),lit(0)).alias('isRevenueFullyReversed')\
            ,coalesce(col('stg.isRatioInvalid'),lit(0)).alias('isRatioInvalid')\
            ,coalesce(col('stg.isOrigin'),lit(0)).alias('isOrigin')\
            ,coalesce(col('stg.isClearing'),lit(0)).alias('isClearing')\
            ,coalesce(col('stg.isBankCash'),lit(0)).alias('isBankCash')\
            ,coalesce(col('stg.caseID'),lit(0)).alias('caseID')\
            ,coalesce(col('stg.flagBatchInput'),lit(0)).alias('flagBatchInput')\
            ,coalesce(col('stg.reGrouping'),lit(0)).alias('reGrouping')\
             ).distinct()\
        .withColumn("revenueClearingAttributesSurrogateKey",v_surrogateKey)   
    
    fin_L2_DIM_RevenueClearingAttribute = objDataTransformation\
        .gen_convertToCDMandCache(fin_L2_DIM_RevenueClearingAttribute,\
            'fin','L2_DIM_RevenueClearingAttribute',False,targetPath=gl_CDMLayer2Path)

    default_List =[[0,'NONE',0,0,'',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'']]
    default_df = spark.createDataFrame(default_List)

    otc_vw_DIM_RevenueClearingAttributes=fin_L2_DIM_RevenueClearingAttribute.alias('rca')\
      .select(col('rca.revenueClearingAttributesSurrogateKey').alias('revenueClearingAttributesSurrogateKey')\
             ,col('rca.chainCashType').alias('clearingTransactionCashType')\
             ,col('rca.isLInkEBFKey').alias('EBFLinkIndicator')\
             ,col('rca.isLinkEBFItem').alias('EBFMatchIndicator')\
             ,coalesce(col('rca.glSpecialIndicator'),lit('')).alias('GLSpecialIndicator')\
             ,col('rca.heuristicDifferenceReason').alias('heuristicDifferenceReason')\
             ,col('rca.isChainFromPY').alias('isChainFromPreviousYear')\
             ,col('rca.isChainFromSubsequent').alias('isChainFromSubsequent')\
             ,col('rca.isInterCompanyAccount').alias('isInterCompanyAccount')\
             ,col('rca.isInterCompanyAccountChain').alias('isIntercoClearingTransaction')\
             ,col('rca.isMisinterpretedChain').alias('isMisinterpretedChain')\
             ,col('rca.isMissingJE').alias('isMissingJE')\
             ,col('rca.isOtherAP').alias('isOtherAP')\
             ,col('rca.isOtherAR').alias('isOtherAR')\
             ,col('rca.isRevenue').alias('isRevenue')\
             ,col('rca.isRevenueOpen').alias('isRevenueOpen')\
             ,col('rca.isReversal').alias('isReversal')\
             ,col('rca.isRevisedByRevenue').alias('isRevisedByRevenue')\
             ,col('rca.isTradeAP').alias('isTradeAP')\
             ,col('rca.isTradeAR').alias('isTradeAR')\
             ,col('rca.isTradeAROpen').alias('isTradeAROpen')\
             ,col('rca.isVAT').alias('isVAT')\
             ,coalesce(col('rca.reGrouping'),lit('')).alias('reclassification')\
             ).union(default_df)

    objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_RevenueClearingAttributes\
                                          ,gl_CDMLayer2Path +"otc_vw_DIM_RevenueClearingAttributes.parquet")

    executionStatus = "L2_DIM_RevenueClearingAttribute populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
