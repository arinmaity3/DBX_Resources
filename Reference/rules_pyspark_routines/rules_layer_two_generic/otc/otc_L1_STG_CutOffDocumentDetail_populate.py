# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,coalesce

def otc_L1_STG_CutOffDocumentDetail_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

    global otc_L1_STG_RevenueSLCutOffData
    global otc_L1_STG_CutOffDocumentDetail
    
    CTE_DF = otc_L1_STG_RevenueSLCutOffData.alias("RSLCOD")\
       .filter(expr("RSLCOD.invoiceRank IN (0,1)"))\
       .selectExpr("RSLCOD.hasBranchReturn as hasBranchReturn"\
       ,"RSLCOD.isBranchRevenueRelevant as isBranchRevenueRelevant"\
       ,"RSLCOD.hasBranchInvoiceCancellation as hasBranchInvoiceCancellation"\
       ,"RSLCOD.isDelivery as isDelivery"\
       ,"RSLCOD.isDeliveryMissing as isDeliveryMissing"\
       ,"RSLCOD.isDeliveryReturn as isDeliveryReturn"\
       ,"RSLCOD.hasUnrecognizedRevenueAmount as hasUnrecognizedRevenueAmount"\
       ,"RSLCOD.hasDocumentRevenue as hasDocumentRevenue"\
       ,"RSLCOD.isActGoMovDateInvalid as isActGoMovDateInvalid"\
       ,"RSLCOD.isActGoMovDateInvalidButCreationDateAfterEndDate as isActGoMovDateInvalidButCreationDateAfterEndDate"\
       ,"RSLCOD.isActGoMovDateInvalidButDocumentDateAfterExtractionDate as isActGoMovDateInvalidButDocumentDateAfterExtractionDate"\
       ,"RSLCOD.isActGoMovDateInvalidButBothAreValid as isActGoMovDateInvalidButBothAreValid"\
       ,"RSLCOD.IsOrderRelatedBillingBranch as IsOrderRelatedBillingBranch"\
       ,"RSLCOD.invPeriod as isInTimeFrameWithPriorAndSubsequentDetails")\
       .distinct()
    
    otc_L1_STG_CutOffDocumentDetail = CTE_DF\
       .selectExpr("row_number() OVER (ORDER BY (SELECT NULL)) as cutOffDocumentDetailSurrogateKey"\
       ,"hasBranchReturn as hasBranchReturn"\
       ,"isBranchRevenueRelevant as isBranchRevenueRelevant"\
       ,"hasBranchInvoiceCancellation as hasBranchInvoiceCancellation"\
       ,"isDelivery as isDelivery"\
       ,"isDeliveryMissing as isDeliveryMissing"\
       ,"isDeliveryReturn as isDeliveryReturn"\
       ,"hasUnrecognizedRevenueAmount as hasUnrecognizedRevenueAmount"\
       ,"hasDocumentRevenue as hasDocumentRevenue"\
       ,"isActGoMovDateInvalid as isActGoMovDateInvalid"\
       ,"isActGoMovDateInvalidButCreationDateAfterEndDate as isActGoMovDateInvalidButCreationDateAfterEndDate"\
       ,"isActGoMovDateInvalidButDocumentDateAfterExtractionDate as isActGoMovDateInvalidButDocumentDateAfterExtractionDate"\
       ,"isActGoMovDateInvalidButBothAreValid as isActGoMovDateInvalidButBothAreValid"\
       ,"IsOrderRelatedBillingBranch as IsOrderRelatedBillingBranch"\
       ,"isInTimeFrameWithPriorAndSubsequentDetails as isInTimeFrameWithPriorAndSubsequentDetails")
    
    otc_L1_STG_RevenueSLCutOffData = otc_L1_STG_RevenueSLCutOffData.alias('RSLCOD')\
       .join(otc_L1_STG_CutOffDocumentDetail.alias('COD0'),\
               ((col('RSLCOD.hasBranchReturn').eqNullSafe(col('COD0.hasBranchReturn')))&
               (col('RSLCOD.hasBranchInvoiceCancellation').eqNullSafe(col('COD0.hasBranchInvoiceCancellation')))&
               (col('RSLCOD.isBranchRevenueRelevant').eqNullSafe(col('COD0.isBranchRevenueRelevant')))&
               (col('RSLCOD.isDelivery').eqNullSafe(col('COD0.isDelivery')))&
               (col('RSLCOD.isDeliveryMissing').eqNullSafe(col('COD0.isDeliveryMissing')))&
               (col('RSLCOD.isDeliveryReturn').eqNullSafe(col('COD0.isDeliveryReturn')))&
               (col('RSLCOD.hasDocumentRevenue').eqNullSafe(col('COD0.hasDocumentRevenue')))&
               (col('RSLCOD.hasUnrecognizedRevenueAmount').eqNullSafe(col('COD0.hasUnrecognizedRevenueAmount')))&
               (col('RSLCOD.isActGoMovDateInvalid').eqNullSafe(col('COD0.isActGoMovDateInvalid')))&
               (col('RSLCOD.isActGoMovDateInvalidButCreationDateAfterEndDate').eqNullSafe\
               (col('COD0.isActGoMovDateInvalidButCreationDateAfterEndDate')))&
               (col('RSLCOD.isActGoMovDateInvalidButDocumentDateAfterExtractionDate').eqNullSafe\
               (col('COD0.isActGoMovDateInvalidButDocumentDateAfterExtractionDate')))&
               (col('RSLCOD.isActGoMovDateInvalidButBothAreValid').eqNullSafe\
               (col('COD0.isActGoMovDateInvalidButBothAreValid')))&\
               (col('RSLCOD.IsOrderRelatedBillingBranch').eqNullSafe(col('COD0.IsOrderRelatedBillingBranch')))&\
               (col('RSLCOD.invPeriod').eqNullSafe(col('COD0.isInTimeFrameWithPriorAndSubsequentDetails')))),how='left')\
       .select(col('RSLCOD.*'),col('COD0.cutOffDocumentDetailSurrogateKey').alias('co_cutOffDocumentDetailSurrogateKey'))\
       .withColumn("colDerived",when(col('RSLCOD.cutOffDocumentDetailSurrogateKey').isNotNull()\
                                          ,col('co_cutOffDocumentDetailSurrogateKey'))\
    		  .otherwise(col('RSLCOD.cutOffDocumentDetailSurrogateKey')))
    
    otc_L1_STG_RevenueSLCutOffData = otc_L1_STG_RevenueSLCutOffData.drop(*['cutOffDocumentDetailSurrogateKey','co_cutOffDocumentDetailSurrogateKey'])
    otc_L1_STG_RevenueSLCutOffData = otc_L1_STG_RevenueSLCutOffData.withColumnRenamed("colDerived",'cutOffDocumentDetailSurrogateKey')
       
    otc_L1_STG_CutOffDocumentDetail  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_CutOffDocumentDetail,'otc','L1_STG_CutOffDocumentDetail',False)
    
    fileNamePath = gl_Layer2Staging  + "otc_L1_STG_RevenueSLCutOffData.delta"
    otc_L1_STG_RevenueSLCutOffData.write.format("delta").mode("overwrite").save(fileNamePath)

    executionStatus = "otc_L1_STG_CutOffDocumentDetail populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

