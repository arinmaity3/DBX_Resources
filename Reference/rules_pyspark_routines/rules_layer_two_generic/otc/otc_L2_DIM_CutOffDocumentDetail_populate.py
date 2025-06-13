# Databricks notebook source
#[otc].[usp_L2_DIM_CutOffDocumentDetail_populate] 
from pyspark.sql.functions import row_number,expr,trim,col,lit
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
def otc_L2_DIM_CutOffDocumentDetail_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    #logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global otc_L2_DIM_CutOffDocumentDetail
    
    otc_L2_DIM_CutOffDocumentDetail =otc_L1_STG_CutOffDocumentDetail.alias("cutoffdoc")\
        .select(col('cutoffdoc.cutOffDocumentDetailSurrogateKey').alias('cutOffDocumentDetailSurrogateKey')\
            ,col('cutoffdoc.hasBranchReturn').alias('hasBranchReturn')\
            ,col('cutoffdoc.isBranchRevenueRelevant').alias('isBranchRevenueRelevant')\
            ,col('cutoffdoc.hasBranchInvoiceCancellation').alias('hasBranchInvoiceCancellation')\
            ,col('cutoffdoc.IsOrderRelatedBillingBranch').alias('IsOrderRelatedBillingBranch')\
            ,col('cutoffdoc.isDelivery').alias('isDelivery')\
            ,col('cutoffdoc.isDeliveryMissing').alias('isDeliveryMissing')\
            ,col('cutoffdoc.isDeliveryReturn').alias('isDeliveryReturn')\
            ,col('cutoffdoc.hasUnrecognizedRevenueAmount').alias('hasUnrecognizedRevenueAmount')\
            ,col('cutoffdoc.hasDocumentRevenue').alias('hasDocumentRevenue')\
            ,col('cutoffdoc.isActGoMovDateInvalid').alias('isActGoMovDateInvalid')\
            ,col('cutoffdoc.isActGoMovDateInvalidButCreationDateAfterEndDate')\
                .alias('isActGoMovDateInvalidButCreationDateAfterEndDate')\
            ,col('cutoffdoc.isActGoMovDateInvalidButDocumentDateAfterExtractionDate')\
                .alias('isActGoMovDateInvalidButDocumentDateAfterExtractionDate')\
            ,col('cutoffdoc.isActGoMovDateInvalidButBothAreValid').alias('isActGoMovDateInvalidButBothAreValid')\
            ,col('cutoffdoc.isInTimeFrameWithPriorAndSubsequentDetails')\
                .alias('isInTimeFrameWithPriorAndSubsequentDetails')\
               )
      
    otc_L2_DIM_CutOffDocumentDetail = objDataTransformation.gen_convertToCDMandCache(otc_L2_DIM_CutOffDocumentDetail,\
                                          'otc','L2_DIM_CutOffDocumentDetail',False)
    
    otc_vw_DIM_CutOffDocumentDetail=otc_L2_DIM_CutOffDocumentDetail.alias("cdd").select(\
        col('cdd.cutOffDocumentDetailSurrogateKey').alias('cutOffDocumentDetailSurrogateKey')\
        ,when(col('hasBranchReturn')==1,'Yes - Branch has Return')\
          .otherwise('No - Branch has no Return').alias('hasBranchReturn')\
        ,when(col('isBranchRevenueRelevant')==1,'Yes - Branch is Revenue relevant')\
          .otherwise('No - Branch is Revenue relevant').alias('isBranchRevenueRelevant')\
        ,when(col('hasBranchInvoiceCancellation')==1,'Yes - Branch has Invoice Cancellation')\
          .otherwise('No - Branch has no Invoice Cancellation').alias('hasBranchInvoiceCancellation')\
        ,when(col('isDelivery')==1,'Yes - Document is delivery')\
          .otherwise('No - Document is no Delivery').alias('isDelivery')\
        ,when(col('isDeliveryMissing')==1,'Yes - Delivery is mising')\
          .otherwise('No - Delivery is not mising').alias('isDeliveryMissing')\
        ,when(col('isDeliveryReturn')==1,'Yes - Delivery is return')\
          .otherwise('No - Delivery is no return').alias('isDeliveryReturn')\
        ,when(col('hasUnrecognizedRevenueAmount')==1,'Yes - Delivery has unrecognized Revenue')\
          .otherwise('No - Delivery has no unrecognized Revenue').alias('hasUnrecognizedRevenueAmount')\
        ,when(col('hasDocumentRevenue')==1,'Yes - Document has Revenue')\
          .otherwise('No - Document has no Revenue').alias('hasDocumentRevenue')\
        ,when(col('isActGoMovDateInvalid')==1,'Yes - ActGoodsMovement Date is invalid')\
          .otherwise('No - ActGoodsMovement Date is not invalid').alias('isActGoMovDateInvalid')\
        ,when(col('isActGoMovDateInvalidButCreationDateAfterEndDate')==1,\
                'Yes - ActGoodsMovement is invalid but ButCreationDateAfterEndDate')\
          .otherwise('No - Not ActGoodsMovement is invalid but ButCreationDateAfterEndDate')\
              .alias('isActGoMovDateInvalidButCreationDateAfterEndDate')\
        ,when(col('isActGoMovDateInvalidButDocumentDateAfterExtractionDate')==1,\
                'Yes - ActGoodsMovement is invalid But DocumentDateAfterExtractionDate')\
          .otherwise('No - Not ActGoodsMovement is invalid But DocumentDateAfterExtractionDate')\
              .alias('isActGoMovDateInvalidButDocumentDateAfterExtractionDate')\
        ,when(col('isActGoMovDateInvalidButBothAreValid')==1,\
                'Yes - isActGoMovDate is Invalid But Both Are Valid')\
          .otherwise('No - Not isActGoMovDate is Invalid But Both Are Valid')\
              .alias('isActGoMovDateInvalidButBothAreValid')\
        ,when(col('IsOrderRelatedBillingBranch')==1,\
                'Yes - Both Billing and Shipping transactions linked to Order transaction')\
          .otherwise('No - Both Billing and Shipping transactions linked to Order transaction')\
            .alias('IsOrderRelatedBillingBranch')\
        ,when(col('isInTimeFrameWithPriorAndSubsequentDetails')==1,'Prior period revenue')\
          .when(col('isInTimeFrameWithPriorAndSubsequentDetails')==2,'Revenue in analysis period')\
          .when(col('isInTimeFrameWithPriorAndSubsequentDetails')==2,'Subsequent period revenue')\
          .otherwise('NONE').alias('isInTimeFrameWithPriorAndSubsequentDetails')\
                                                                                       )
    default_List =[[0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'\
                    ,'NONE','NONE','NONE','NONE','NONE']]
    default_df = spark.createDataFrame(default_List)
    otc_vw_DIM_CutOffDocumentDetail = otc_vw_DIM_CutOffDocumentDetail.union(default_df)       
    
#     otc_vw_DIM_CutOffDocumentDetail = objDataTransformation\
#         .gen_convertToCDMStructure_generate(otc_vw_DIM_CutOffDocumentDetail\
#                                         ,'dwh','vw_DIM_BifurcationPatternSource',isIncludeAnalysisID = True)[0]
    objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_CutOffDocumentDetail\
                                        ,gl_CDMLayer2Path +"otc_vw_DIM_CutOffDocumentDetail.parquet")



    executionStatus = "L2_DIM_CutOffDocumentDetail populated sucessfully"
    #executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    #executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


