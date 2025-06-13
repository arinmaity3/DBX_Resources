# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when
from pyspark.sql.window import Window

def otc_L2_DIM_SalesSLToGLReconciliation_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        
        global otc_L2_DIM_SalesSLToGLReconciliation
        
        w = Window().orderBy(lit(''))
        
        otc_L2_DIM_SalesSLToGLReconciliation = otc_L1_STG_SLToGLReconciliationResult.alias('RES')\
          .join(knw_LK_GD_SalesSLToGLReconciliationInterpretation.alias('MDT')\
               ,(col('MDT.ResultInterpretationIdentifier')==(concat(lit(9),col('RES.PostedCategoryID')\
               ,col('RES.RevenueMatchCategoryID'),col('RES.SDFIExistenceCategoryID')\
               ,col('RES.DifferenceInterpretationID')))),how='inner')\
          .select(col('RES.billingDocumentNumber').alias('billingDocumentNumber')\
                 ,when(col('RES.revenuePostingDate').isNull(),lit('1900-01-01'))\
                       .otherwise(col('RES.revenuePostingDate')).alias('revenuePostingDate')\
                 ,when(col('RES.revenueFinancialPeriod').isNull(),lit(1))\
                       .otherwise(col('RES.revenueFinancialPeriod')).alias('revenueFinancialPeriod')\
                 ,when(col('RES.glDocumentNumber').isNull(),lit('#NA#'))\
                       .otherwise(col('RES.glDocumentNumber')).alias('glDocumentNumber')\
                 ,when(col('RES.companyCode').isNull(),lit('#NA#'))\
                       .otherwise(col('RES.companyCode')).alias('companyCode')\
                 ,when(col('RES.fiscalYear').isNull(),lit('#NA#'))\
                       .otherwise(col('RES.fiscalYear')).alias('fiscalYear')\
                 ,when(col('RES.localCurrency').isNull(),lit('#NA#'))\
                       .otherwise(col('RES.localCurrency')).alias('localCurrency')\
                 ,when(col('RES.AccountFootPrint').isNull(),lit('#NA#'))\
                       .otherwise(col('RES.AccountFootPrint')).alias('AccountFootPrint')\
                 ,col('RES.SLBillingDocumentMissingCategoryID').alias('SLBillingDocumentMissingCategoryID')\
                 ,col('RES.SDFIExistenceCategoryID').alias('SDFIExistenceCategoryID')\
                 ,col('MDT.SDFIExistenceCategory').alias('SDFIExistenceCategory')\
                 ,col('RES.PostedCategoryID').alias('PostedCategoryID')\
                 ,col('MDT.PostedCategory').alias('PostedCategory')\
                 ,col('RES.RevenueMatchCategoryID').alias('RevenueMatchCategoryID')\
                 ,col('MDT.RevenueMatchCategory').alias('RevenueMatchCategory')\
                 ,col('RES.DifferenceInterpretationID').alias('DifferenceInterpretationID')\
                 ,col('MDT.DifferenceInterpretationDescription').alias('DifferenceInterpretationDescription')\
                 ,col('MDT.ResultInterpretationDescription').alias('ResultInterpretationDescription')\
                 ,when(col('RES.SumRevenueGLValueLC').isNull(),lit(0))\
                      .otherwise(col('RES.SumRevenueGLValueLC')).alias('GLRevenueAmountLC')\
                 ,when(col('RES.SumRevenueGLValueRC').isNull(),lit(0))\
                      .otherwise(col('RES.SumRevenueGLValueRC')).alias('GLRevenueAmountRC')\
                 ,when(col('RES.sumSDBillingAmountLC').isNull(),lit(0))\
                      .otherwise(col('RES.sumSDBillingAmountLC')).alias('SLBillingAmountLC')\
                 ,when(col('RES.sumSDBillingAmountRC').isNull(),lit(0))\
                      .otherwise(col('RES.sumSDBillingAmountRC')).alias('SLBillingAmountRC')\
                 ,col('RES.combinedPopulationID').alias('combinedPopulationID'))
        
        otc_L2_DIM_SalesSLToGLReconciliation = otc_L2_DIM_SalesSLToGLReconciliation.\
                withColumn("salesSLToGLReconciliationSurrogateKey", row_number().over(w))
        
        otc_L2_DIM_SalesSLToGLReconciliation = objDataTransformation.gen_convertToCDMandCache\
              (otc_L2_DIM_SalesSLToGLReconciliation,'otc','L2_DIM_SalesSLToGLReconciliation',False)
        
        
        default_List =[[0,'NONE','1900-01-01','-1','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE']]
        default_df = spark.createDataFrame(default_List)
        
        v_AccountFootPrint = expr("LEFT(sl.AccountFootPrint,4000)")
        
        otc_vw_DIM_SalesSLToGLReconciliation = otc_L2_DIM_SalesSLToGLReconciliation.alias('sl')\
             .select(col('sl.salesSLToGLReconciliationSurrogateKey').alias('salesSLToGLReconciliationSurrogateKey')\
                 ,col('sl.billingDocumentNumber').alias('SLBillingDocumentNumber')\
                 ,col('sl.revenuePostingDate').alias('RevenuePostingDate')\
                 ,col('sl.revenueFinancialPeriod').alias('RevenueFinancialPeriod')\
                 ,col('sl.glDocumentNumber').alias('GLDocumentNumber')\
                 ,col('sl.companyCode').alias('GLCompanyCode')\
                 ,col('sl.fiscalYear').alias('GLFiscalYear')\
                 ,lit(v_AccountFootPrint).alias('AccountFootPrint')\
                 ,col('sl.SDFIExistenceCategory').alias('SDtoFIExistenceCategory')\
                 ,col('sl.PostedCategory').alias('PostedCategory')\
                 ,col('sl.RevenueMatchCategory').alias('RevenueMatchCategory')\
                 ,col('sl.DifferenceInterpretationDescription').alias('DifferenceInterpretationCategory')\
                 ,col('sl.ResultInterpretationDescription').alias('ResultInterpretation'))\
             .union(default_df)

        objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_SalesSLToGLReconciliation\
                    ,gl_CDMLayer2Path +"otc_vw_DIM_SalesSLToGLReconciliation.parquet") 
        
        executionStatus = "L2_DIM_SalesSLToGLReconciliation populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    

