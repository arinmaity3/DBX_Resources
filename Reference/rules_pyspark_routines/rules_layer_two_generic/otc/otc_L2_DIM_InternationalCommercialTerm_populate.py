# Databricks notebook source
#[otc].[usp_L2_DIM_InternationalCommercialTerm_populate]
from pyspark.sql.functions import row_number,expr,trim,col,lit
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
def otc_L2_DIM_InternationalCommercialTerm_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    #logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global otc_L2_DIM_InternationalCommercialTerm
    
    v_surrogateKey = row_number().over(Window().orderBy(lit('')))
    
    df_L1_STG_40_SalesFlowTreeDocumentCore_tmp=otc_L1_STG_40_SalesFlowTreeDocumentCore.alias("slf40")\
       .join(knw_LK_CD_ReportingSetup.alias('lkrs')\
        ,(col("slf40.DocumentCompanyCode") == col("lkrs.companyCode")),"inner")\
      .select(when(col('slf40.shippingInternationalCommercialTerm').isNull(),lit('#NA#'))\
                .when(col('slf40.shippingInternationalCommercialTerm')=='',lit('#NA#'))\
                .otherwise(col('slf40.shippingInternationalCommercialTerm'))\
              .alias("shipping_salesDocumentIncoTerm")\
            ,when(col('slf40.shippingInternationalCommercialTermDetail').isNull(),lit('#NA#'))\
              .when(col('slf40.shippingInternationalCommercialTermDetail')=='',lit('#NA#'))\
              .otherwise(col('slf40.shippingInternationalCommercialTermDetail'))\
             .alias("shipping_salesDocumentIncoTermDetails")\
           ,when(col('slf40.isShippingInternationalCommercialTermStandard').isNull(),lit('#NA#'))\
              .when(col('slf40.isShippingInternationalCommercialTermStandard')=='',lit('#NA#'))\
              .otherwise(col('slf40.isShippingInternationalCommercialTermStandard'))\
             .alias("shipping_isSalesDocumentIncoTermStandard")\
            ,when(col('slf40.billingInternationalCommercialTerm').isNull(),lit('#NA#'))\
              .when(col('slf40.billingInternationalCommercialTerm')=='',lit('#NA#'))\
              .otherwise(col('slf40.billingInternationalCommercialTerm'))\
             .alias("billing_salesDocumentIncoTerm")\
            ,when(col('slf40.billingInternationalCommercialTermDetail').isNull(),lit('#NA#'))\
              .when(col('slf40.billingInternationalCommercialTermDetail')=='',lit('#NA#'))\
              .otherwise(col('slf40.billingInternationalCommercialTermDetail'))\
             .alias("billing_salesDocumentIncoTermDetails")\
           ,when(col('slf40.isBillingInternationalCommercialTermStandard').isNull(),lit('#NA#'))\
              .when(col('slf40.isBillingInternationalCommercialTermStandard')=='',lit('#NA#'))\
              .otherwise(col('slf40.isBillingInternationalCommercialTermStandard'))\
              .alias("billing_isSalesDocumentIncoTermStandard")\
            ,col('lkrs.clientCode')\
            ,col('lkrs.KPMGDataReportingLanguage')\
            ,col('slf40.shippingInternationalCommercialTerm')\
            ,col('slf40.billingInternationalCommercialTerm')\
           )

    v_shipping_salesDocumentIncoTermName=concat(col('shipping_salesDocumentIncoTerm')\
                                   ,lit(" "),lit('(')\
                                  ,when(col('ictmd1.description').isNull(),lit('#NA#'))\
                                   .when(col('ictmd1.description')=='',lit('#NA#'))\
                                   .otherwise(col('ictmd1.description'))\
                                   ,lit(')')
                                  )
    df_shipping_InternationalCommercialTerm=df_L1_STG_40_SalesFlowTreeDocumentCore_tmp.alias("slf40_lkrs")\
    .join(otc_L1_MD_InternationalCommercialterm.alias("ictmd1")\
         ,((col('slf40_lkrs.clientCode').eqNullSafe(col("ictmd1.clientCode")))\
           & (col('slf40_lkrs.KPMGDataReportingLanguage').eqNullSafe(col("ictmd1.languageCode")))\
           & (col('slf40_lkrs.shippingInternationalCommercialTerm').eqNullSafe(col("ictmd1.value")))\
          ),"left")\
    .select( col('shipping_salesDocumentIncoTerm').alias('salesDocumentIncoTerm')\
            ,lit(v_shipping_salesDocumentIncoTermName).alias('salesDocumentIncoTermName')\
            ,when(col('ictmd1.description').isNull(),lit('#NA#'))\
              .when(col('ictmd1.description')=='',lit('#NA#'))\
              .otherwise(col('ictmd1.description'))\
              .alias("salesDocumentIncoTermDescription")\
            ,col('shipping_salesDocumentIncoTermDetails').alias('salesDocumentIncoTermDetails')\
            ,col('shipping_isSalesDocumentIncoTermStandard').alias('isSalesDocumentIncoTermStandard')\
          ).distinct()
    
    v_billing_salesDocumentIncoTermName=concat(col('billing_salesDocumentIncoTerm')\
                                   ,lit(" "),lit('(')\
                                  ,when(col('ictmd1.description').isNull(),lit('#NA#'))\
                                   .when(col('ictmd1.description')=='',lit('#NA#'))\
                                   .otherwise(col('ictmd1.description'))\
                                   ,lit(')')
                                  )
    df_billing_InternationalCommercialTerm=df_L1_STG_40_SalesFlowTreeDocumentCore_tmp.alias("slf40_lkrs")\
      .join(otc_L1_MD_InternationalCommercialterm.alias("ictmd1")\
         ,((col('slf40_lkrs.clientCode').eqNullSafe(col("ictmd1.clientCode")))\
           & (col('slf40_lkrs.KPMGDataReportingLanguage').eqNullSafe(col("ictmd1.languageCode")))\
           & (col('slf40_lkrs.billingInternationalCommercialTerm').eqNullSafe(col("ictmd1.value")))\
          ),"left")\
    .select( col('billing_salesDocumentIncoTerm').alias('salesDocumentIncoTerm')\
            ,lit(v_billing_salesDocumentIncoTermName).alias('salesDocumentIncoTermName')\
            ,when(col('ictmd1.description').isNull(),lit('#NA#'))\
              .when(col('ictmd1.description')=='',lit('#NA#'))\
              .otherwise(col('ictmd1.description'))\
             .alias("salesDocumentIncoTermDescription")\
            ,col('billing_salesDocumentIncoTermDetails').alias('salesDocumentIncoTermDetails')\
            ,col('billing_isSalesDocumentIncoTermStandard').alias('isSalesDocumentIncoTermStandard')\
          ).distinct()
    
    otc_L2_DIM_InternationalCommercialTerm=df_shipping_InternationalCommercialTerm\
      .union(df_billing_InternationalCommercialTerm).distinct()\
      .withColumn("internationCommercialTermSurrogateKey",v_surrogateKey)   
    
    otc_L2_DIM_InternationalCommercialTerm = objDataTransformation\
                  .gen_convertToCDMandCache(otc_L2_DIM_InternationalCommercialTerm,\
                                          'otc','L2_DIM_InternationalCommercialTerm',False)
 
    default_List =[[0,'NONE','NONE','NONE','NONE','NONE']]
    default_df = spark.createDataFrame(default_List)
    
    otc_vw_DIM_InternationalCommercialTerm=otc_L2_DIM_InternationalCommercialTerm.\
        select(col('internationCommercialTermSurrogateKey').alias('internationCommercialTermSurrogateKey')\
               ,col('salesDocumentIncoTerm').alias('salesDocumentIncoTerm')\
               ,col('salesDocumentIncoTermDetails').alias('salesDocumentIncoTermDetails')\
               ,col('salesDocumentIncoTermName').alias('salesDocumentIncoTermName')\
               ,col('salesDocumentIncoTermDescription').alias('salesDocumentIncoTermDescription')\
               ,col('isSalesDocumentIncoTermStandard').alias('isSalesDocumentIncoTermStandard')\
               ).union(default_df)
    
#     otc_vw_DIM_InternationalCommercialTerm = objDataTransformation\
#         .gen_convertToCDMStructure_generate(otc_vw_DIM_InternationalCommercialTerm\
#                                         ,'otc','vw_DIM_InternationalCommercialTerm',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_InternationalCommercialTerm\
                                        ,gl_CDMLayer2Path +"otc_vw_DIM_InternationalCommercialTerm.parquet")

    executionStatus = "L2_DIM_InternationalCommercialTerm populated sucessfully"
    #executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    #executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
