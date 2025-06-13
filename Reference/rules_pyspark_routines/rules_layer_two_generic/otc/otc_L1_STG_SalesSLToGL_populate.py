# Databricks notebook source
#[otc].[usp_L1_STG_SalesSLToGL_populate]
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce,abs
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
def otc_L1_STG_SalesSLToGL_populate():
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    #logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global otc_L1_STG_ARSLToGL
    global otc_L1_STG_SalesSLToGL
    global otc_L1_STG_RevenueSLToGL
    
    if 'fin_L1_TD_Journal_ReferenceId' not in globals():
      fin_L1_TD_Journal_ReferenceId = spark.createDataFrame([], StructType([]))
      fin_L1_TD_Journal_ReferenceId = objDataTransformation.\
      gen_convertToCDMStructure_generate(dfSource=fin_L1_TD_Journal_ReferenceId\
                                              ,schemaName='fin',tableName='L1_TD_Journal_ReferenceId')[0]
    
    df_L1_TMP_DistinctJEItem=fin_L1_STG_JELineItem.alias("jeline")\
      .select(col('jeline.companyCode'),col('jeline.financialYear')\
              ,col('jeline.GLJEDocumentNumber'),col('jeline.idGLJE')\
             ).distinct()
   
    #Populating L1_TMP_BillingSDToRevenue    
    df_L1_TMP_RevenueSLToGL_Billing_grp=otc_L1_TD_Billing.alias("bil")\
      .groupBy("bil.billingDocumentNumber")\
      .agg(sum("bil.billingDocumentAmountDC").alias("billingDocumentAmountDC")\
        ,sum(abs(col("bil.billingDocumentAmountDC"))).alias("absBillingDocumentAmountDC")\
        ,sum("bil.billingDocumentQuantitySKU").alias("billingDocumentQuantitySKU")
        ,count(col('bil.billingDocumentNumber')).alias("countRows")
        )  
    df_L1_TMP_BillingSDToRevenue=otc_L1_TD_Billing.alias("bil")\
      .join(df_L1_TMP_RevenueSLToGL_Billing_grp.alias("grp")\
        ,(col('bil.billingDocumentNumber') == col("grp.billingDocumentNumber")\
         ),"inner")\
      .select(col('bil.billingDocumentNumber')\
         ,col('bil.billingDocumentLineItem')\
         ,col('bil.billingDocumentAmountDC')\
         ,col('bil.billingDocumentAmountLC')\
         ,lit(None).alias('billingDocumentAmountRC')\
         ,col('bil.billingDocumentQuantitySU')\
         ,when(col('grp.absBillingDocumentAmountDC')!=lit(0),\
               (abs(col('bil.billingDocumentAmountDC'))/col('grp.absBillingDocumentAmountDC')))\
              .otherwise(lit(None)).alias('amountSplitFactorBilling')\
         ,when(col('grp.billingDocumentQuantitySKU')!=0,\
           (abs(col('bil.billingDocumentQuantitySKU'))/col('grp.billingDocumentQuantitySKU')))\
            .otherwise(lit(None)).alias('quantitySplitFactorBilling')\
         ,lit(lit(1.0)/col('grp.countRows')).alias('countSplitFactorBilling')\
         ,col('grp.billingDocumentAmountDC').alias('billingDocumentAmountValueMatch')\
      )
    df_L1_TMP_RevenueSLToGL_Revenue_grp=fin_L1_STG_Revenue.alias("rev")\
      .groupBy("rev.idGLJE")\
      .agg(sum("rev.revenueGLValueLC").alias("revenueGLValueLC")\
          ,sum(abs(col("rev.revenueGLValueLC"))).alias("absRevenueGLValueLC")\
          ,count(col('*')).alias("countRows")\
          )
    
    df_L1_TMP_SalesSLToGL_AR_grp=fin_L1_STG_Receivable.alias("ar")\
      .join(df_L1_TMP_DistinctJEItem.alias("je")\
        ,((col('ar.companyCode') == col("je.companyCode"))\
          &(col('ar.fiscalYear') == col("je.financialYear"))\
          &(col('ar.GLJEDocumentNumber') == col("je.GLJEDocumentNumber"))\
         ),"inner")\
      .groupBy("ar.companyCode","ar.fiscalYear","ar.GLJEDocumentNumber","je.idGLJE")\
      .agg(sum("ar.amountLC").alias("ARGLValueLC")\
          ,sum(abs(col("ar.amountLC"))).alias("absARGLValueLC")\
          ,count(col('*')).alias("countRows")\
        )    
    df_L1_TMP_SDToAR=fin_L1_STG_Receivable.alias("ar")\
      .join(df_L1_TMP_SalesSLToGL_AR_grp.alias("grp")\
        ,((col('ar.companyCode') == col("grp.companyCode"))\
          &(col('ar.fiscalYear') == col("grp.fiscalYear"))\
          &(col('ar.GLJEDocumentNumber') == col("grp.GLJEDocumentNumber"))\
         ),"inner")\
      .select(col('ar.billingDocumentNumber')\
         ,col('grp.idGLJE')\
         ,lit(None).alias('idGLJELine')
         ,col('ar.amountLC')\
         ,col('ar.amountDC')\
         ,when(col('grp.absARGLValueLC')!=lit(0),\
               (col('ar.amountLC')/col('grp.absARGLValueLC')))\
              .otherwise(lit(None)).alias('GLValueLCSplitFactorAR')\
         ,lit(lit(1.0)/col('grp.countRows')).alias('countSplitFactorAR')\
         ,col('grp.ARGLValueLC').alias('ARAmountValueMatch')\
         ,col('ar.isSLBillingDocumentAmbiguous').alias('isSLBillingDocumentAmbiguous')\
        )  
    df_L1_TMP_SDToAR_1=df_L1_TMP_SDToAR.alias("ar1")\
      .join(fin_L1_STG_Revenue.select(col('idGLJE'),col('isSLBillingDocumentAmbigous'))\
            .distinct().alias("rev1"),\
         (col('ar1.idGLJE').eqNullSafe(col("rev1.idGLJE"))),"left")\
      .select(col('ar1.billingDocumentNumber').alias("billingDocumentNumber")\
         ,col('ar1.idGLJE').alias("idGLJE")\
         ,col('ar1.idGLJELine').alias('idGLJELine')
         ,col('ar1.amountLC').alias("amountLC")\
         ,col('ar1.amountDC').alias("amountDC")\
         ,col('ar1.GLValueLCSplitFactorAR').alias("GLValueLCSplitFactorAR")\
         ,col('ar1.countSplitFactorAR').alias("countSplitFactorAR")\
         ,col('ar1.ARAmountValueMatch').alias("ARAmountValueMatch")\
         ,when(col('rev1.idGLJE').isNull(),col('ar1.isSLBillingDocumentAmbiguous'))\
            .otherwise(col('rev1.isSLBillingDocumentAmbigous')).alias("isSLBillingDocumentAmbiguous")\
         )
    df_L1_TMP_SDToRevenue=fin_L1_STG_Revenue.alias("rev")\
      .join(df_L1_TMP_RevenueSLToGL_Revenue_grp.alias("grp")\
        ,(col('rev.idGLJE') == col("grp.idGLJE")),"inner")\
      .join(fin_L1_TD_Journal_ReferenceId.alias("jr")\
        ,((col('rev.companyCode').eqNullSafe(col("jr.CompanyCode")))\
          &(col('rev.glDocumentNumber').eqNullSafe(col("jr.documentNumber")))\
          &(col('rev.glDocumentLineItem').eqNullSafe(col("jr.lineItem")))\
          &(col('rev.billingDocumentNumber').eqNullSafe(col("jr.referenceID")))\
         ),"left")\
      .select(coalesce(col('jr.referenceSubledgerDocumentNumber'),col("rev.billingDocumentNumber"))\
            .alias('billingDocumentNumber')
         ,col('rev.idGLJE')\
         ,col('rev.idGLJELine')\
         ,col('rev.revenueGLValueLC')\
         ,col('rev.revenueGLValueDC')\
         ,col('rev.revenueGLValueRC')\
         ,when(col('grp.absRevenueGLValueLC')!=lit(0),\
               (abs(col('rev.revenueGLValueLC'))/col('grp.absRevenueGLValueLC')))\
              .otherwise(lit(None)).alias('GLValueLCSplitFactorRevenue')\
         ,lit(lit(1.0)/col('grp.countRows')).alias('countSplitFactorRevenue')\
         ,col('grp.revenueGLValueLC').alias('revenueAmountValueMatch')\
         ,col('rev.isSLBillingDocumentAmbigous')\
        ).distinct()  
    
    v_billingValueSplitFactor=when(col('jr.referenceSubledgerDocumentNumber').isNull()\
          ,coalesce(col('bil1.amountSplitFactorBilling'),col('bil1.countSplitFactorBilling'), lit(1.0)))\
        .when((col('bil1.billingDocumentAmountLC')!=0) & (col('rev1.revenueGLValueLC')!=0)\
         ,(col('bil1.billingDocumentAmountLC') * \
           coalesce(col('rev1.GLValueLCSplitFactorRevenue'),col('rev1.countSplitFactorRevenue'), lit(1.0))\
           /col('rev1.revenueGLValueLC')))\
       .otherwise(0)\
    
    df_L1_STG_RevenueSLToGL=df_L1_TMP_BillingSDToRevenue.alias("bil1")\
      .join(df_L1_TMP_SDToRevenue.alias("rev1"),\
           (col('bil1.billingDocumentNumber').eqNullSafe(col('rev1.billingDocumentNumber')))\
           &(col('rev1.isSLBillingDocumentAmbigous')==0),"left")\
      .join(fin_L1_TD_Journal_ReferenceId.alias("jr")\
        ,(col('rev1.billingDocumentNumber').eqNullSafe(col('jr.referenceSubledgerDocumentNumber'))),"left")\
      .select(col('bil1.billingDocumentNumber')\
         ,col('bil1.billingDocumentLineItem')\
         ,col('rev1.idGLJE')\
         ,col('rev1.idGLJELine')\
         ,col('bil1.billingDocumentAmountLC').alias('billingValueLC')\
         ,col('bil1.billingDocumentAmountRC').alias('billingValueRC')\
         ,col('bil1.billingDocumentAmountDC').alias('billingValueDC')\
         ,coalesce(col('rev1.revenueGLValueLC'),lit(0)).alias('revenueGLValueLC')\
         ,col('rev1.revenueGLValueRC')\
         ,col('rev1.revenueGLValueDC')\
         ,col('bil1.billingDocumentQuantitySU')\
         ,lit(v_billingValueSplitFactor).alias("billingValueSplitFactor")\
         ,coalesce(col('rev1.GLValueLCSplitFactorRevenue'),col('rev1.countSplitFactorRevenue'), lit(1.0))\
          .alias("revenueGLSplitFactor")\
         ,col('bil1.billingDocumentAmountValueMatch')\
         ,col('rev1.revenueAmountValueMatch')\
             )
    otc_L1_STG_RevenueSLToGL=df_L1_STG_RevenueSLToGL.alias("bil1")\
      .select(col('bil1.billingDocumentNumber')\
         ,col('bil1.billingDocumentLineItem')\
         ,col('bil1.idGLJE')\
         ,col('bil1.idGLJELine')\
         ,lit(col('bil1.billingValueLC')*col('revenueGLSplitFactor')).alias('billingValueLC')\
         ,lit(col('bil1.billingValueRC')*col('revenueGLSplitFactor')).alias('billingValueRC')\
         ,lit(col('bil1.billingValueDC')*col('revenueGLSplitFactor')).alias('billingValueDC')\
         ,col('billingDocumentQuantitySU')
         ,lit(col('bil1.revenueGLValueLC')*col('billingValueSplitFactor')).alias('revenueGLValueLC')\
         ,lit(col('bil1.revenueGLValueRC')*col('billingValueSplitFactor')).alias('revenueGLValueRC')\
         ,lit(col('bil1.revenueGLValueDC')*col('billingValueSplitFactor')).alias('revenueGLValueDC')\
         ,when(col('billingDocumentAmountValueMatch')==col('revenueAmountValueMatch'),1)\
              .otherwise(0).alias('isValueMatch')\
         ,col('bil1.billingValueSplitFactor')\
         ,col('bil1.revenueGLSplitFactor')\
             )
    otc_L1_STG_RevenueSLToGL = objDataTransformation\
      .gen_convertToCDMandCache(otc_L1_STG_RevenueSLToGL,'otc','L1_STG_RevenueSLToGL',False)

    #Populate otc_L1_STG_ARSLToGL
    df_L1_STG_ARSLToGL=df_L1_TMP_BillingSDToRevenue.alias("bil1")\
      .join(df_L1_TMP_SDToAR_1.alias("ar1"),\
           (col('bil1.billingDocumentNumber').eqNullSafe(col('ar1.billingDocumentNumber')))\
           &(col('ar1.isSLBillingDocumentAmbiguous')==0),"left")\
      .select(col('bil1.billingDocumentNumber')\
         ,col('bil1.billingDocumentLineItem')\
         ,col('ar1.idGLJE')\
         ,col('ar1.idGLJELine')\
         ,col('bil1.billingDocumentAmountLC').alias('billingValueLC')\
         ,col('bil1.billingDocumentAmountRC').alias('billingValueRC')\
         ,col('bil1.billingDocumentAmountDC').alias('billingValueDC')\
         ,col('ar1.amountLC').alias('revenueGLValueLC')\
         ,col('ar1.amountDC').alias('revenueGLValueDC')\
         ,col('bil1.billingDocumentQuantitySU')\
         ,coalesce(col('bil1.amountSplitFactorBilling'),col('bil1.countSplitFactorBilling'), lit(1.0))\
            .alias("billingValueSplitFactor")\
         ,coalesce(col('ar1.GLValueLCSplitFactorAR'),col('ar1.countSplitFactorAR'), lit(1.0))\
            .alias("revenueGLSplitFactor")\
         ,col('bil1.billingDocumentAmountValueMatch')\
         ,col('ar1.ARAmountValueMatch').alias('revenueAmountValueMatch')\
             )
    otc_L1_STG_ARSLToGL=df_L1_STG_ARSLToGL.alias("bil1")\
      .select(col('bil1.billingDocumentNumber')\
           ,col('bil1.billingDocumentLineItem')\
           ,col('bil1.idGLJE')\
           ,col('bil1.idGLJELine')\
           ,lit(col('bil1.billingValueLC')*col('revenueGLSplitFactor')).alias('billingValueLC')\
           ,lit(col('bil1.billingValueRC')*col('revenueGLSplitFactor')).alias('billingValueRC')\
           ,lit(col('bil1.billingValueDC')*col('revenueGLSplitFactor')).alias('billingValueDC')\
           ,col('billingDocumentQuantitySU')
           ,lit(col('bil1.revenueGLValueLC')*col('billingValueSplitFactor')).alias('revenueGLValueLC')\
           ,lit(col('bil1.revenueGLValueDC')*col('billingValueSplitFactor')).alias('revenueGLValueDC')\
           ,when(col('bil1.billingDocumentAmountValueMatch')==col('bil1.revenueAmountValueMatch'),1)\
                .otherwise(0).alias('isValueMatch')\
           ,col('bil1.billingValueSplitFactor')\
           ,col('bil1.revenueGLSplitFactor')\
               )
    otc_L1_STG_ARSLToGL = objDataTransformation\
      .gen_convertToCDMandCache(otc_L1_STG_ARSLToGL,'otc','L1_STG_ARSLToGL',False)
    
    #Populate otc_L1_STG_SalesSLToGL
    df_RevenueSLToGL= otc_L1_STG_RevenueSLToGL.alias("rev")\
      .select(col("rev.idGLJE"),lit(0).alias("isSLBillingDocumentMissing"))\
      .filter(coalesce(col('rev.billingDocumentNumber'),lit('')) !=lit(''))\
      .distinct()   
    df_ARSLToGL= otc_L1_STG_ARSLToGL.alias("ar")\
      .select(col("ar.idGLJE"),lit(0).alias("isSLBillingDocumentMissing"))\
      .filter(coalesce(col('ar.billingDocumentNumber'),lit('')) !=lit(''))\
      .distinct()   
    df_Receivable= fin_L1_STG_Receivable.alias("rec")\
      .join(df_L1_TMP_DistinctJEItem.alias("je"),((col('rec.companyCode')==col('je.companyCode'))\
                                                 & (col('rec.fiscalYear')==col('je.financialYear'))\
                                                 & (col('rec.GLJEDocumentNumber')==col('je.GLJEDocumentNumber'))\
                                                 ),"inner")\
      .select(col("je.idGLJE").alias('idGLJE')\
              ,col('rec.isSLBillingDocumentMissing').alias("isSLBillingDocumentMissing"))\
      .filter(col('rec.isSLBillingDocumentMissing')==1)\
      .distinct()
    df_Revenue= fin_L1_STG_Revenue.alias("revenue")\
      .select(col("revenue.idGLJE"),col('revenue.isSLBillingDocumentMissing'))\
      .filter(coalesce(col('revenue.isSLBillingDocumentMissing'),lit('')) ==1)\
      .distinct()
    
    otc_L1_STG_SalesSLToGL=df_ARSLToGL.union(df_ARSLToGL)\
          .union(df_Receivable).union(df_Revenue)\
          .distinct()
  
    otc_L1_STG_SalesSLToGL = objDataTransformation\
      .gen_convertToCDMandCache(otc_L1_STG_SalesSLToGL,'otc','L1_STG_SalesSLToGL',False)      
    
    executionStatus = "L1 STG Sales SL To GL populated sucessfully"
    print(executionStatus)
    #executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    #executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

