# Databricks notebook source
#fin.usp_L1_STG_Revenue_populate
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce,abs
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.functions as func
import sys
import traceback
def otc_L1_STG_Revenue_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    #logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L1_STG_Revenue
    startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
    defaultDate='1900-01-01'
    defaultChainCashType= 'No information available'
    v_surrogateKey = row_number().over(Window().orderBy(lit('')))
    v_bitTrue=True
    v_bitFalse=False
    
    if 'fin_L1_TD_Journal_ReferenceId' not in globals():
      fin_L1_TD_Journal_ReferenceId = spark.createDataFrame([], StructType([]))
      fin_L1_TD_Journal_ReferenceId = objDataTransformation.\
      gen_convertToCDMStructure_generate(dfSource=fin_L1_TD_Journal_ReferenceId\
                                              ,schemaName='fin',tableName='L1_TD_Journal_ReferenceId')[0]
   
    df_L1_TMP_RevenueAccount=fin_L1_MD_GLAccount.alias("glac1")\
       .join(fin_L1_STG_GLAccountMapping.alias('map')\
          ,((col('glac1.accountNumber') == col("map.accountNumber"))\
            & (col('map.isRevenue')==lit(1))  ),"inner")\
      .select(col('glac1.companyCode').alias('companyCode')\
         ,col('glac1.accountNumber').alias('accountNumber')\
             ).distinct()
    
    df_L1_TMP_Revenue=fin_L1_TD_Journal.alias("jou1")\
       .join(df_L1_TMP_RevenueAccount.alias('revacc')\
        ,((col('jou1.companyCode') == col("revacc.companyCode"))\
          & (col('jou1.accountNumber')==col('revacc.accountNumber'))  ),"inner")\
      .select(col('jou1.idGLJE').alias('idGLJE')\
        ,col('jou1.idGLJELine').alias('idGLJELine')\
        ,col('jou1.companyCode').alias('companyCode')\
        ,col('jou1.fiscalYear').alias('fiscalYear')\
        ,col('jou1.financialPeriod').alias('financialPeriod')\
        ,col('jou1.postingDate').alias('postingDate')\
        ,col('jou1.documentNumber').alias('glDocumentNumber')\
        ,col('jou1.lineItem').alias('glDocumentLineItem')\
        ,when(col('jou1.debitCreditIndicator')==lit('D'),(col('jou1.amountLC')*-1.0))\
              .otherwise(col('jou1.amountLC')).alias('revenueGLValueLC')\
        ,when(col('jou1.debitCreditIndicator')==lit('D'),(col('jou1.amountDC')*-1.0))\
              .otherwise(col('jou1.amountDC')).alias('revenueGLValueDC')\
        ,lit(None).alias('revenueGLValueRC')\
        ,col('jou1.localCurrency').alias('localCurrency')\
        ,col('jou1.documentCurrency').alias('documentCurrency')\
        ,col('jou1.accountNumber').alias('accountNumber')\
        ,when(col('jou1.referenceSubledger')==lit('Sales S/L'),lit(v_bitTrue))\
              .otherwise(lit(v_bitFalse)).alias('isBillingFromSD')\
        ,col('jou1.referenceSubledger').alias('referenceTransaction')\
        ,lit(v_bitFalse).alias('isSLBillingDocumentAmbigous')\
        ,lit(v_bitFalse).alias('isSLBillingDocumentMissing')\
        ,col('jou1.referenceSubledgerDocumentNumber').alias('referenceSubledgerDocumentNumber')\
        ,F.split(F.col("referenceSubledgerDocumentNumber"),"_").getItem(0).alias("billingDocumentNumber")
             ).distinct()
    
    df_L1_TMP_RevenueAmbiguousBilling=df_L1_TMP_Revenue.alias("trev1")\
        .groupby("trev1.companyCode","trev1.billingDocumentNumber")\
        .agg(func.countDistinct('trev1.idGLJE').alias('glDocumentNumberCount')\
          )\
        .filter(col('glDocumentNumberCount')>1)
    
    df_L1_TMP_RevenueAmbiguousBillingMinFiscalYr=df_L1_TMP_Revenue.alias("trev1")\
      .join(df_L1_TMP_RevenueAmbiguousBilling.alias("ambbill"),\
           ((col('trev1.billingDocumentNumber')==col("ambbill.billingDocumentNumber")) \
            & (col('trev1.companyCode')==col("ambbill.companyCode"))),"inner")\
      .groupby(col("trev1.companyCode"),col("trev1.billingDocumentNumber")\
               ,col("trev1.glDocumentNumber").alias("GLJEDocumentNumber"))\
      .agg(func.min("fiscalYear").alias("minFiscalYear"))\
    
    df_L1_TMP_Revenue_Extended=df_L1_TMP_Revenue.alias("revt1")\
      .join(fin_L1_TD_Journal_ReferenceId.alias("refid"),\
           ((col('revt1.companyCode').eqNullSafe(col("refid.companyCode"))) &\
            (col('revt1.billingDocumentNumber').eqNullSafe(col("refid.referenceID")))),"left")\
      .select(col('revt1.idGLJE').alias("idGLJE")\
             ,col('revt1.idGLJELine').alias("idGLJELine")\
             ,col('revt1.companyCode')\
             ,col('revt1.fiscalYear').alias("fiscalYear")\
             ,col('revt1.financialPeriod').alias("financialPeriod")\
             ,col('revt1.postingDate').alias("postingDate")\
             ,col('revt1.glDocumentNumber').alias("glDocumentNumber")\
             ,col('revt1.glDocumentLineItem').alias("glDocumentLineItem")\
             ,col('revt1.revenueGLValueLC').alias("revenueGLValueLC")\
             ,col('revt1.revenueGLValueDC').alias("revenueGLValueDC")\
             ,col('revt1.revenueGLValueRC').alias("revenueGLValueRC")\
             ,col('revt1.localCurrency').alias("localCurrency")\
             ,col('revt1.documentCurrency').alias("documentCurrency")\
             ,col('revt1.accountNumber').alias("accountNumber")\
             ,col('revt1.isBillingFromSD').alias("isBillingFromSD")\
             ,col('revt1.isSLBillingDocumentMissing').alias("isSLBillingDocumentMissing")\
             ,col('isSLBillingDocumentAmbigous')\
             ,col('revt1.referenceTransaction').alias("referenceTransaction")\
             ,col('revt1.billingDocumentNumber').alias("billingDocumentNumber")\
             ,col('revt1.referenceSubledgerDocumentNumber').alias("referenceSubledgerDocumentNumber")\
             ,coalesce(col('refid.referenceSubledgerDocumentNumber')\
                       ,col('revt1.billingDocumentNumber')).alias("billingDocumentNumber_ReferenceId")\
             ) 
     
    df_L1_TMP_Revenue_SLBillingAmbigousDocs=df_L1_TMP_Revenue_Extended.alias("revt1")\
      .join(df_L1_TMP_RevenueAmbiguousBilling.alias("ramb1"),\
         ((col('revt1.companyCode')==col("ramb1.companyCode")) &\
          (col('revt1.billingDocumentNumber')==col("ramb1.billingDocumentNumber"))),"inner")\
      .join(df_L1_TMP_RevenueAmbiguousBillingMinFiscalYr.alias("minfsc"),\
         ((col('revt1.billingDocumentNumber')==col('minfsc.billingDocumentNumber')) &\
          (col('revt1.companyCode')==col('minfsc.companyCode')) &\
          (col('revt1.glDocumentNumber')==col('minfsc.GLJEDocumentNumber'))),"inner")\
      .select(col('revt1.companyCode').alias("companyCode")\
           ,col('revt1.glDocumentNumber').alias("glDocumentNumber")\
           ,col('revt1.billingDocumentNumber').alias("billingDocumentNumber")\
           ,when((col('revt1.glDocumentNumber')==col('revt1.billingDocumentNumber_ReferenceId'))\
                 & ( col('revt1.fiscalYear')== col('minfsc.minFiscalYear'))\
                  ,lit(v_bitFalse)).otherwise(lit(v_bitTrue)).alias('isSLBillingDocumentAmbigous')\
          ,col('revt1.referenceSubledgerDocumentNumber')\
          ,col('revt1.fiscalYear')\
          ,col('minfsc.minFiscalYear')\
           )
    
    df_L1_TMP_Revenue_SLBillingAmbigousDocs=df_L1_TMP_Revenue_SLBillingAmbigousDocs\
        .filter(col('isSLBillingDocumentAmbigous')==v_bitTrue)
   
    df_L1_TMP_Revenue_Extended=df_L1_TMP_Revenue_Extended.alias("revt1")\
      .join(df_L1_TMP_Revenue_SLBillingAmbigousDocs.alias("ambdc"),\
         ((col('revt1.companyCode').eqNullSafe(col("ambdc.companyCode"))) \
          &(col('revt1.glDocumentNumber').eqNullSafe(col("ambdc.glDocumentNumber"))) \
          &(col('revt1.billingDocumentNumber').eqNullSafe(col("ambdc.billingDocumentNumber")))),"left")\
      .select(col('revt1.idGLJE').alias("idGLJE")\
         ,col('revt1.idGLJELine').alias("idGLJELine")\
         ,col('revt1.fiscalYear').alias("fiscalYear")\
         ,col('revt1.companyCode')\
         ,col('revt1.financialPeriod').alias("financialPeriod")\
         ,col('revt1.postingDate').alias("postingDate")\
         ,col('revt1.glDocumentNumber').alias("glDocumentNumber")\
         ,col('revt1.glDocumentLineItem').alias("glDocumentLineItem")\
         ,col('revt1.revenueGLValueLC').alias("revenueGLValueLC")\
         ,col('revt1.revenueGLValueDC').alias("revenueGLValueDC")\
         ,col('revt1.revenueGLValueRC').alias("revenueGLValueRC")\
         ,col('revt1.localCurrency').alias("localCurrency")\
         ,col('revt1.documentCurrency').alias("documentCurrency")\
         ,col('revt1.accountNumber').alias("accountNumber")\
         ,col('revt1.isBillingFromSD').alias("isBillingFromSD")\
         ,col('revt1.isSLBillingDocumentMissing').alias("isSLBillingDocumentMissing")\
         ,col('revt1.referenceTransaction').alias("referenceTransaction")\
         ,col('revt1.billingDocumentNumber').alias("billingDocumentNumber")\
         ,col('revt1.referenceSubledgerDocumentNumber').alias("referenceSubledgerDocumentNumber")\
         ,col('revt1.billingDocumentNumber_ReferenceId')\
  		 ,when(col('ambdc.billingDocumentNumber').isNull(),col('revt1.isSLBillingDocumentAmbigous'))\
            .otherwise(col('ambdc.isSLBillingDocumentAmbigous')).alias("isSLBillingDocumentAmbigous")\
         )
    
    df_L1_TMP_Revenue_SLBillingDocumentMissing=df_L1_TMP_Revenue_Extended.alias("rev")\
      .join(otc_L1_TD_Billing.alias("billing"),\
         (col('rev.billingDocumentNumber_ReferenceId')==col('billing.billingDocumentNumber')),"left")\
      .select(col('rev.billingDocumentNumber_ReferenceId').alias("billingDocumentNumber")\
         )\
      .filter((col('rev.isBillingFromSD')==v_bitTrue )\
         & col('billing.billingDocumentNumber').isNull()).distinct()
       
    df_L1_TMP_Revenue_Extended=df_L1_TMP_Revenue_Extended.alias("revt1")\
      .join(df_L1_TMP_Revenue_SLBillingDocumentMissing.alias("trdne1"),\
         (col('revt1.billingDocumentNumber_ReferenceId').eqNullSafe(col("trdne1.billingDocumentNumber"))),"left")\
      .select(col('revt1.idGLJE').alias("idGLJE")\
         ,col('revt1.idGLJELine').alias("idGLJELine")\
         ,col('revt1.companyCode')\
         ,col('revt1.fiscalYear').alias("fiscalYear")\
         ,col('revt1.financialPeriod').alias("financialPeriod")\
         ,col('revt1.postingDate').alias("postingDate")\
         ,col('revt1.glDocumentNumber').alias("glDocumentNumber")\
         ,col('revt1.glDocumentLineItem').alias("glDocumentLineItem")\
         ,col('revt1.revenueGLValueLC').alias("revenueGLValueLC")\
         ,col('revt1.revenueGLValueDC').alias("revenueGLValueDC")\
         ,col('revt1.revenueGLValueRC').alias("revenueGLValueRC")\
         ,col('revt1.localCurrency').alias("localCurrency")\
         ,col('revt1.documentCurrency').alias("documentCurrency")\
         ,col('revt1.accountNumber').alias("accountNumber")\
         ,col('revt1.isBillingFromSD').alias("isBillingFromSD")\
         ,col('revt1.referenceTransaction').alias("referenceTransaction")\
         ,col('revt1.billingDocumentNumber').alias("billingDocumentNumber")\
         ,col('revt1.referenceSubledgerDocumentNumber').alias("referenceSubledgerDocumentNumber")\
         ,col('revt1.billingDocumentNumber_ReferenceId')\
         ,col('revt1.isSLBillingDocumentAmbigous')\
         ,when(col('trdne1.billingDocumentNumber').isNull(),col('revt1.isSLBillingDocumentMissing'))\
            .otherwise(lit(v_bitTrue)).alias("isSLBillingDocumentMissing")\
         )
    
    fin_L1_STG_Revenue=df_L1_TMP_Revenue_Extended.alias("rev")\
      .select(
        col('rev.idGLJE').alias('idGLJE')\
        ,col('rev.idGLJELine').alias('idGLJELine')\
        ,col('rev.companyCode').alias('companyCode')\
        ,col('rev.fiscalYear').alias('fiscalYear')\
        ,col('rev.financialPeriod').alias('financialPeriod')\
        ,col('rev.postingDate').alias('postingDate')\
        ,col('rev.glDocumentNumber').alias('glDocumentNumber')\
        ,col('rev.glDocumentLineItem').alias('glDocumentLineItem')\
        ,col('rev.revenueGLValueLC').alias('revenueGLValueLC')\
        ,col('rev.revenueGLValueDC').alias('revenueGLValueDC')\
        ,col('rev.revenueGLValueRC').alias('revenueGLValueRC')\
        ,col('rev.localCurrency').alias('localCurrency')\
        ,col('rev.documentCurrency').alias('documentCurrency')\
        ,col('rev.accountNumber').alias('accountNumber')\
        ,col('rev.isBillingFromSD').alias('isBillingFromSD')\
        ,col('rev.isSLBillingDocumentAmbigous').alias('isSLBillingDocumentAmbigous')\
        ,col('rev.isSLBillingDocumentMissing').alias('isSLBillingDocumentMissing')\
        ,col('rev.billingDocumentNumber').alias('billingDocumentNumber')\
        )
    
    fin_L1_STG_Revenue = objDataTransformation\
          .gen_convertToCDMandCache(fin_L1_STG_Revenue,'fin','L1_STG_Revenue',False)
    
    fin_L1_STG_Revenue.display()
    executionStatus = "L1 STG Revenue populated sucessfully"
    print(executionStatus)
    #executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    #executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

