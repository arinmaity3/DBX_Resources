# Databricks notebook source
#otc_L2_DIM_SalesTransactionDetail_populate
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when,sum,avg,max,concat,coalesce,abs
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType    
from datetime import datetime
from pyspark.sql.functions import *
def otc_L2_DIM_SalesTransactionDetail_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

    global otc_L2_DIM_SalesTransactionDetail
    v_isDeliveryReturn=when(col('SFTB.interpretedDocumentTypeClientID') == lit(28),lit(1)).otherwise(lit(0))

    df_L2_TMP_SalesTransactionDetail_v1=otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
        .join(otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('SFTC')\
           ,(col("SFTB.childID")==col("SFTC.childID")),how="inner")\
        .join(otc_L1_STG_40_InspectTreeTransactionDetail.alias('tdtl')\
           ,((col("SFTB.salesOrderSubTransactionID").eqNullSafe(col("tdtl.orderSubTransactionID")))\
             & (col("SFTB.salesTransactionID").eqNullSafe(col("tdtl.salesTransactionID")))\
               ),how="left")\
         .groupBy('SFTB.salesorderSubTransactionID','SFTB.salesTransactionID'\
                  ,'SFTB.salesparentTransactionID','SFTC.isTransactionArtificial'\
                   , 'SFTC.isParentTransactionArtificial','SFTC.isTransactionService'\
                    ,'tdtl.orderingDocumentTiming','tdtl.billingDocumentType'\
                  )\
         .agg(coalesce(max('SFTC.DocumentCompanyCode'),lit(None)).alias("salesDocumentCompanyCode")\
              ,sum(lit(v_isDeliveryReturn)).alias("tmp_isDeliveryReturn")\
              )\
         .select(col('salesOrderSubTransactionID').alias('salesOrderSubTransactionID')\
                 ,col('salesTransactionID').alias('salesTransactionID')\
                 ,col('salesParentTransactionID').alias('salesParentTransactionID')\
                 ,coalesce(col('billingDocumentType'),lit('NONE')).alias('billingDocumentType')\
                 ,coalesce(col('orderingDocumentTiming'),lit('NONE')).alias('salesOrderDocumentTiming')\
                 ,coalesce(col('salesDocumentCompanyCode'),lit('NONE')).alias('salesDocumentCompanyCode')\
                 ,col('isParentTransactionArtificial').alias('isParentTransactionArtificial')\
                 ,col('isTransactionArtificial').alias('isTransactionArtificial')\
                 ,col('isTransactionService').alias('isTransactionService')\
                 ,when(col('tmp_isDeliveryReturn')>0,lit(1)).otherwise(lit(0)).alias('isDeliveryReturn')\
                 )
    #df_L2_TMP_SalesTransactionDetail_v1.display()
    v_salesTransactionID=\
          when((col('RECO.SDFIExistenceCategoryID')==lit(3)) & (col('RECO.SLBillingDocumentMissingCategoryID')==lit(2))\
                               ,lit(-1))\
          .when((col('RECO.SDFIExistenceCategoryID')!=lit(3)) & (col('RECO.SLBillingDocumentMissingCategoryID').isin(5,6))\
                               ,lit(-2))\
          .otherwise(lit(0))

    df_L2_TMP_SalesTransactionDetail_v2=otc_L1_STG_SLToGLReconciliationResult.alias('RECO')\
        .join(otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
           ,(col("SFTB.salesDocumentNumber").eqNullSafe(col("RECO.billingDocumentNumber"))),how="left")\
        .filter(col('SFTB.salesDocumentNumber').isNull())\
        .select(lit(0).alias('salesOrderSubTransactionID')\
               ,lit(v_salesTransactionID).alias("salesTransactionID")\
               ,lit('NONE').alias("billingDocumentType")\
               ,lit('NONE').alias("salesOrderDocumentTiming")\
               ,coalesce(col('RECO.companyCode'),lit('None')).alias('salesDocumentCompanyCode')\
               ,lit(1).alias("isTransactionArtificial")\
               ,lit(0).alias("isTransactionService")\
               ,lit(0).alias("isDeliveryReturn")\
               )\

    df_L2_TMP_SalesTransactionDetail_v2=df_L2_TMP_SalesTransactionDetail_v2.alias("a")\
        .select(col('a.salesOrderSubTransactionID')\
                ,col('salesTransactionID')\
                ,col('salesTransactionID').alias('salesParentTransactionID')\
                ,col('billingDocumentType')\
                ,col('salesOrderDocumentTiming')\
                ,col('salesDocumentCompanyCode')\
                ,col('salesTransactionID').alias('sParentTransactionArtificial')\
                ,col('isTransactionArtificial')\
                ,col('isTransactionService')\
                ,col('isDeliveryReturn')\
               ).distinct()

    v_salesTransactionID = -1000+ row_number().over(Window().orderBy(col('orgu.companyCode')))

    df_L2_TMP_SalesTransactionDetail_v3=gen_L2_DIM_Organization.alias("orgu")\
        .select(lit(0).alias("salesOrderSubTransactionID")\
               ,lit(v_salesTransactionID).alias("salesTransactionID")\
               ,lit(0).alias("salesParentTransactionID")\
               ,lit('None').alias("billingDocumentType")\
               ,lit('None').alias("orderingDocumentTiming")\
               ,col('orgu.companyCode').alias("companyCode")\
               ,lit(1).alias("isParentTransactionArtificial")\
               ,lit(1).alias("isTransactionArtificial")\
               ,lit(1).alias("isTransactionService")\
               ,lit(0).alias("isDeliveryReturn")\
               )

    df_L2_TMP_SalesTransactionDetail=df_L2_TMP_SalesTransactionDetail_v1\
        .union(df_L2_TMP_SalesTransactionDetail_v2)\
        .union(df_L2_TMP_SalesTransactionDetail_v3)

    v_surrogateKey = row_number().over(Window().orderBy(lit('')))

    otc_L2_DIM_SalesTransactionDetail=df_L2_TMP_SalesTransactionDetail.alias("TD")\
        .select(lit(v_surrogateKey).alias("transactionDetailSurrogateKey")\
              ,col('TD.salesOrderSubTransactionID').alias("salesOrderSubTransactionID")\
              ,col('TD.salesTransactionID').alias("salesTransactionID")\
              ,col('TD.salesParentTransactionID').alias("salesParentTransactionID")\
              ,col('TD.billingDocumentType').alias("billingDocumentType")\
              ,col('TD.salesOrderDocumentTiming').alias("orderingDocumentTiming")\
              ,col('TD.salesDocumentCompanyCode').alias("companyCode")\
              ,col('TD.isParentTransactionArtificial').alias("isParentTransactionArtificial")\
              ,col('TD.isTransactionArtificial').alias("isTransactionArtificial")\
              ,col('TD.isTransactionService').alias("isTransactionService")\
              ,col('TD.isDeliveryReturn').alias("isDeliveryReturn")\
               )

    otc_L2_DIM_SalesTransactionDetail  = objDataTransformation.gen_convertToCDMandCache\
          (otc_L2_DIM_SalesTransactionDetail,'otc','L2_DIM_SalesTransactionDetail',False)    
    
    [otc].[vw_DIM_SalesTransactionDetail]

    languageCode = knw_LK_CD_ReportingSetup.select(col("KPMGDataReportingLanguage")).collect()[0][0]
    erpGENSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC'))

    v_isSoDConflict=\
          when(col('LTOSL.parentTransactionID').isNotNull() ,lit('Contains SoD Conflict - Yes'))\
         .otherwise(lit('Does not contain SoD Conflict - No'))

    otc_L2_DIM_SalesTransactionDetail
    df_vw_DIM_SalesTransactionDetail = otc_L2_DIM_SalesTransactionDetail.alias('STD')\
        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('trat'),(col('trat.targetLanguageCode') == lit(languageCode)) \
              & (col('trat.sourceERPSystemID') == lit(erpGENSystemID))\
              & (col('trat.businessDatatype') == lit('Is Transaction Artificial')) \
              & (col('trat.sourceSystemValue') == col('STD.isTransactionArtificial')) ,how =('left')) \
        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('ptat'),(col('ptat.targetLanguageCode') == lit(languageCode)) \
              & (col('ptat.sourceERPSystemID') == lit(erpGENSystemID))\
              & (col('ptat.businessDatatype') == lit('Is Parent Transaction Artificial')) \
              & (col('ptat.sourceSystemValue') == col('STD.isParentTransactionArtificial')) ,how =('left')) \
        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('trsv'),(col('trsv.targetLanguageCode') == lit(languageCode)) \
              & (col('trsv.sourceERPSystemID') == lit(erpGENSystemID))\
              & (col('trsv.businessDatatype') == lit('Is Transaction Service')) \
              & (col('trsv.sourceSystemValue') == col('STD.isTransactionService')) ,how =('left')) \
        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('idrt'),(col('idrt.targetLanguageCode') == lit(languageCode)) \
              & (col('idrt.sourceERPSystemID') == lit(erpGENSystemID))\
              & (col('idrt.businessDatatype') == lit('Is Delivery Return')) \
              & (col('idrt.sourceSystemValue') == col('STD.isDeliveryReturn')) ,how =('left')) \
        .join(itc_L2_DIM_SoDParentTransactionConflictInformationLinkToSL.alias('LTOSL')\
           ,((col("STD.salesParentTransactionID").eqNullSafe(col("LTOSL.parentTransactionID"))) \
             & (col("LTOSL.processID")==lit(1))),how="left")\
        .select(col('STD.transactionDetailSurrogateKey')\
            ,col('STD.salesOrderSubTransactionID').alias('OrderSubTransactionID')\
            ,col('STD.salesTransactionID').alias('TransactionID')\
            ,col('STD.salesParentTransactionID').alias('ParentTransactionID')\
            ,col('STD.companyCode').alias('companyCode')\
            ,col('trat.targetSystemValueDescription').alias('isTransactionArtificial')\
            ,col('ptat.targetSystemValueDescription').alias('isParentTransactionArtificial')\
            ,col('trsv.targetSystemValueDescription').alias('isTransactionService')\
            ,col('STD.orderingDocumentTiming').alias('orderingDocumentTiming')\
            ,col('STD.billingDocumentType').alias('billingDocumentType')\
            ,col('idrt.targetSystemValueDescription').alias('isDeliveryReturn')\
            ,lit(v_isSoDConflict).alias('isSoDConflict')\
            ,coalesce(col('LTOSL.ScenarioContent'),lit('NONE')).alias('SoDConflictScenario')\
               )

    default_List =[[0,0,0,0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE']]
    default_df = spark.createDataFrame(default_List)

    otc_vw_DIM_SalesTransactionDetail=df_vw_DIM_SalesTransactionDetail.union(default_df)
    
    objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_SalesTransactionDetail\
                ,gl_CDMLayer2Path +"otc_vw_DIM_SalesTransactionDetail.parquet") 

    executionStatus = "L2_DIM_SalesSLToGLReconciliation populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
