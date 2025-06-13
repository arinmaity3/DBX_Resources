# Databricks notebook source
def otc_L2_STG_RevenueSLTreeToClearing_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global otc_L2_STG_RevenueSLTreeToClearing

      cte_L1_STG_ReceivableSubledger = fin_L1_STG_ReceivableSubledger.groupBy("receivableSubledgerCompanyCode")\
          .agg(max("receivableSubledgerDatePerspective").alias("cutOffDate"))

      otc_L1_TMP_ReceivableSubledgerClearingDetail = fin_L1_STG_ReceivableSubledger.alias("rvsl")\
          .join(cte_L1_STG_ReceivableSubledger.alias("cte")\
          ,(col('rvsl.receivableSubledgerCompanyCode') == col('cte.receivableSubledgerCompanyCode'))\
          &(col('rvsl.receivableSubledgerDatePerspective') == col('cte.cutOffDate')),how="inner")\
          .select(col('rvsl.IDGLJE').alias('idGlJe')\
          ,col('rvsl.receivableSubledgerClearingStatus').alias('receivableSubledgerClearingStatus')\
          ).distinct()

      cte_L1_TMP_ReceivableSubledgerClearingDetail = otc_L1_TMP_ReceivableSubledgerClearingDetail.groupBy("idGlJe")\
                .agg(count(lit(1)).alias("ClearingStatusCount")).filter(col("ClearingStatusCount") > 1)

      otc_L1_TMP_ReceivableSubledgerClearingDetail = otc_L1_TMP_ReceivableSubledgerClearingDetail.alias('cd')\
          .join(cte_L1_TMP_ReceivableSubledgerClearingDetail.alias('cte')\
          ,(col('cd.idGlJe') == col('cte.idGlJe')),how = 'left')\
          .select(col('cd.idGlJe').alias('idGlJe')\
          ,when(col('cte.ClearingStatusCount').isNotNull(),lit('NA'))\
          .otherwise(col('cd.receivableSubledgerClearingStatus')).alias('receivableSubledgerClearingStatus'))

      innerCase = when (col('arv.receivableSubledgerClearingStatus') == lit('CL'),lit(1))\
                  .when(col('arv.receivableSubledgerClearingStatus') == lit('OP'),lit(0))\
                  .otherwise(lit(2))
      outerCase = when(col('cmc.matchingCategory') == lit('not_cleared'),lit(innerCase))\
                  .otherwise(lit(2))

      otc_L2_STG_RevenueSLTreeToClearing = fin_L2_FACT_RevenueClearing.alias('rc')\
          .join(fin_L2_DIM_RevenueClearingAttribute.alias('rca')\
          ,(col('rc.revenueClearingAttributesSurrogateKey') == col('rca.revenueClearingAttributesSurrogateKey')),how ='inner')\
          .join(fin_L2_DIM_FinancialPeriod.alias('fp')\
          ,(col('rc.reportingDateSurrogateKey') == col('fp.financialPeriodSurrogateKey')),how = 'inner')\
          .join(gen_L2_DIM_Organization.alias('org')\
          ,(col('rc.organizationUnitSurrogateKey') == col('org.organizationUnitSurrogateKey')),how = 'inner')\
          .join(otc_L1_STG_RevenueSLToGL.alias('rev')\
          ,(col('rev.idGLJE') == col('rc.idGlJe'))\
          &(col('rev.idGLJELine') == col('rc.IdGLJELine')),how = 'inner')\
          .join(otc_L1_TMP_ReceivableSubledgerClearingDetail.alias('arv')\
          ,(col('arv.idGlJe') == col('rev.idGLJE')),how = 'left')\
          .join(otc_L2_DIM_ClearingMatchingCategory.alias('cmc')\
          ,(col('cmc.clearingMatchingCategorySurrogateKey') == col('rc.clearingMatchingCategorySurrogateKey')),how = 'left')\
          .select(col('rc.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
          ,col('rc.reportingDateSurrogateKey').alias('reportingDateSurrogateKey')\
          ,col('rev.billingDocumentNumber').alias('slBillingDocumentNumber')\
          ,col('rc.GLDocumentNumber').alias('glDocumentNumber')\
          ,col('rc.clearingTypeChainSurrogateKey').alias('clearingTypeChainSurrogateKey')\
          ,col('rc.clearingMatchingCategorySurrogateKey').alias('clearingMatchingCategorySurrogateKey')\
          ,col('rca.chainCashType').alias('chainCashType')\
          ,lit(outerCase).alias('IsClearedInARAtAnalysisEndDate')).distinct()

      otc_L2_STG_RevenueSLTreeToClearing = objDataTransformation.gen_convertToCDMandCache\
              (otc_L2_STG_RevenueSLTreeToClearing,'otc','L2_STG_RevenueSLTreeToClearing',False)
                  
      executionStatus = "otc_L2_STG_RevenueSLTreeToClearing populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  
