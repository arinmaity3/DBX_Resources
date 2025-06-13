# Databricks notebook source
def fin_L2_FACT_RevenueClearing_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global fin_L2_FACT_RevenueClearing
      defaultChainCashType = lit('No information available')
      defaultMatchingCategoryID = knw_LK_GD_ClearingBoxLookupMatchingCategory.select(col('ID')).filter(col('matchingCategory') == lit('unknown')).collect()[0][0]


      fin_L1_TMP_RevenueClearingStep01 = gen_L1_STG_ClearingBox_BaseData.alias('dat')\
          .join(fin_L1_TD_Journal.alias('joi1')\
          ,(col('dat.idGLJELine') == col('joi1.transactionIDbyPrimaryKey')),how = 'left')\
          .join(fin_L2_FACT_Journal.alias('jrnl2')\
          ,(col('joi1.journalSurrogateKey') == col('jrnl2.journalSurrogateKey')),how = 'left')\
          .join(fin_L2_STG_JEAdditionalAttribute.alias('jead1')\
          ,(col('jead1.journalSurrogateKey') == col('joi1.journalSurrogateKey')),how = 'left')\
          .join(gen_L2_DIM_Organization.alias('orga2')\
          ,((col('dat.clearingBoxCompanyCode') == col('orga2.companyCode'))\
          & (col('dat.clearingBoxClientERP') == col('orga2.clientID'))),how = 'inner')\
          .join(fin_L2_DIM_FinancialPeriod.alias('fp')\
          ,((col('dat.clearingBoxGLPostingDate') == col('fp.postingDate'))\
          & (col('dat.clearingBoxGLFinancialPeriod') == col('fp.financialPeriod'))\
          & (col('orga2.organizationUnitSurrogateKey') == col('fp.organizationUnitSurrogateKey'))),how = 'inner')\
          .join(fin_L2_DIM_GLAccount.alias('glac2')\
          ,((col('dat.clearingBoxGLAccountNumber') == col('glac2.accountNumber'))\
          & (col('orga2.organizationUnitSurrogateKey') == col('glac2.organizationUnitSurrogateKey'))\
          & (col('glac2.accountCategory') == lit('#NA#'))),how = 'inner')\
          .join(knw_LK_GD_ClearingBoxLookupTypeLineItem.alias('ctli')\
          ,(col('dat.idClearingTypeLineItem') == col('ctli.id')),how = 'inner')\
          .join(otc_L2_DIM_ClearingTypeLineItem.alias('ctli2')\
          ,((col('ctli.typeGroup') == col('ctli2.clearingTransactionTypeGroup'))\
          & (col('ctli.typeLineItem') == col('ctli2.clearingTransactionLineItemType'))),how = 'inner')\
          .join(knw_LK_GD_ClearingBoxLookupTypeChain.alias('ctc')\
          ,(col('dat.idClearingTypeChain') == col('ctc.ID')),how = 'inner')\
          .join(otc_L2_DIM_ClearingTypeChain.alias('cltc2')\
          ,((col('ctc.typeGroup') == col('cltc2.clearingTypeGroup'))\
          & (col('ctc.typeChain') == col('cltc2.clearingTypeChain'))),how = 'inner')\
          .join(gen_L1_STG_ClearingBox_BaseDataByChain.alias('bc')\
          ,(col('dat.idGroupChain') == col('bc.idGroupChain')),how = 'left')\
          .join(knw_LK_GD_ClearingBoxLookupMatchingCombination.alias('mc')\
          ,((col('mc.reportSubcategory') == col('ctc.reportSubcategory'))\
          & (col('mc.cashTypeText') == col('bc.chainCashType'))),how = 'left')\
          .select(col('orga2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
		          ,lit(0).alias('userSurrogateKey')\
				  ,lit(0).alias('customerSurrogateKey')\
				  ,lit(0).alias('clearingChainCustomerSurrogateKey')\
				  ,col('glac2.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
				  ,col('fp.financialPeriodSurrogateKey').alias('reportingDateSurrogateKey')\
				  ,when(col('jrnl2.JEDocumentClassificationSurrogateKey').isNull(),lit(0))\
				  .otherwise(col('jrnl2.JEDocumentClassificationSurrogateKey')).alias('documentClassificationSurrogateKey')\
				  ,when(col('jead1.JEAdditionalAttributeSurrogateKey').isNull(),lit(0))\
				  .otherwise(col('jead1.JEAdditionalAttributeSurrogateKey')).alias('JEAdditionalAttributeSurrogateKey')\
				  ,col('dat.clearingBoxClientERP').alias('client')\
				  ,col('dat.clearingBoxCompanyCode').alias('companyCode')\
				  ,col('dat.idGroupChain').alias('idGroupChain')\
				  ,col('dat.chainInterference').alias('chainInference')\
				  ,col('cltc2.clearingTypeChainSurrogateKey').alias('clearningTypeChainSurrogateKey')\
				  ,col('ctli2.clearingTransactionTypeLineItemsSurrogateKey').alias('clearingTypeLineItemSurrogateKey')\
				  ,when(col('mc.clearingMatchingCategorySurrogateKey').isNull(),lit(defaultMatchingCategoryID))\
				  .otherwise(col('mc.clearingMatchingCategorySurrogateKey')).alias('clearingMatchingCategorySurrogateKey')\
				  ,col('dat.idGLJE').alias('idGlJe')\
				  ,col('dat.idGLJELine').alias('IdGLJELine')\
				  ,col('dat.clearingBoxGLClearingDocument').alias('GLCLearingDocumentNumber')\
				  ,col('dat.clearingBoxGLPostingDate').alias('GlDocumentPostingDate')\
				  ,col('dat.clearingBoxGLClearingPostingDate').alias('GLClearingDocumentPostingDate')\
				  ,when(col('bc.clearingBoxGLClearingPostingDateMax').isNull(),lit('1900-01-01'))\
				  .otherwise(col('bc.clearingBoxGLClearingPostingDateMax')).alias('GLClearingDocumentPostingDateMax')\
				  ,col('dat.clearingBoxGLJEDocument').alias('GLDocumentNumber')\
				  ,col('dat.clearingBoxGLJELineNumber').alias('GLLineItem')\
				  ,when(col('dat.clearingBoxRatioRevenuePaid').isNull(),lit(0))\
				  .otherwise(col('dat.clearingBoxRatioRevenuePaid')).alias('ratioRevenuePaid')\
				  ,when(col('dat.isRevenueFullyReversed').isNull(),lit(0))\
				  .otherwise(col('dat.isRevenueFullyReversed')).alias('isRevenueFullyReversed')\
				  ,lit(0).alias('isRatioInvalid')\
				  ,col('dat.clearingBoxGLAmountRevenueLC').alias('amountNetRevenue')\
				  ,col('dat.clearingBoxGLAmountLC').alias('amountDebitCredit')\
				  ,col('dat.clearingBoxAmountSign').alias('amountSign')\
				  ,lit(0).alias('amountARDiscountLC')\
				  ,lit(0).alias('amountRelatedvatLC')\
				  ,lit('').alias('GLSpecialIndicator')\
				  ,when(col('dat.isOrigin').isNull(),lit(0))\
				  .otherwise(col('dat.isOrigin')).alias('isOrigin')\
				  ,when(col('dat.isClearing').isNull(),lit(0))\
				  .otherwise(col('dat.isClearing')).alias('isClearing')\
				  ,when(col('dat.isRevenue').isNull(),lit(0))\
				  .otherwise(col('dat.isRevenue')).alias('isRevenue')\
				  ,when(col('dat.isVAT').isNull(),lit(0))\
				  .otherwise(col('dat.isVAT')).alias('isVAT')\
				  ,when(col('dat.isTradeAR').isNull(),lit(0))\
				  .otherwise(col('dat.isTradeAR')).alias('isTradeAR')\
				  ,when(col('dat.isTradeAROpen').isNull(),lit(0))\
				  .otherwise(col('dat.isTradeAROpen')).alias('isTradeAROpen')\
				  ,when(col('dat.isRevenueOpen').isNull(),lit(0))\
				  .otherwise(col('dat.isRevenueOpen')).alias('isRevenueOpen')\
				  ,when(col('dat.isNotExistsTradeAR').isNull(),lit(0))\
				  .otherwise(col('dat.isNotExistsTradeAR')).alias('isNotExistsTradeAR')\
				  ,when(col('dat.isTradeAP').isNull(),lit(0))\
				  .otherwise(col('dat.isTradeAP')).alias('isTradeAP')\
				  ,when(col('dat.isOtherAR').isNull(),lit(0))\
				  .otherwise(col('dat.isOtherAR')).alias('isOtherAR')\
				  ,when(col('dat.isOtherAP').isNull(),lit(0))\
				  .otherwise(col('dat.isOtherAP')).alias('isOtherAP')\
				  ,when(col('dat.isBankCash').isNull(),lit(0))\
				  .otherwise(col('dat.isBankCash')).alias('isBankCash')\
				  ,when(col('dat.isInterCompanyAccount').isNull(),lit(0))\
				  .otherwise(col('dat.isInterCompanyAccount')).alias('isInterCompanyAccount')\
				  ,when(col('dat.isInterCompanyAccountChain').isNull(),lit(0))\
				  .otherwise(col('dat.isInterCompanyAccountChain')).alias('isInterCompanyAccountChain')\
				  ,when(col('dat.isReversedOrReversal').isNull(),lit(0))\
				  .otherwise(col('dat.isReversedOrReversal')).alias('isReversal')\
				  ,lit(0).alias('flagBatchInput')\
				  ,when(col('dat.isInTimeFrame').isNull(),lit(0))\
				  .otherwise(col('dat.isInTimeFrame')).alias('isInTimeFrame')\
				  ,lit(0).alias('isExistARClearing')\
				  ,when(col('dat.isRevisedByRevenue').isNull(),lit(0))\
				  .otherwise(col('dat.isRevisedByRevenue')).alias('isRevisedByRevenue')\
				  ,when(col('bc.isMissingJE').isNull(),lit(0))\
				  .otherwise(col('bc.isMissingJE')).alias('isMissingJE')\
				  ,when(col('dat.EBFKey').isNotNull(),lit(True))\
				  .otherwise(lit(False)).alias('isLinkEBFKey')\
				  ,when(col('dat.EBFItem').isNotNull(),lit(True))\
				  .otherwise(lit(False)).alias('isLinkEBFItem')\
				  ,when(col('bc.isChainFromPY').isNull(),lit(0))\
				  .otherwise(col('bc.isChainFromPY')).alias('isChainFromPY')\
				  ,when(col('bc.isChainFromSubsequent').isNull(),lit(0))\
				  .otherwise(col('bc.isChainFromSubsequent')).alias('isChainFromSubsequent')\
				  ,when(col('dat.isMisinterpretedChain').isNull(),lit(0))\
				  .otherwise(col('dat.isMisinterpretedChain')).alias('isMisinterpretedChain')\
				  ,lit(0).alias('caseId')\
				  ,col('dat.clearingBoxGLReferenceSLSource').alias('GLDocumentSubSource')\
				  ,col('dat.EBFKey').alias('EBFKey')\
				  ,col('dat.EBFItem').alias('EBFItem')\
				  ,col('dat.clearingBoxGLCustomerNumber').alias('customerNumber')\
				  ,col('dat.clearingBoxChainCustomerNumber').alias('chainCustomerNumber')\
				  ,col('dat.clearingBoxGLCreatedByUser').alias('GLDocumentUser')\
				  ,col('dat.clearingBoxGLAccountNumber').alias('GLAccountNumber')\
				  ,lit(0).alias('heuristicDifferenceReason')\
				  ,when(col('bc.chainCashType').isNull(),lit(defaultChainCashType))\
				  .otherwise(col('bc.chainCashType')).alias('chainCashType')\
				  ,when(col('dat.clearingBoxReGrouping').isNull(),lit(0))\
				  .otherwise(col('dat.clearingBoxReGrouping')).alias('regrouping')\
				  ,when(col('dat.clearingBoxGLReferenceSLSource') == lit('Sales S/L'),col('dat.clearingBoxSLDocumentNumber'))\
				  .otherwise(lit('')).alias('billingDocumentNumber'))

      colDerived_1 = uuid.uuid4().hex
      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.alias('rcsp1')\
		  .join(gen_L2_DIM_Organization.alias('ognt2')\
		  ,((col('rcsp1.companyCode') == col('ognt2.companyCode'))\
		  &(col('rcsp1.client') == col('ognt2.clientID'))),how = 'left')\
		  .join(gen_L2_DIM_User.alias('usrd2')\
		  ,((col('rcsp1.GLDocumentUser') == col('usrd2.userName'))\
		  & (col('ognt2.organizationUnitSurrogateKey') == col('usrd2.organizationUnitSurrogateKey'))),how = 'left')\
		  .select(col('rcsp1.*'),col('usrd2.userSurrogateKey').alias('usrd2_userSurrogateKey'))\
		  .withColumn(colDerived_1,when(col('usrd2_userSurrogateKey').isNull(),col('rcsp1.userSurrogateKey'))\
		  .otherwise(col('usrd2_userSurrogateKey')))

      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.drop(*['userSurrogateKey','usrd2_userSurrogateKey'])
      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.withColumnRenamed(colDerived_1,'userSurrogateKey')

      colDerived_2 = uuid.uuid4().hex
      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.alias('rcsp1')\
		  .join(gen_L2_DIM_Organization.alias('ognt2')\
		  ,((col('rcsp1.companyCode') == col('ognt2.companyCode'))\
		  &(col('rcsp1.client') == col('ognt2.clientID'))),how = 'left')\
		  .join(otc_L2_DIM_Customer.alias('ctmr2')\
		  ,((col('rcsp1.customerNumber') == col('ctmr2.customerNumber'))\
		  & (col('ognt2.organizationUnitSurrogateKey') == col('ctmr2.organizationUnitSurrogateKey'))),how = 'left')\
		  .select(col('rcsp1.*'),col('ctmr2.customerSurrogateKey').alias('ctmr2_customerSurrogateKey'))\
		  .withColumn(colDerived_2,when(col('ctmr2_customerSurrogateKey').isNull(),col('rcsp1.customerSurrogateKey'))\
		  .otherwise(col('ctmr2_customerSurrogateKey')))

      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.drop(*['customerSurrogateKey','ctmr2_customerSurrogateKey'])
      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.withColumnRenamed(colDerived_2,'customerSurrogateKey')

      colDerived_3 = uuid.uuid4().hex
      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.alias('rcsp1')\
		  .join(gen_L2_DIM_Organization.alias('ognt2')\
		  ,((col('rcsp1.companyCode') == col('ognt2.companyCode'))\
		  &(col('rcsp1.client') == col('ognt2.clientID'))),how = 'left')\
		  .join(otc_L2_DIM_Customer.alias('ctmr2')\
		  ,((col('rcsp1.chainCustomerNumber') == col('ctmr2.customerNumber'))\
		  & (col('ognt2.organizationUnitSurrogateKey') == col('ctmr2.organizationUnitSurrogateKey'))),how = 'left')\
		  .select(col('rcsp1.*'),col('ctmr2.customerSurrogateKey').alias('ctmr2_customerSurrogateKey'))\
		  .withColumn(colDerived_3,when(col('ctmr2_customerSurrogateKey').isNull(),col('rcsp1.clearingChainCustomerSurrogateKey'))\
		  .otherwise(col('ctmr2_customerSurrogateKey')))

      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.drop(*['clearingChainCustomerSurrogateKey','ctmr2_customerSurrogateKey'])
      fin_L1_TMP_RevenueClearingStep01 = fin_L1_TMP_RevenueClearingStep01.withColumnRenamed(colDerived_3,'clearingChainCustomerSurrogateKey')

      w = Window().orderBy(lit(''))
      fin_L2_FACT_RevenueClearing = fin_L1_TMP_RevenueClearingStep01.alias('rcs1')\
		  .join(fin_L2_DIM_RevenueClearingAttribute.alias('rclat2')\
		  ,((col('rclat2.isChainFromPY') == col('rcs1.isChainFromPY'))\
		  & (col('rclat2.isChainFromSubsequent') == col('rcs1.isChainFromSubsequent'))\
		  & (col('rclat2.isExistsARClearing') == col('rcs1.isExistARClearing'))\
		  & (col('rclat2.isInterCompanyAccount') == col('rcs1.isInterCompanyAccount'))\
		  & (col('rclat2.isInterCompanyAccountChain') == col('rcs1.isInterCompanyAccountChain'))\
		  & (col('rclat2.isLinkEBFItem') == col('rcs1.isLinkEBFItem'))\
		  & (col('rclat2.isLInkEBFKey') == col('rcs1.isLinkEBFKey'))\
		  & (col('rclat2.isMisinterpretedChain') == col('rcs1.isMisinterpretedChain'))\
		  & (col('rclat2.isMissingJE') == col('rcs1.isMissingJE'))\
		  & (col('rclat2.isNotExistsTradeAR') == col('rcs1.isNotExistsTradeAR'))\
		  & (col('rclat2.isOtherAP') == col('rcs1.isOtherAP'))\
		  & (col('rclat2.isOtherAR') == col('rcs1.isOtherAR'))\
		  & (col('rclat2.isRevenue') == col('rcs1.isRevenue'))\
		  & (col('rclat2.isRevenueOpen') == col('rcs1.isRevenueOpen'))\
		  & (col('rclat2.isReversal') == col('rcs1.isReversal'))\
		  & (col('rclat2.isRevisedByRevenue') == col('rcs1.isRevisedByRevenue'))\
		  & (col('rclat2.isTradeAP') == col('rcs1.isTradeAP'))\
		  & (col('rclat2.isTradeAR') == col('rcs1.isTradeAR'))\
		  & (col('rclat2.isTradeAROpen') == col('rcs1.isTradeARopen'))\
		  & (col('rclat2.isVAT') == col('rcs1.isVAT'))\
		  & (col('rclat2.heuristicDifferenceReason') == col('rcs1.heuristicDifferenceReason'))\
		  & (col('rclat2.glSpecialIndicator') == col('rcs1.GLSpecialIndicator'))\
		  & (col('rclat2.isInTimeFrame') == col('rcs1.isInTimeFrame'))\
		  & (col('rclat2.chainCashType') == col('rcs1.chainCashType'))\
		  & (col('rclat2.isRevenueFullyReversed') == col('rcs1.isRevenueFullyReversed'))\
		  & (col('rclat2.isRatioInvalid') == col('rcs1.isRatioInvalid'))\
		  & (col('rclat2.isOrigin') == col('rcs1.isOrigin'))\
		  & (col('rclat2.isClearing') == col('rcs1.isClearing'))\
		  & (col('rclat2.isBankCash') == col('rcs1.isBankCash'))\
		  & (col('rclat2.caseID') == col('rcs1.caseId'))\
		  & (col('rclat2.flagBatchInput') == col('rcs1.flagBatchInput'))\
		  & (col('rclat2.reGrouping') == col('rcs1.regrouping'))),how = 'inner')\
		  .join(gen_L2_DIM_CalendarDate.alias('calDate2')\
		  ,((col('rcs1.organizationUnitSurrogateKey') == col('calDate2.organizationUnitSurrogateKey'))\
		  & (col('rcs1.GLClearingDocumentPostingDate') == col('calDate2.calendarDate'))),how = 'left')\
		  .select(col('rcs1.clearningTypeChainSurrogateKey').alias('clearingTypeChainSurrogateKey')\
		  ,col('rcs1.clearingTypeLineItemSurrogateKey').alias('clearingTypeLineItemSurrogateKey')\
		  ,col('rcs1.clearingMatchingCategorySurrogateKey').alias('clearingMatchingCategorySurrogateKey')\
		  ,col('rcs1.documentClassificationSurrogateKey').alias('documentClassificationSurrogateKey')\
		  ,col('rcs1.JEAdditionalAttributeSurrogateKey').alias('JEAdditionalAttributeSurrogateKey')\
		  ,col('rcs1.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
		  ,col('rclat2.revenueClearingAttributesSurrogateKey').alias('revenueClearingAttributesSurrogateKey')\
		  ,col('rcs1.reportingDateSurrogateKey').alias('reportingDateSurrogateKey')\
		  ,col('rcs1.customerSurrogateKey').alias('customerSurrogateKey')\
		  ,col('rcs1.userSurrogateKey').alias('userSurrogateKey')\
		  ,col('rcs1.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
		  ,col('rcs1.ratioRevenuePaid').alias('ratioRevenuePaid')\
		  ,col('rcs1.amountNetRevenue').alias('amountNetRevenue')\
		  ,col('rcs1.amountDebitCredit').alias('amountDebitCredit')\
		  ,col('rcs1.amountSign').alias('amountSign')\
		  ,col('rcs1.amountARDiscountLC').alias('amountARDiscountLC')\
		  ,col('rcs1.amountRelatedvatLC').alias('amountReleventVatLC')\
		  ,col('rcs1.idGroupChain').alias('idGroupChain')\
		  ,col('rcs1.chainInference').alias('chainInference')\
		  ,col('rcs1.EBFItem').alias('EBFitem')\
		  ,col('rcs1.EBFKey').alias('EBFKey')\
		  ,col('rcs1.GLCLearingDocumentNumber').alias('GLCLearingDocumentNumber')\
		  ,when(col('calDate2.dateSurrogateKey').isNull(),lit(0))\
		  .otherwise(col('calDate2.dateSurrogateKey')).alias('GLClearingDocumentPostingDatesurrogateKey')\
		  ,col('rcs1.GLDocumentNumber').alias('GLDocumentNumber')\
		  ,col('rcs1.GLLineItem').alias('GLLineItem')\
		  ,col('rcs1.idGlJe').alias('idGlJe')\
		  ,col('rcs1.IdGLJELine').alias('IdGLJELine')\
		  ,when(month(col('calDate2.calendarDate')).isNull(),lit(1))\
		  .otherwise(month(col('calDate2.calendarDate'))).alias('periodId')\
		  ,col('rcs1.billingDocumentNumber').alias('billingDocumentNumber'))\
		  .withColumn("revenueClearingSurrogateKey", row_number().over(w))

      agg_gen_L2_DIM_CalendarDate = gen_L2_DIM_CalendarDate.alias('agg')\
		              .filter(('agg.calendarDate') == lit('1900-01-01'))\
    				  .groupBy('agg.organizationUnitSurrogateKey')\
                      .agg(min('agg.dateSurrogateKey').alias('dateSurrogateKey'))

      colDerived_4 = uuid.uuid4().hex
      fin_L2_FACT_RevenueClearing = fin_L2_FACT_RevenueClearing.alias('rcle2')\
		  .join(agg_gen_L2_DIM_CalendarDate.alias('calDate')\
		  ,(col('rcle2.organizationUnitSurrogateKey') == col('calDate.organizationUnitSurrogateKey')),how = 'left')\
		  .select(col('rcle2.*'),col('calDate.dateSurrogateKey').alias('calDate_dateSurrogateKey'))\
		  .withColumn(colDerived_4,when((col('rcle2.GLClearingDocumentPostingDatesurrogateKey') == lit(0)),col('calDate_dateSurrogateKey'))\
		  .otherwise(col('rcle2.GLClearingDocumentPostingDatesurrogateKey')))

      fin_L2_FACT_RevenueClearing = fin_L2_FACT_RevenueClearing.drop(*['GLClearingDocumentPostingDatesurrogateKey','calDate_dateSurrogateKey'])
      fin_L2_FACT_RevenueClearing = fin_L2_FACT_RevenueClearing.withColumnRenamed(colDerived_4,'GLClearingDocumentPostingDatesurrogateKey')

      fin_L2_FACT_RevenueClearing  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L2_FACT_RevenueClearing,'fin','L2_FACT_RevenueClearing',False)

      default_List =[[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'','','',0,'0000000000','000',0,0,1,'0000000000']]
      default_df = spark.createDataFrame(default_List)

      otc_vw_FACT_RevenueClearing = fin_L2_FACT_RevenueClearing.alias('fact')\
		  .select(col('fact.revenueClearingSurrogateKey').alias('revenueClearingSurrogateKey')\
		  ,col('fact.clearingTypeChainSurrogateKey').alias('clearingTypeChainSurrogateKey')\
		  ,col('fact.clearingTypeLineItemSurrogateKey').alias('clearingTypeLineItemSurrogateKey')\
		  ,col('fact.documentClassificationSurrogateKey').alias('documentClassificationSurrogateKey')\
		  ,col('fact.clearingMatchingCategorySurrogateKey').alias('clearingMatchingCategorySurrogateKey')\
		  ,col('fact.JEAdditionalAttributeSurrogateKey').alias('JEAdditionalAttributeSurrogateKey')\
		  ,col('fact.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
		  ,when(col('fact.currencySurrogateKey').isNull(),lit(0))\
		  .otherwise(col('fact.currencySurrogateKey')).alias('currencySurrogateKey')\
		  ,col('fact.revenueClearingAttributesSurrogateKey').alias('revenueClearingAttributesSurrogateKey')\
		  ,col('fact.reportingDateSurrogateKey').alias('reportingDateSurrogateKey')\
		  ,col('fact.customerSurrogateKey').alias('customerSurrogateKey')\
		  ,col('fact.userSurrogateKey').alias('userSurrogateKey')\
		  ,col('fact.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
		  ,col('fact.ratioRevenuePaid').alias('ratioRevenuePaid')\
		  ,col('fact.amountNetRevenue').alias('amountNetRevenue')\
		  ,when(col('fact.amountDebitCredit') > lit(0),col('fact.amountDebitCredit'))\
		  .otherwise(lit(0)).alias('amountDebit')\
		  ,when(col('fact.amountDebitCredit') < lit(0),col('fact.amountDebitCredit'))\
		  .otherwise(lit(0)).alias('amountCredit')\
		  ,when(col('fact.amountDebitCredit') > lit(0),col('fact.amountNetRevenue'))\
		  .otherwise(lit(0)).alias('amountRevenueDebit')\
		  ,when(col('fact.amountDebitCredit') < lit(0),col('fact.amountNetRevenue'))\
		  .otherwise(lit(0)).alias('amountRevenueCredit')\
		  ,col('fact.amountDebitCredit').alias('amountDebitCredit')\
		  ,col('fact.amountSign').alias('amountSign')\
		  ,col('fact.amountARDiscountLC').alias('amountARDiscountLC')\
		  ,col('fact.amountReleventVatLC').alias('amountReleventVatLC')\
		  ,col('fact.idGroupChain').alias('idGroupChain')\
		  ,when(col('fact.chainInference').isNull(),lit(0))\
		  .otherwise(col('fact.chainInference')).alias('chainInference')\
		  ,col('fact.EBFitem').alias('EBFitem')\
		  ,col('fact.EBFKey').alias('EBFKey')\
		  ,col('fact.GLCLearingDocumentNumber').alias('GLCLearingDocumentNumber')\
		  ,col('fact.GLClearingDocumentPostingDatesurrogateKey').alias('GLClearingDocumentPostingDatesurrogateKey')\
		  ,col('fact.GLDocumentNumber').alias('GLDocumentNumber')\
		  ,col('fact.GLLineItem').alias('GLLineItem')\
		  ,col('fact.idGlJe').alias('idGlJe')\
		  ,col('fact.IdGLJELine').alias('IdGLJELine')\
		  ,col('fact.periodId').alias('periodId')\
		  ,when(col('fact.billingDocumentNumber') != lit(''),col('fact.billingDocumentNumber'))\
		  .otherwise(lit('NONE')).alias('billingDocumentNumber'))\
		  .union(default_df)
      
      otc_vw_FACT_RevenueClearing = objDataTransformation.gen_convertToCDMStructure_generate\
		  (otc_vw_FACT_RevenueClearing,'otc','vw_FACT_RevenueClearing',isIncludeAnalysisID = True, isSqlFormat = True)[0]
      objGenHelper.gen_writeToFile_perfom(otc_vw_FACT_RevenueClearing,gl_CDMLayer2Path + "fin_L2_FACT_RevenueClearing.parquet" )

      executionStatus = "fin_L2_FACT_RevenueClearing populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  
