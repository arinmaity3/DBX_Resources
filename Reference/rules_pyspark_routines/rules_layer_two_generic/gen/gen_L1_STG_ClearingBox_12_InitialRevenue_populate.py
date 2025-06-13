# Databricks notebook source
def gen_L1_STG_ClearingBox_12_InitialRevenue_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L1_STG_ClearingBox_12_AllJournal
      global gen_L1_STG_ClearingBox_12_RevenueByDocument
      global gen_L1_STG_ClearingBox_12_RevenueByGroupChain

      # variables not definedor assigned with values @tmpId,@tmpTypeGroupLI,@tmpTypeLI

      gen_L1_STG_ClearingBox_12_AllJournal = gen_L1_STG_ClearingBox_11_JournalInterpreted.alias('crbx11')\
          .select(\
          col('crbx11.idGroupChain').alias('idGroupChain')\
          ,col('crbx11.chainInterference').alias('chainInterference')\
          ,col('crbx11.idClearingTypeChain').alias('idClearingTypeChain')\
          ,col('crbx11.typeGroupChain').alias('typeGroupChain')\
          ,col('crbx11.typeChain').alias('typeChain')\
          ,expr("coalesce(\
                          CASE WHEN crbx11.isRevenue = 1 AND crbx11.typeGroupLineItem = 'revenue' THEN crbx11.idClearingTypeLine ELSE NULL END\
						  ,NULL \
						  ,crbx11.idClearingTypeLine\
		   )").alias('idClearingTypeLineItem')\
          ,expr("coalesce(\
							CASE WHEN crbx11.isRevenue = 1 AND crbx11.typeGroupLineItem = 'revenue' THEN crbx11.typeGroupLineItem ELSE NULL END\
							,NULL\
							,crbx11.typeGroupLineItem\
		   )").alias('typeGroupLineItem')\
          ,expr("coalesce(\
							CASE WHEN crbx11.isRevenue = 1 AND crbx11.typeGroupLineItem = 'revenue' THEN crbx11.typeLineItem ELSE NULL END\
							,NULL\
							,crbx11.typeLineItem\
		   )").alias('typeLineItem')\
          ,col('crbx11.idGLJE').alias('idGLJE')\
          ,col('crbx11.idGLJELine').alias('idGLJELine')\
          ,col('crbx11.clearingBoxClientERP').alias('clearingBoxClientERP')\
          ,col('crbx11.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
          ,col('crbx11.clearingBoxGLFiscalYear').alias('clearingBoxGLFiscalYear')\
          ,col('crbx11.clearingBoxGLFinancialPeriod').alias('clearingBoxGLFinancialPeriod')\
          ,col('crbx11.clearingBoxGLPostingDate').alias('clearingBoxGLPostingDate')\
          ,col('crbx11.clearingBoxGLClearingDocument').alias('clearingBoxGLClearingDocument')\
          ,col('crbx11.clearingBoxGLClearingPostingDate').alias('clearingBoxGLClearingPostingDate')\
          ,col('crbx11.clearingBoxGLJEDocument').alias('clearingBoxGLJEDocument')\
          ,col('crbx11.clearingBoxGLJELineNumber').alias('clearingBoxGLJELineNumber')\
          ,col('crbx11.clearingBoxGLAccountNumber').alias('clearingBoxGLAccountNumber')\
          ,col('crbx11.clearingBoxGLAccountDescription').alias('clearingBoxGLAccountDescription')\
          ,col('crbx11.clearingBoxGLAccountCategory').alias('clearingBoxGLAccountCategory')\
          ,col('crbx11.clearingBoxTrackAccountCategory').alias('clearingBoxTrackAccountCategory')\
          ,col('crbx11.clearingBoxGLAmountLC').alias('clearingBoxGLAmountLC')\
          ,when(col('crbx11.isRevenue') == True,(lit(-1.0) * col('crbx11.clearingBoxGLAmountLC')))\
          .otherwise(lit(0)).alias('clearingBoxGLAmountRevenueLC')\
          ,col('crbx11.clearingBoxAmountSign').alias('clearingBoxAmountSign')\
          ,col('crbx11.clearingBoxGLReferenceSLSource').alias('clearingBoxGLReferenceSLSource')\
          ,col('crbx11.clearingBoxSLDocumentNumber').alias('clearingBoxSLDocumentNumber')\
          ,col('crbx11.clearingBoxSLFiscalYear').alias('clearingBoxSLFiscalYear')\
          ,col('crbx11.clearingBoxSLDocumentTypeERP').alias('clearingBoxSLDocumentTypeERP')\
          ,col('crbx11.clearingBoxSLDocumentType').alias('clearingBoxSLDocumentType')\
          ,col('crbx11.clearingBoxGLReferenceDocument').alias('clearingBoxGLReferenceDocument')\
          ,when(((col('crbx11.clearingBoxGLCustomerNumber').isNull()) | (col('crbx11.clearingBoxGLCustomerNumber')==lit(''))),lit('#NA#'))\
          .otherwise(col('crbx11.clearingBoxGLCustomerNumber')).alias('clearingBoxGLCustomerNumber')\
          ,when(((col('crbx11.clearingBoxGLVendorNumber').isNull()) | (col('crbx11.clearingBoxGLVendorNumber')==lit(''))),lit('#NA#'))\
          .otherwise(col('crbx11.clearingBoxGLVendorNumber')).alias('clearingBoxGLVendorNumber')\
          ,when(((col('crbx11.clearingBoxChainCustomerNumber').isNull()) | (col('crbx11.clearingBoxChainCustomerNumber')==lit(''))),lit('#NA#'))\
          .otherwise(col('crbx11.clearingBoxChainCustomerNumber')).alias('clearingBoxChainCustomerNumber')\
          ,when(((col('crbx11.clearingBoxChainVendorNumber').isNull()) | (col('crbx11.clearingBoxChainVendorNumber')==lit(''))),lit('#NA#'))\
          .otherwise(col('crbx11.clearingBoxChainVendorNumber')).alias('clearingBoxChainVendorNumber')\
          ,lit(0).alias('clearingBoxRatioRevenuePaid')\
          ,lit(False).alias('isRevenueFullyReversed')\
          ,col('crbx11.isOrigin').alias('isOrigin')\
          ,col('crbx11.isClearing').alias('isClearing')\
          ,col('crbx11.isRevenue').alias('isRevenue')\
          ,col('crbx11.isVAT').alias('isVAT')\
          ,col('crbx11.isTradeAR').alias('isTradeAR')\
          ,when(((col('crbx11.isTradeAR') == True) & (col('crbx11.clearingBoxGLClearingDocument') == lit(''))),lit(True))\
          .otherwise(lit(False)).alias('isTradeAROpen')\
          ,when(((col('crbx11.isTradeAP') == True) & (col('crbx11.clearingBoxGLClearingDocument') == lit(''))),lit(True))\
          .otherwise(lit(False)).alias('isTradeAPOpen')\
          ,lit(False).alias('isRevenueOpen')\
          ,lit(False).alias('isNotExistsTradeAR')\
          ,lit(False).alias('isARExists')\
          ,lit(False).alias('isOpenARExists')\
          ,lit(False).alias('isNotExistsTradeAP')\
          ,lit(False).alias('isAPExists')\
          ,lit(False).alias('isOpenAPExists')\
          ,col('crbx11.isTradeAP').alias('isTradeAP')\
          ,col('crbx11.isOtherAR').alias('isOtherAR')\
          ,col('crbx11.isOtherAP').alias('isOtherAP')\
          ,col('crbx11.isBankCash').alias('isBankCash')\
          ,col('crbx11.isInterCompanyAccount').alias('isInterCompanyAccount')\
          ,col('crbx11.isReversedOrReversal').alias('isReversedOrReversal')\
          ,col('crbx11.isInTimeFrame').alias('isInTimeFrame')\
          ,lit(False).alias('isRevisedByRevenue')\
          ,lit(False).alias('isInterCompanyAccountChain')\
          ,col('crbx11.isMissingJE').alias('isMissingJE')\
          ,when(col('crbx11.isCurrencyFX').isNull(),lit(False))\
          .otherwise(col('crbx11.isCurrencyFX')).alias('isCurrencyFX')\
          ,when(col('crbx11.isMisInterpretedChain').isNull(),lit(False))\
          .otherwise(col('crbx11.isMisInterpretedChain')).alias('isMisInterpretedChain')\
          ,col('crbx11.isOpenItemPY').alias('isOpenItemPY')\
          ,col('crbx11.isOpenItemYE').alias('isOpenItemYE')\
          ,col('crbx11.clearingBoxReGrouping').alias('clearingBoxReGrouping')\
          ,lit(1).alias('caseID')\
          )

      gen_L1_STG_ClearingBox_12_RevenueByDocument = gen_L1_STG_ClearingBox_12_AllJournal.alias('crbx12')\
          .groupBy('crbx12.idGLJE').agg(\
          expr("max(crbx12.clearingBoxClientERP)").alias('clearingBoxClientERP')\
          ,expr("max(crbx12.clearingBoxCompanyCode)").alias('clearingBoxCompanyCode')\
          ,expr("max(crbx12.clearingBoxGLFiscalYear)").alias('clearingBoxGLFiscalYear')\
          ,expr("max(crbx12.clearingBoxGLPostingDate)").alias('clearingBoxGLPostingDate')\
          ,expr("max(crbx12.clearingBoxGLJEDocument)").alias('clearingBoxGLJEDocument')\
          ,expr("sum(crbx12.clearingBoxGLAmountRevenueLC)").alias('clearingBoxGLAmountRevenueLC')\
          ,expr("sum(CASE WHEN crbx12.isRevenue = True THEN crbx12.clearingBoxGLAmountLC ELSE 0 END)").alias('clearingBoxGLAmountRevenueLCORI')\
          ,expr("sum(crbx12.clearingBoxGLAmountLC)").alias('clearingBoxGLAmountLC')\
          ,expr("sum(CASE WHEN crbx12.isVAT = True THEN crbx12.clearingBoxGLAmountLC ELSE 0 END)").alias('amountVAT')\
          ,expr("cast((CASE WHEN max(cast(crbx12.isTradeAR as INT)) = 0 THEN 1 ELSE 0 END) as BOOLEAN)").alias('isNotExistsTradeAR')\
          ,expr("cast((CASE WHEN max(cast(crbx12.isTradeAP as INT)) = 0 THEN 1 ELSE 0 END) as BOOLEAN)").alias('isNotExistsTradeAP')\
          ,expr("cast(max(cast(crbx12.isTradeAR as INT)) as BOOLEAN)").alias('isTradeAR')\
          ,expr("cast(max(cast(crbx12.isTradeAP as INT)) as BOOLEAN)").alias('isTradeAP')\
          ,expr("cast(MIN(cast((CASE WHEN crbx12.isTradeAR = True AND crbx12.isTradeAROpen = False THEN 0 ELSE 1 END) as INT)) as BOOLEAN)").alias('isTradeAROpen')\
          ,expr("cast(MIN(cast((CASE WHEN crbx12.isTradeAP = True AND crbx12.isTradeAPOpen = False THEN 0 ELSE 1 END) as INT)) as BOOLEAN)").alias('isTradeAPOpen')\
          ,expr("cast(max(cast(crbx12.isRevenue as INT)) as BOOLEAN)").alias('isRevenue')\
          ,expr("cast(\
						CASE \
                            WHEN max(cast(crbx12.isRevenue as INT)) = 1 AND max(CASE \
                                                                                    WHEN (crbx12.isTradeAR = True OR crbx12.isOtherAR = True) AND crbx12.clearingBoxGLClearingDocument = '' \
                                                                                    THEN 1 ELSE 0 END) = 1 \
                            THEN 1 ELSE 0 END as BOOLEAN\
		   )").alias('isRevenueOpen')\
          ,expr("cast(min(cast((CASE WHEN crbx12.isTradeAR = True AND crbx12.isInterCompanyAccount = False THEN 0 ELSE 1 END) as INT)) as BOOLEAN)").alias('isTradeARInterCompany')\
          ,expr("cast(min(cast((CASE WHEN crbx12.isTradeAP = True AND crbx12.isInterCompanyAccount = False THEN 0 ELSE 1 END) as INT)) as BOOLEAN)").alias('isTradeAPInterCompany')\
          ,expr("cast(max(cast(crbx12.isOtherAR as INT)) as BOOLEAN)").alias('isOtherAR')\
          ,expr("cast(max(cast(crbx12.isOtherAP as INT)) as BOOLEAN)").alias('isOtherAP')\
          ,expr("cast(max(cast(crbx12.isBankCash as INT)) as BOOLEAN)").alias('isBankCash')\
          ,expr("cast(max(cast(crbx12.isReversedOrReversal as INT)) as BOOLEAN)").alias('isReversedOrReversal')\
          ,expr("cast(max(cast(crbx12.isInTimeFrame as INT)) as BOOLEAN)").alias('isInTimeFrame')\
          ,expr("cast((CASE WHEN round(sum(CASE WHEN crbx12.isVAT = True THEN crbx12.clearingBoxGLAmountLC ELSE 0 END), 2) <> 0 \
                       AND sum(crbx12.clearingBoxGLAmountRevenueLC) <> 0 THEN 1 ELSE 0 END) as BOOLEAN)").alias('isRevenueVATExists')\
          ,expr("cast(max(CASE WHEN (crbx12.isTradeAR = True OR crbx12.isOtherAR = True) THEN 1 ELSE 0 END) as BOOLEAN)").alias('isARExists')\
          ,expr("cast(max(CASE WHEN (crbx12.isTradeAP = True OR crbx12.isOtherAR = True) THEN 1 ELSE 0 END) as BOOLEAN)").alias('isAPExists')\
          ,expr("cast(\
					CASE WHEN max(CASE WHEN (crbx12.isTradeAR = True OR crbx12.isOtherAR = True) THEN 1 ELSE 0 END) = 1\
					AND max(CASE WHEN (crbx12.isTradeAR = True OR crbx12.isOtherAR = True) AND crbx12.clearingBoxGLClearingPostingDate = '1900-01-01' \
					THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END as BOOLEAN\
			)").alias('isOpenARExists')\
          ,expr("cast(\
					CASE WHEN max(CASE WHEN (crbx12.isTradeAP = True OR crbx12.isOtherAP = True) THEN 1 ELSE 0 END) = 1\
					AND max(CASE WHEN (crbx12.isTradeAP = True OR crbx12.isOtherAP = True) AND crbx12.clearingBoxGLClearingPostingDate = '1900-01-01' \
					THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END as BOOLEAN\
			)").alias('isOpenAPExists')\
          ,expr("cast(max(cast(crbx12.isOpenItemPY as INT)) as BOOLEAN)").alias('isOpenItemPY')\
          ,expr("cast(max(cast(crbx12.isOpenItemYE as INT)) as BOOLEAN)").alias('isOpenItemYE')\
          )

      gen_L1_STG_ClearingBox_12_RevenueByDocument = objDataTransformation.gen_convertToCDMandCache\
              (gen_L1_STG_ClearingBox_12_RevenueByDocument,'gen','L1_STG_ClearingBox_12_RevenueByDocument',False)

      gen_L1_STG_ClearingBox_12_AllJournal = gen_L1_STG_ClearingBox_12_AllJournal.alias('crbx12')\
          .join(gen_L1_STG_ClearingBox_12_RevenueByDocument.alias('crbd12'),\
          col('crbx12.idGLJE') == col('crbd12.idGLJE'), how = 'left')\
          .select(\
          col('crbx12.idGroupChain').alias('idGroupChain')\
          ,col('crbx12.chainInterference').alias('chainInterference')\
          ,col('crbx12.idClearingTypeChain').alias('idClearingTypeChain')\
          ,col('crbx12.typeGroupChain').alias('typeGroupChain')\
          ,col('crbx12.typeChain').alias('typeChain')\
          ,col('crbx12.idClearingTypeLineItem').alias('idClearingTypeLineItem')\
          ,col('crbx12.typeGroupLineItem').alias('typeGroupLineItem')\
          ,col('crbx12.typeLineItem').alias('typeLineItem')\
          ,col('crbx12.idGLJE').alias('idGLJE')\
          ,col('crbx12.idGLJELine').alias('idGLJELine')\
          ,col('crbx12.clearingBoxClientERP').alias('clearingBoxClientERP')\
          ,col('crbx12.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
          ,col('crbx12.clearingBoxGLFiscalYear').alias('clearingBoxGLFiscalYear')\
          ,col('crbx12.clearingBoxGLFinancialPeriod').alias('clearingBoxGLFinancialPeriod')\
          ,col('crbx12.clearingBoxGLPostingDate').alias('clearingBoxGLPostingDate')\
          ,col('crbx12.clearingBoxGLClearingDocument').alias('clearingBoxGLClearingDocument')\
          ,col('crbx12.clearingBoxGLClearingPostingDate').alias('clearingBoxGLClearingPostingDate')\
          ,col('crbx12.clearingBoxGLJEDocument').alias('clearingBoxGLJEDocument')\
          ,col('crbx12.clearingBoxGLJELineNumber').alias('clearingBoxGLJELineNumber')\
          ,col('crbx12.clearingBoxGLAccountNumber').alias('clearingBoxGLAccountNumber')\
          ,col('crbx12.clearingBoxGLAccountDescription').alias('clearingBoxGLAccountDescription')\
          ,col('crbx12.clearingBoxGLAccountCategory').alias('clearingBoxGLAccountCategory')\
          ,col('crbx12.clearingBoxTrackAccountCategory').alias('clearingBoxTrackAccountCategory')\
          ,col('crbx12.clearingBoxGLAmountLC').alias('clearingBoxGLAmountLC')\
          ,col('crbx12.clearingBoxGLAmountRevenueLC').alias('clearingBoxGLAmountRevenueLC')\
          ,col('crbx12.clearingBoxAmountSign').alias('clearingBoxAmountSign')\
          ,col('crbx12.clearingBoxGLReferenceSLSource').alias('clearingBoxGLReferenceSLSource')\
          ,col('crbx12.clearingBoxSLDocumentNumber').alias('clearingBoxSLDocumentNumber')\
          ,col('crbx12.clearingBoxSLFiscalYear').alias('clearingBoxSLFiscalYear')\
          ,col('crbx12.clearingBoxSLDocumentTypeERP').alias('clearingBoxSLDocumentTypeERP')\
          ,col('crbx12.clearingBoxSLDocumentType').alias('clearingBoxSLDocumentType')\
          ,col('crbx12.clearingBoxGLReferenceDocument').alias('clearingBoxGLReferenceDocument')\
          ,col('crbx12.clearingBoxGLCustomerNumber').alias('clearingBoxGLCustomerNumber')\
          ,col('crbx12.clearingBoxGLVendorNumber').alias('clearingBoxGLVendorNumber')\
          ,col('crbx12.clearingBoxChainCustomerNumber').alias('clearingBoxChainCustomerNumber')\
          ,col('crbx12.clearingBoxChainVendorNumber').alias('clearingBoxChainVendorNumber')\
          ,col('crbx12.clearingBoxRatioRevenuePaid').alias('clearingBoxRatioRevenuePaid')\
          ,col('crbx12.isRevenueFullyReversed').alias('isRevenueFullyReversed')\
          ,col('crbx12.isOrigin').alias('isOrigin')\
          ,col('crbx12.isClearing').alias('isClearing')\
          ,col('crbx12.isRevenue').alias('isRevenue')\
          ,col('crbx12.isVAT').alias('isVAT')\
          ,col('crbx12.isTradeAR').alias('isTradeAR')\
          ,col('crbx12.isTradeAROpen').alias('isTradeAROpen')\
          ,col('crbx12.isTradeAPOpen').alias('isTradeAPOpen')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE')) &(col('crbx12.idGroupChain').isNull())),col('crbd12.isRevenueOpen'))\
          .otherwise(col('crbx12.isRevenueOpen')).alias('isRevenueOpen')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE')) &(col('crbx12.idGroupChain').isNull())),col('crbd12.isNotExistsTradeAR'))\
          .otherwise(col('crbx12.isNotExistsTradeAR')).alias('isNotExistsTradeAR')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE')) &(col('crbx12.idGroupChain').isNull())),col('crbd12.isARExists'))\
          .otherwise(col('crbx12.isARExists')).alias('isARExists')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE')) &(col('crbx12.idGroupChain').isNull())),col('crbd12.isOpenARExists'))\
          .otherwise(col('crbx12.isOpenARExists')).alias('isOpenARExists')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE')) &(col('crbx12.idGroupChain').isNull())),col('crbd12.isNotExistsTradeAP'))\
          .otherwise(col('crbx12.isNotExistsTradeAP')).alias('isNotExistsTradeAP')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE')) &(col('crbx12.idGroupChain').isNull())),col('crbd12.isAPExists'))\
          .otherwise(col('crbx12.isAPExists')).alias('isAPExists')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE')) &(col('crbx12.idGroupChain').isNull())),col('crbd12.isOpenAPExists'))\
          .otherwise(col('crbx12.isOpenAPExists')).alias('isOpenAPExists')\
          ,col('crbx12.isTradeAP').alias('isTradeAP')\
          ,col('crbx12.isOtherAR').alias('isOtherAR')\
          ,col('crbx12.isOtherAP').alias('isOtherAP')\
          ,col('crbx12.isBankCash').alias('isBankCash')\
          ,col('crbx12.isInterCompanyAccount').alias('isInterCompanyAccount')\
          ,col('crbx12.isReversedOrReversal').alias('isReversedOrReversal')\
          ,col('crbx12.isInTimeFrame').alias('isInTimeFrame')\
          ,col('crbx12.isRevisedByRevenue').alias('isRevisedByRevenue')\
          ,col('crbx12.isInterCompanyAccountChain').alias('isInterCompanyAccountChain')\
          ,col('crbx12.isMissingJE').alias('isMissingJE')\
          ,col('crbx12.isCurrencyFX').alias('isCurrencyFX')\
          ,col('crbx12.isMisinterpretedChain').alias('isMisinterpretedChain')\
          ,col('crbx12.isOpenItemPY').alias('isOpenItemPY')\
          ,col('crbx12.isOpenItemYE').alias('isOpenItemYE')\
          ,col('crbx12.clearingBoxReGrouping').alias('clearingBoxReGrouping')\
          ,col('crbx12.caseID').alias('caseID')\
          )

      gen_L1_STG_ClearingBox_12_RevenueByGroupChain = gen_L1_STG_ClearingBox_12_AllJournal.alias('crbx12')\
          .join (gen_L1_STG_ClearingBox_12_RevenueByDocument.alias('crbd12')\
          ,col('crbx12.idGLJE') == col('crbd12.idGLJE'), how = 'inner')\
          .filter(col('crbx12.idGroupChain').isNotNull())\
          .groupBy(col('crbx12.idGroupChain'))\
          .agg(\
          expr("cast(max(cast(crbd12.isRevenueOpen as INT)) as BOOLEAN)").alias('isRevenueOpen')\
          ,expr("cast(max(cast(crbd12.isNotExistsTradeAR as INT)) as BOOLEAN)").alias('isNotExistsTradeAR')\
          ,expr("cast(max(cast(crbd12.isARExists as INT)) as BOOLEAN)").alias('isARExists')\
          ,expr("cast(max(cast(crbd12.isOpenARExists as INT)) as BOOLEAN)").alias('isOpenARExists')\
          ,expr("cast(max(cast(crbd12.isNotExistsTradeAP as INT)) as BOOLEAN)").alias('isNotExistsTradeAP')\
          ,expr("cast(max(cast(crbd12.isAPExists as INT)) as BOOLEAN)").alias('isAPExists')\
          ,expr("cast(max(cast(crbd12.isOpenAPExists as INT)) as BOOLEAN)").alias('isOpenAPExists'))

      gen_L1_STG_ClearingBox_12_RevenueByGroupChain = objDataTransformation.gen_convertToCDMandCache\
              (gen_L1_STG_ClearingBox_12_RevenueByGroupChain,'gen','L1_STG_ClearingBox_12_RevenueByGroupChain',False)

      gen_L1_STG_ClearingBox_12_AllJournal = gen_L1_STG_ClearingBox_12_AllJournal.alias('crbx12')\
          .join(gen_L1_STG_ClearingBox_12_RevenueByGroupChain.alias('crgc12'),\
          col('crbx12.idGroupChain') == col('crgc12.idGroupChain'), how = 'left')\
          .select(\
          col('crbx12.idGroupChain').alias('idGroupChain')\
          ,col('crbx12.chainInterference').alias('chainInterference')\
          ,col('crbx12.idClearingTypeChain').alias('idClearingTypeChain')\
          ,col('crbx12.typeGroupChain').alias('typeGroupChain')\
          ,col('crbx12.typeChain').alias('typeChain')\
          ,col('crbx12.idClearingTypeLineItem').alias('idClearingTypeLineItem')\
          ,col('crbx12.typeGroupLineItem').alias('typeGroupLineItem')\
          ,col('crbx12.typeLineItem').alias('typeLineItem')\
          ,col('crbx12.idGLJE').alias('idGLJE')\
          ,col('crbx12.idGLJELine').alias('idGLJELine')\
          ,col('crbx12.clearingBoxClientERP').alias('clearingBoxClientERP')\
          ,col('crbx12.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
          ,col('crbx12.clearingBoxGLFiscalYear').alias('clearingBoxGLFiscalYear')\
          ,col('crbx12.clearingBoxGLFinancialPeriod').alias('clearingBoxGLFinancialPeriod')\
          ,col('crbx12.clearingBoxGLPostingDate').alias('clearingBoxGLPostingDate')\
          ,col('crbx12.clearingBoxGLClearingDocument').alias('clearingBoxGLClearingDocument')\
          ,col('crbx12.clearingBoxGLClearingPostingDate').alias('clearingBoxGLClearingPostingDate')\
          ,col('crbx12.clearingBoxGLJEDocument').alias('clearingBoxGLJEDocument')\
          ,col('crbx12.clearingBoxGLJELineNumber').alias('clearingBoxGLJELineNumber')\
          ,col('crbx12.clearingBoxGLAccountNumber').alias('clearingBoxGLAccountNumber')\
          ,col('crbx12.clearingBoxGLAccountDescription').alias('clearingBoxGLAccountDescription')\
          ,col('crbx12.clearingBoxGLAccountCategory').alias('clearingBoxGLAccountCategory')\
          ,col('crbx12.clearingBoxTrackAccountCategory').alias('clearingBoxTrackAccountCategory')\
          ,col('crbx12.clearingBoxGLAmountLC').alias('clearingBoxGLAmountLC')\
          ,col('crbx12.clearingBoxGLAmountRevenueLC').alias('clearingBoxGLAmountRevenueLC')\
          ,col('crbx12.clearingBoxAmountSign').alias('clearingBoxAmountSign')\
          ,col('crbx12.clearingBoxGLReferenceSLSource').alias('clearingBoxGLReferenceSLSource')\
          ,col('crbx12.clearingBoxSLDocumentNumber').alias('clearingBoxSLDocumentNumber')\
          ,col('crbx12.clearingBoxSLFiscalYear').alias('clearingBoxSLFiscalYear')\
          ,col('crbx12.clearingBoxSLDocumentTypeERP').alias('clearingBoxSLDocumentTypeERP')\
          ,col('crbx12.clearingBoxSLDocumentType').alias('clearingBoxSLDocumentType')\
          ,col('crbx12.clearingBoxGLReferenceDocument').alias('clearingBoxGLReferenceDocument')\
          ,col('crbx12.clearingBoxGLCustomerNumber').alias('clearingBoxGLCustomerNumber')\
          ,col('crbx12.clearingBoxGLVendorNumber').alias('clearingBoxGLVendorNumber')\
          ,col('crbx12.clearingBoxChainCustomerNumber').alias('clearingBoxChainCustomerNumber')\
          ,col('crbx12.clearingBoxChainVendorNumber').alias('clearingBoxChainVendorNumber')\
          ,col('crbx12.clearingBoxRatioRevenuePaid').alias('clearingBoxRatioRevenuePaid')\
          ,col('crbx12.isRevenueFullyReversed').alias('isRevenueFullyReversed')\
          ,col('crbx12.isOrigin').alias('isOrigin')\
          ,col('crbx12.isClearing').alias('isClearing')\
          ,col('crbx12.isRevenue').alias('isRevenue')\
          ,col('crbx12.isVAT').alias('isVAT')\
          ,col('crbx12.isTradeAR').alias('isTradeAR')\
          ,col('crbx12.isTradeAROpen').alias('isTradeAROpen')\
          ,col('crbx12.isTradeAPOpen').alias('isTradeAPOpen')\
          ,when((col('crbx12.idGroupChain') == col('crgc12.idGroupChain')),col('crgc12.isRevenueOpen'))\
          .otherwise(col('crbx12.isRevenueOpen')).alias('isRevenueOpen')\
          ,when((col('crbx12.idGroupChain') == col('crgc12.idGroupChain')),col('crgc12.isNotExistsTradeAR'))\
          .otherwise(col('crbx12.isNotExistsTradeAR')).alias('isNotExistsTradeAR')\
          ,when((col('crbx12.idGroupChain') == col('crgc12.idGroupChain')),col('crgc12.isARExists'))\
          .otherwise(col('crbx12.isARExists')).alias('isARExists')\
          ,when((col('crbx12.idGroupChain') == col('crgc12.idGroupChain')),col('crgc12.isOpenARExists'))\
          .otherwise(col('crbx12.isOpenARExists')).alias('isOpenARExists')\
          ,when((col('crbx12.idGroupChain') == col('crgc12.idGroupChain')),col('crgc12.isNotExistsTradeAP'))\
          .otherwise(col('crbx12.isNotExistsTradeAP')).alias('isNotExistsTradeAP')\
          ,when((col('crbx12.idGroupChain') == col('crgc12.idGroupChain')),col('crgc12.isAPExists'))\
          .otherwise(col('crbx12.isAPExists')).alias('isAPExists')\
          ,when((col('crbx12.idGroupChain') == col('crgc12.idGroupChain')),col('crgc12.isOpenAPExists'))\
          .otherwise(col('crbx12.isOpenAPExists')).alias('isOpenAPExists')\
          ,col('crbx12.isTradeAP').alias('isTradeAP')\
          ,col('crbx12.isOtherAR').alias('isOtherAR')\
          ,col('crbx12.isOtherAP').alias('isOtherAP')\
          ,col('crbx12.isBankCash').alias('isBankCash')\
          ,col('crbx12.isInterCompanyAccount').alias('isInterCompanyAccount')\
          ,col('crbx12.isReversedOrReversal').alias('isReversedOrReversal')\
          ,col('crbx12.isInTimeFrame').alias('isInTimeFrame')\
          ,col('crbx12.isRevisedByRevenue').alias('isRevisedByRevenue')\
          ,col('crbx12.isInterCompanyAccountChain').alias('isInterCompanyAccountChain')\
          ,col('crbx12.isMissingJE').alias('isMissingJE')\
          ,col('crbx12.isCurrencyFX').alias('isCurrencyFX')\
          ,col('crbx12.isMisinterpretedChain').alias('isMisinterpretedChain')\
          ,col('crbx12.isOpenItemPY').alias('isOpenItemPY')\
          ,col('crbx12.isOpenItemYE').alias('isOpenItemYE')\
          ,col('crbx12.clearingBoxReGrouping').alias('clearingBoxReGrouping')\
          ,col('crbx12.caseID').alias('caseID')\
          )


      chainGroupTypeID = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_open_arap'))\
           .select(col('lkctc.ID')).collect()[0][0]
      chainGroupTypeGroup = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_open_arap'))\
           .select(col('lkctc.typeGroup')).collect()[0][0]
      chainGroupType = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_open_arap'))\
           .select(col('lkctc.typeChain')).collect()[0][0]

      gen_L1_STG_ClearingBox_12_AllJournal = gen_L1_STG_ClearingBox_12_AllJournal.alias('crbx12')\
          .join(gen_L1_STG_ClearingBox_12_RevenueByDocument.alias('crbd12'),\
          ((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isRevenueOpen') == True)),how = 'left')\
          .select(\
          col('crbx12.idGroupChain').alias('idGroupChain')\
          ,col('crbx12.chainInterference').alias('chainInterference')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isRevenueOpen') == True)),lit(chainGroupTypeID))\
          .otherwise(col('crbx12.idClearingTypeChain')).alias('idClearingTypeChain')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isRevenueOpen') == True)),lit(chainGroupTypeGroup))\
          .otherwise(col('crbx12.typeGroupChain')).alias('typeGroupChain')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isRevenueOpen') == True)),lit(chainGroupType))\
          .otherwise(col('crbx12.typeChain')).alias('typeChain')\
          ,col('crbx12.idClearingTypeLineItem').alias('idClearingTypeLineItem')\
          ,col('crbx12.typeGroupLineItem').alias('typeGroupLineItem')\
          ,col('crbx12.typeLineItem').alias('typeLineItem')\
          ,col('crbx12.idGLJE').alias('idGLJE')\
          ,col('crbx12.idGLJELine').alias('idGLJELine')\
          ,col('crbx12.clearingBoxClientERP').alias('clearingBoxClientERP')\
          ,col('crbx12.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
          ,col('crbx12.clearingBoxGLFiscalYear').alias('clearingBoxGLFiscalYear')\
          ,col('crbx12.clearingBoxGLFinancialPeriod').alias('clearingBoxGLFinancialPeriod')\
          ,col('crbx12.clearingBoxGLPostingDate').alias('clearingBoxGLPostingDate')\
          ,col('crbx12.clearingBoxGLClearingDocument').alias('clearingBoxGLClearingDocument')\
          ,col('crbx12.clearingBoxGLClearingPostingDate').alias('clearingBoxGLClearingPostingDate')\
          ,col('crbx12.clearingBoxGLJEDocument').alias('clearingBoxGLJEDocument')\
          ,col('crbx12.clearingBoxGLJELineNumber').alias('clearingBoxGLJELineNumber')\
          ,col('crbx12.clearingBoxGLAccountNumber').alias('clearingBoxGLAccountNumber')\
          ,col('crbx12.clearingBoxGLAccountDescription').alias('clearingBoxGLAccountDescription')\
          ,col('crbx12.clearingBoxGLAccountCategory').alias('clearingBoxGLAccountCategory')\
          ,col('crbx12.clearingBoxTrackAccountCategory').alias('clearingBoxTrackAccountCategory')\
          ,col('crbx12.clearingBoxGLAmountLC').alias('clearingBoxGLAmountLC')\
          ,col('crbx12.clearingBoxGLAmountRevenueLC').alias('clearingBoxGLAmountRevenueLC')\
          ,col('crbx12.clearingBoxAmountSign').alias('clearingBoxAmountSign')\
          ,col('crbx12.clearingBoxGLReferenceSLSource').alias('clearingBoxGLReferenceSLSource')\
          ,col('crbx12.clearingBoxSLDocumentNumber').alias('clearingBoxSLDocumentNumber')\
          ,col('crbx12.clearingBoxSLFiscalYear').alias('clearingBoxSLFiscalYear')\
          ,col('crbx12.clearingBoxSLDocumentTypeERP').alias('clearingBoxSLDocumentTypeERP')\
          ,col('crbx12.clearingBoxSLDocumentType').alias('clearingBoxSLDocumentType')\
          ,col('crbx12.clearingBoxGLReferenceDocument').alias('clearingBoxGLReferenceDocument')\
          ,col('crbx12.clearingBoxGLCustomerNumber').alias('clearingBoxGLCustomerNumber')\
          ,col('crbx12.clearingBoxGLVendorNumber').alias('clearingBoxGLVendorNumber')\
          ,col('crbx12.clearingBoxChainCustomerNumber').alias('clearingBoxChainCustomerNumber')\
          ,col('crbx12.clearingBoxChainVendorNumber').alias('clearingBoxChainVendorNumber')\
          ,col('crbx12.clearingBoxRatioRevenuePaid').alias('clearingBoxRatioRevenuePaid')\
          ,col('crbx12.isRevenueFullyReversed').alias('isRevenueFullyReversed')\
          ,col('crbx12.isOrigin').alias('isOrigin')\
          ,col('crbx12.isClearing').alias('isClearing')\
          ,col('crbx12.isRevenue').alias('isRevenue')\
          ,col('crbx12.isVAT').alias('isVAT')\
          ,col('crbx12.isTradeAR').alias('isTradeAR')\
          ,col('crbx12.isTradeAROpen').alias('isTradeAROpen')\
          ,col('crbx12.isTradeAPOpen').alias('isTradeAPOpen')\
          ,col('crbx12.isRevenueOpen').alias('isRevenueOpen')\
          ,col('crbx12.isNotExistsTradeAR').alias('isNotExistsTradeAR')\
          ,col('crbx12.isARExists').alias('isARExists')\
          ,col('crbx12.isOpenARExists').alias('isOpenARExists')\
          ,col('crbx12.isNotExistsTradeAP').alias('isNotExistsTradeAP')\
          ,col('crbx12.isAPExists').alias('isAPExists')\
          ,col('crbx12.isOpenAPExists').alias('isOpenAPExists')\
          ,col('crbx12.isTradeAP').alias('isTradeAP')\
          ,col('crbx12.isOtherAR').alias('isOtherAR')\
          ,col('crbx12.isOtherAP').alias('isOtherAP')\
          ,col('crbx12.isBankCash').alias('isBankCash')\
          ,col('crbx12.isInterCompanyAccount').alias('isInterCompanyAccount')\
          ,col('crbx12.isReversedOrReversal').alias('isReversedOrReversal')\
          ,col('crbx12.isInTimeFrame').alias('isInTimeFrame')\
          ,col('crbx12.isRevisedByRevenue').alias('isRevisedByRevenue')\
          ,col('crbx12.isInterCompanyAccountChain').alias('isInterCompanyAccountChain')\
          ,col('crbx12.isMissingJE').alias('isMissingJE')\
          ,col('crbx12.isCurrencyFX').alias('isCurrencyFX')\
          ,col('crbx12.isMisinterpretedChain').alias('isMisinterpretedChain')\
          ,col('crbx12.isOpenItemPY').alias('isOpenItemPY')\
          ,col('crbx12.isOpenItemYE').alias('isOpenItemYE')\
          ,col('crbx12.clearingBoxReGrouping').alias('clearingBoxReGrouping')\
          ,col('crbx12.caseID').alias('caseID')\
          )

      chainGroupTypeID = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_without_arap'))\
           .select(col('lkctc.ID')).collect()[0][0]
      chainGroupTypeGroup = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_without_arap'))\
           .select(col('lkctc.typeGroup')).collect()[0][0]
      chainGroupType = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_without_arap'))\
           .select(col('lkctc.typeChain')).collect()[0][0]

      gen_L1_STG_ClearingBox_12_AllJournal = gen_L1_STG_ClearingBox_12_AllJournal.alias('crbx12')\
          .join(gen_L1_STG_ClearingBox_12_RevenueByDocument.alias('crbd12'),\
          ((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isARExists') == False)\
          & (col('crbd12.isAPExists') == False)),how = 'left')\
          .select(\
          col('crbx12.idGroupChain').alias('idGroupChain')\
          ,col('crbx12.chainInterference').alias('chainInterference')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isARExists') == False)\
          & (col('crbd12.isAPExists') == False)),lit(chainGroupTypeID))\
          .otherwise(col('crbx12.idClearingTypeChain')).alias('idClearingTypeChain')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isARExists') == False)\
          & (col('crbd12.isAPExists') == False)),lit(chainGroupTypeGroup))\
          .otherwise(col('crbx12.typeGroupChain')).alias('typeGroupChain')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) \
          & (col('crbd12.isARExists') == False)\
          & (col('crbd12.isAPExists') == False)),lit(chainGroupType))\
          .otherwise(col('crbx12.typeChain')).alias('typeChain')\
          ,col('crbx12.idClearingTypeLineItem').alias('idClearingTypeLineItem')\
          ,col('crbx12.typeGroupLineItem').alias('typeGroupLineItem')\
          ,col('crbx12.typeLineItem').alias('typeLineItem')\
          ,col('crbx12.idGLJE').alias('idGLJE')\
          ,col('crbx12.idGLJELine').alias('idGLJELine')\
          ,col('crbx12.clearingBoxClientERP').alias('clearingBoxClientERP')\
          ,col('crbx12.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
          ,col('crbx12.clearingBoxGLFiscalYear').alias('clearingBoxGLFiscalYear')\
          ,col('crbx12.clearingBoxGLFinancialPeriod').alias('clearingBoxGLFinancialPeriod')\
          ,col('crbx12.clearingBoxGLPostingDate').alias('clearingBoxGLPostingDate')\
          ,col('crbx12.clearingBoxGLClearingDocument').alias('clearingBoxGLClearingDocument')\
          ,col('crbx12.clearingBoxGLClearingPostingDate').alias('clearingBoxGLClearingPostingDate')\
          ,col('crbx12.clearingBoxGLJEDocument').alias('clearingBoxGLJEDocument')\
          ,col('crbx12.clearingBoxGLJELineNumber').alias('clearingBoxGLJELineNumber')\
          ,col('crbx12.clearingBoxGLAccountNumber').alias('clearingBoxGLAccountNumber')\
          ,col('crbx12.clearingBoxGLAccountDescription').alias('clearingBoxGLAccountDescription')\
          ,col('crbx12.clearingBoxGLAccountCategory').alias('clearingBoxGLAccountCategory')\
          ,col('crbx12.clearingBoxTrackAccountCategory').alias('clearingBoxTrackAccountCategory')\
          ,col('crbx12.clearingBoxGLAmountLC').alias('clearingBoxGLAmountLC')\
          ,col('crbx12.clearingBoxGLAmountRevenueLC').alias('clearingBoxGLAmountRevenueLC')\
          ,col('crbx12.clearingBoxAmountSign').alias('clearingBoxAmountSign')\
          ,col('crbx12.clearingBoxGLReferenceSLSource').alias('clearingBoxGLReferenceSLSource')\
          ,col('crbx12.clearingBoxSLDocumentNumber').alias('clearingBoxSLDocumentNumber')\
          ,col('crbx12.clearingBoxSLFiscalYear').alias('clearingBoxSLFiscalYear')\
          ,col('crbx12.clearingBoxSLDocumentTypeERP').alias('clearingBoxSLDocumentTypeERP')\
          ,col('crbx12.clearingBoxSLDocumentType').alias('clearingBoxSLDocumentType')\
          ,col('crbx12.clearingBoxGLReferenceDocument').alias('clearingBoxGLReferenceDocument')\
          ,col('crbx12.clearingBoxGLCustomerNumber').alias('clearingBoxGLCustomerNumber')\
          ,col('crbx12.clearingBoxGLVendorNumber').alias('clearingBoxGLVendorNumber')\
          ,col('crbx12.clearingBoxChainCustomerNumber').alias('clearingBoxChainCustomerNumber')\
          ,col('crbx12.clearingBoxChainVendorNumber').alias('clearingBoxChainVendorNumber')\
          ,col('crbx12.clearingBoxRatioRevenuePaid').alias('clearingBoxRatioRevenuePaid')\
          ,col('crbx12.isRevenueFullyReversed').alias('isRevenueFullyReversed')\
          ,col('crbx12.isOrigin').alias('isOrigin')\
          ,col('crbx12.isClearing').alias('isClearing')\
          ,col('crbx12.isRevenue').alias('isRevenue')\
          ,col('crbx12.isVAT').alias('isVAT')\
          ,col('crbx12.isTradeAR').alias('isTradeAR')\
          ,col('crbx12.isTradeAROpen').alias('isTradeAROpen')\
          ,col('crbx12.isTradeAPOpen').alias('isTradeAPOpen')\
          ,col('crbx12.isRevenueOpen').alias('isRevenueOpen')\
          ,col('crbx12.isNotExistsTradeAR').alias('isNotExistsTradeAR')\
          ,col('crbx12.isARExists').alias('isARExists')\
          ,col('crbx12.isOpenARExists').alias('isOpenARExists')\
          ,col('crbx12.isNotExistsTradeAP').alias('isNotExistsTradeAP')\
          ,col('crbx12.isAPExists').alias('isAPExists')\
          ,col('crbx12.isOpenAPExists').alias('isOpenAPExists')\
          ,col('crbx12.isTradeAP').alias('isTradeAP')\
          ,col('crbx12.isOtherAR').alias('isOtherAR')\
          ,col('crbx12.isOtherAP').alias('isOtherAP')\
          ,col('crbx12.isBankCash').alias('isBankCash')\
          ,col('crbx12.isInterCompanyAccount').alias('isInterCompanyAccount')\
          ,col('crbx12.isReversedOrReversal').alias('isReversedOrReversal')\
          ,col('crbx12.isInTimeFrame').alias('isInTimeFrame')\
          ,col('crbx12.isRevisedByRevenue').alias('isRevisedByRevenue')\
          ,col('crbx12.isInterCompanyAccountChain').alias('isInterCompanyAccountChain')\
          ,col('crbx12.isMissingJE').alias('isMissingJE')\
          ,col('crbx12.isCurrencyFX').alias('isCurrencyFX')\
          ,col('crbx12.isMisinterpretedChain').alias('isMisinterpretedChain')\
          ,col('crbx12.isOpenItemPY').alias('isOpenItemPY')\
          ,col('crbx12.isOpenItemYE').alias('isOpenItemYE')\
          ,col('crbx12.clearingBoxReGrouping').alias('clearingBoxReGrouping')\
          ,col('crbx12.caseID').alias('caseID')\
          )

      chainGroupTypeID = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_other'))\
           .select(col('lkctc.ID')).collect()[0][0]
      chainGroupTypeGroup = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_other'))\
           .select(col('lkctc.typeGroup')).collect()[0][0]
      chainGroupType = knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc').filter(col('lkctc.typeChain')==lit('revenue_EQ_other'))\
           .select(col('lkctc.typeChain')).collect()[0][0]

      gen_L1_STG_ClearingBox_12_AllJournal = gen_L1_STG_ClearingBox_12_AllJournal.alias('crbx12')\
          .join(gen_L1_STG_ClearingBox_12_RevenueByDocument.alias('crbd12'),\
          ((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True) ),how = 'left')\
          .select(\
          col('crbx12.idGroupChain').alias('idGroupChain')\
          ,col('crbx12.chainInterference').alias('chainInterference')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True)),lit(chainGroupTypeID))\
          .otherwise(col('crbx12.idClearingTypeChain')).alias('idClearingTypeChain')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True)),lit(chainGroupTypeGroup))\
          .otherwise(col('crbx12.typeGroupChain')).alias('typeGroupChain')\
          ,when(((col('crbx12.idGLJE') == col('crbd12.idGLJE'))\
          & (col('crbx12.idGroupChain').isNull())\
          & (col('crbx12.typeChain').isNull())\
          & (col('crbd12.isRevenue') == True)),lit(chainGroupType))\
          .otherwise(col('crbx12.typeChain')).alias('typeChain')\
          ,col('crbx12.idClearingTypeLineItem').alias('idClearingTypeLineItem')\
          ,col('crbx12.typeGroupLineItem').alias('typeGroupLineItem')\
          ,col('crbx12.typeLineItem').alias('typeLineItem')\
          ,col('crbx12.idGLJE').alias('idGLJE')\
          ,col('crbx12.idGLJELine').alias('idGLJELine')\
          ,col('crbx12.clearingBoxClientERP').alias('clearingBoxClientERP')\
          ,col('crbx12.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
          ,col('crbx12.clearingBoxGLFiscalYear').alias('clearingBoxGLFiscalYear')\
          ,col('crbx12.clearingBoxGLFinancialPeriod').alias('clearingBoxGLFinancialPeriod')\
          ,col('crbx12.clearingBoxGLPostingDate').alias('clearingBoxGLPostingDate')\
          ,col('crbx12.clearingBoxGLClearingDocument').alias('clearingBoxGLClearingDocument')\
          ,col('crbx12.clearingBoxGLClearingPostingDate').alias('clearingBoxGLClearingPostingDate')\
          ,col('crbx12.clearingBoxGLJEDocument').alias('clearingBoxGLJEDocument')\
          ,col('crbx12.clearingBoxGLJELineNumber').alias('clearingBoxGLJELineNumber')\
          ,col('crbx12.clearingBoxGLAccountNumber').alias('clearingBoxGLAccountNumber')\
          ,col('crbx12.clearingBoxGLAccountDescription').alias('clearingBoxGLAccountDescription')\
          ,col('crbx12.clearingBoxGLAccountCategory').alias('clearingBoxGLAccountCategory')\
          ,col('crbx12.clearingBoxTrackAccountCategory').alias('clearingBoxTrackAccountCategory')\
          ,col('crbx12.clearingBoxGLAmountLC').alias('clearingBoxGLAmountLC')\
          ,col('crbx12.clearingBoxGLAmountRevenueLC').alias('clearingBoxGLAmountRevenueLC')\
          ,col('crbx12.clearingBoxAmountSign').alias('clearingBoxAmountSign')\
          ,col('crbx12.clearingBoxGLReferenceSLSource').alias('clearingBoxGLReferenceSLSource')\
          ,col('crbx12.clearingBoxSLDocumentNumber').alias('clearingBoxSLDocumentNumber')\
          ,col('crbx12.clearingBoxSLFiscalYear').alias('clearingBoxSLFiscalYear')\
          ,col('crbx12.clearingBoxSLDocumentTypeERP').alias('clearingBoxSLDocumentTypeERP')\
          ,col('crbx12.clearingBoxSLDocumentType').alias('clearingBoxSLDocumentType')\
          ,col('crbx12.clearingBoxGLReferenceDocument').alias('clearingBoxGLReferenceDocument')\
          ,col('crbx12.clearingBoxGLCustomerNumber').alias('clearingBoxGLCustomerNumber')\
          ,col('crbx12.clearingBoxGLVendorNumber').alias('clearingBoxGLVendorNumber')\
          ,col('crbx12.clearingBoxChainCustomerNumber').alias('clearingBoxChainCustomerNumber')\
          ,col('crbx12.clearingBoxChainVendorNumber').alias('clearingBoxChainVendorNumber')\
          ,col('crbx12.clearingBoxRatioRevenuePaid').alias('clearingBoxRatioRevenuePaid')\
          ,col('crbx12.isRevenueFullyReversed').alias('isRevenueFullyReversed')\
          ,col('crbx12.isOrigin').alias('isOrigin')\
          ,col('crbx12.isClearing').alias('isClearing')\
          ,col('crbx12.isRevenue').alias('isRevenue')\
          ,col('crbx12.isVAT').alias('isVAT')\
          ,col('crbx12.isTradeAR').alias('isTradeAR')\
          ,col('crbx12.isTradeAROpen').alias('isTradeAROpen')\
          ,col('crbx12.isTradeAPOpen').alias('isTradeAPOpen')\
          ,col('crbx12.isRevenueOpen').alias('isRevenueOpen')\
          ,col('crbx12.isNotExistsTradeAR').alias('isNotExistsTradeAR')\
          ,col('crbx12.isARExists').alias('isARExists')\
          ,col('crbx12.isOpenARExists').alias('isOpenARExists')\
          ,col('crbx12.isNotExistsTradeAP').alias('isNotExistsTradeAP')\
          ,col('crbx12.isAPExists').alias('isAPExists')\
          ,col('crbx12.isOpenAPExists').alias('isOpenAPExists')\
          ,col('crbx12.isTradeAP').alias('isTradeAP')\
          ,col('crbx12.isOtherAR').alias('isOtherAR')\
          ,col('crbx12.isOtherAP').alias('isOtherAP')\
          ,col('crbx12.isBankCash').alias('isBankCash')\
          ,col('crbx12.isInterCompanyAccount').alias('isInterCompanyAccount')\
          ,col('crbx12.isReversedOrReversal').alias('isReversedOrReversal')\
          ,col('crbx12.isInTimeFrame').alias('isInTimeFrame')\
          ,col('crbx12.isRevisedByRevenue').alias('isRevisedByRevenue')\
          ,col('crbx12.isInterCompanyAccountChain').alias('isInterCompanyAccountChain')\
          ,col('crbx12.isMissingJE').alias('isMissingJE')\
          ,col('crbx12.isCurrencyFX').alias('isCurrencyFX')\
          ,col('crbx12.isMisinterpretedChain').alias('isMisinterpretedChain')\
          ,col('crbx12.isOpenItemPY').alias('isOpenItemPY')\
          ,col('crbx12.isOpenItemYE').alias('isOpenItemYE')\
          ,col('crbx12.clearingBoxReGrouping').alias('clearingBoxReGrouping')\
          ,col('crbx12.caseID').alias('caseID')\
          )

      gen_L1_STG_ClearingBox_12_AllJournal = objDataTransformation.gen_convertToCDMandCache\
              (gen_L1_STG_ClearingBox_12_AllJournal,'gen','L1_STG_ClearingBox_12_AllJournal',False)

      executionStatus = " gen_L1_STG_ClearingBox_12_AllJournal populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  
