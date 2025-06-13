# Databricks notebook source
def gen_L1_STG_ClearingBox_07_InterpretPreparation_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L1_STG_ClearingBox_07_ChainInterpreted

      gen_L1_STG_ClearingBox_07_ChainInterpreted = gen_L1_STG_ClearingBox_06_ChainInspected.alias('crbx6')\
          .select(col('crbx6.idGroupChain').alias('idGroupChain')\
                  ,col('crbx6.chainInterference').alias('chainInterference')\
                  ,lit(None).alias('idClearingTypeChain')\
                  ,lit(None).alias('typeGroupChain')\
                  ,lit(None).alias('typeChain')\
                  ,col('crbx6.clearingBoxClientERP').alias('clearingBoxClientERP')\
                  ,col('crbx6.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
                  ,when(col('crbx6.amountInvoice').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountInvoice')).alias('amountInvoice')\
                  ,when(col('crbx6.amountCreditMemo').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountCreditMemo')).alias('amountCreditMemo')\
                  ,when(col('crbx6.amountPrepayment').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountPrepayment')).alias('amountPrepayment')\
                  ,when(col('crbx6.amountAROtherClearing').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountAROtherClearing')).alias('amountAROtherClearing')\
                  ,when(col('crbx6.amountAPOtherClearing').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountAPOtherClearing')).alias('amountAPOtherClearing')\
                  ,when(col('crbx6.amountIncome').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountIncome')).alias('amountIncome')\
                  ,when(col('crbx6.amountInterest').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountInterest')).alias('amountInterest')\
                  ,when(col('crbx6.amountExpenseSGA').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountExpenseSGA')).alias('amountExpenseSGA')\
                  ,when(col('crbx6.amountCurrencyFX').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountCurrencyFX')).alias('amountCurrencyFX')\
                  ,when(col('crbx6.amountCash').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountCash')).alias('amountCash')\
                  ,when(col('crbx6.amountCashAdjustment').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountCashAdjustment')).alias('amountCashAdjustment')\
                  ,when(col('crbx6.amountRevenue').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountRevenue')).alias('amountRevenue')\
                  ,when(col('crbx6.amountRevenueIntime').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountRevenueIntime')).alias('amountRevenueIntime')\
                  ,when(col('crbx6.amountOtherIncomeExpense').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountOtherIncomeExpense')).alias('amountOtherIncomeExpense')\
                  ,when(col('crbx6.amountVat').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountVat')).alias('amountVAT')\
                  ,when(col('crbx6.amountAROther').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountAROther')).alias('amountAROther')\
                  ,when(col('crbx6.amountAPOther').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountAPOther')).alias('amountAPOther')\
                  ,when(col('crbx6.amountOtherClearing').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountOtherClearing')).alias('amountOtherClearing')\
                  ,when(col('crbx6.amountOtherPosting').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountOtherPosting')).alias('amountOtherPosting')\
                  ,when(col('crbx6.amountReversalClearing').isNull(),lit(0))\
                        .otherwise(col('crbx6.amountReversalClearing')).alias('amountReversalClearing')\
                  ,when(col('crbx6.isValidReversalClearing').isNull(),lit(0))\
                        .otherwise(col('crbx6.isValidReversalClearing')).alias('isValidReversalClearing')\
                  ,col('crbx6.countInvoiceSL').alias('countInvoiceSL')\
                  ,col('crbx6.countInvoiceGL').alias('countInvoiceGL')\
                  ,col('crbx6.countIncome').alias('countIncome')\
                  ,col('crbx6.countInterest').alias('countInterest')\
                  ,col('crbx6.countExpenseSga').alias('countExpenseSGA')\
                  ,col('crbx6.countCreditMemoSL').alias('countCreditMemoSL')\
                  ,col('crbx6.countCreditMemoGL').alias('countCreditMemoGL')\
                  ,col('crbx6.countAROtherClearing').alias('countAROtherClearing')\
                  ,col('crbx6.countAPOtherClearing').alias('countAPOtherClearing')\
                  ,col('crbx6.countRevenueInvoice').alias('countRevenueInvoice')\
                  ,col('crbx6.countRevenueCreditMemo').alias('countRevenueCreditMemo')\
                  ,col('crbx6.countRevenueGL').alias('countRevenueGL')\
                  ,col('crbx6.countOtherIncomeExpense').alias('countOtherIncomeExpense')\
                  ,col('crbx6.countCashDiscount').alias('countCashDiscount')\
                  ,col('crbx6.countBankCharge').alias('countBankCharge')\
                  ,col('crbx6.countCash').alias('countCash')\
                  ,col('crbx6.countBankStmt').alias('countBankStatement')\
                  ,col('crbx6.countChainInterference').alias('countChainInterference')\
                  ,when(round(col('crbx6.amountCash'),2) != lit(0),True)\
                        .otherwise(False).alias('isCashChain')\
                  ,when(round(col('crbx6.amountInvoice'),2) != lit(0),True)\
                        .otherwise(False).alias('isInvoiceChain')\
                  ,when(round(col('crbx6.amountCreditMemo'),2) != lit(0),True)\
                        .otherwise(False).alias('isCreditMemoChain')\
                  ,when(round(col('crbx6.amountAROtherClearing'),2) != lit(0),True)\
                        .otherwise(False).alias('isOtherTradeARChain')\
                  ,when(round(col('crbx6.amountAPOtherClearing'),2) != lit(0),True)\
                        .otherwise(False).alias('isOtherAPChain')\
                  ,when(round(col('crbx6.amountIncome'),2) != lit(0),True)\
                        .otherwise(False).alias('isOtherIncomeChain')\
                  ,when(round(col('crbx6.amountInterest'),2) != lit(0),True)\
                        .otherwise(False).alias('isOtherInterestChain')\
                  ,when(round(col('crbx6.amountExpenseSGA'),2) != lit(0),True)\
                        .otherwise(False).alias('isExpenseSGAChain')\
                  ,when(round(col('crbx6.amountCurrencyFX'),2) != lit(0),True)\
                        .otherwise(False).alias('isCurrencyFXChain')\
                  ,when(round(col('crbx6.amountPrepayment'),2) != lit(0),True)\
                        .otherwise(False).alias('isOtherPrepaymentChain')\
                  ,lit(False).alias('isMisinterpretedChain')\
                  ,col('crbx6.isRevenueExists').alias('isRevenueExists')\
                  ,col('crbx6.isARExists').alias('isARExists')\
                  ,col('crbx6.isOpenARExists').alias('isOpenARExists')\
                  ,col('crbx6.isAPExists').alias('isAPExists')\
                  ,col('crbx6.isOpenAPExists').alias('isOpenAPExists')\
                  )

      gen_L1_STG_ClearingBox_07_ChainInterpreted = objDataTransformation.gen_convertToCDMandCache\
              (gen_L1_STG_ClearingBox_07_ChainInterpreted,'gen','L1_STG_ClearingBox_07_ChainInterpreted',False)
                  
      executionStatus = "gen_L1_STG_ClearingBox_07_ChainInterpreted populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  
