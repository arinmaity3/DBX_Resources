# Databricks notebook source
def gen_L1_STG_ClearingBox_10_InterpretFinalization_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L1_STG_ClearingBox_07_ChainInterpreted

      gen_L1_STG_ClearingBox_07_ChainInterpreted = gen_L1_STG_ClearingBox_07_ChainInterpreted.alias('crbx7')\
          .withColumn("typeChain",expr("CASE \
                WHEN (crbx7.typeChain IS NULL AND crbx7.isCashChain = True) \
                THEN coalesce (( \
				CASE WHEN crbx7.isOtherIncomeChain = True AND round(crbx7.amountIncome - crbx7.amountCash , 2) = False\
				THEN 'income_EQ_cash' ELSE NULL \
				END\
				 )\
                 ,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isInvoiceChain = True AND \
					round(crbx7.amountIncome + crbx7.amountInvoice - crbx7.amountCash , 2) = False \
					THEN 'income_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'income_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'income_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND \
					round(crbx7.amountIncome - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing - crbx7.amountCash , 2) = False\
					THEN 'income_ar_other_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'income_ar_other_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'income_ar_other_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'income_ar_other_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_ar_other_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_ar_other_inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_ar_other_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_ar_other_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountIncome - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_inv_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_inv_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountIncome - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_inv_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_inv_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_inv_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_inv_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_inv_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'income_ar_other_inv_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND  \
					round(crbx7.amountCurrencyFX - crbx7.amountCash , 2) = False\
					THEN 'currency_fx_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_EQ_cash_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCurrencyFX , 2) = False\
					THEN 'cm_EQ_cash_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_cm_EQ_cash_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND  \
					round(crbx7.amountCurrencyFX - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'currency_fx_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_EQ_cash_adjustment_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountCurrencyFX , 2) = False\
					THEN 'cm_EQ_cash_adjustment_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_cm_EQ_cash_adjustment_currency_fx' ELSE NULL \
					END\
				 )\
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_EQ_cash_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_EQ_cash_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_cm_EQ_cash_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_cm_EQ_cash_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_EQ_cash_adjustment_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_EQ_cash_adjustment_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_cm_EQ_cash_adjustment_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_cm_EQ_cash_adjustment_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountCurrencyFX - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'currency_fx_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_EQ_cash_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'cm_EQ_cash_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_cm_EQ_cash_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountCurrencyFX - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'currency_fx_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_EQ_cash_adjustment_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'cm_EQ_cash_adjustment_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'inv_cm_EQ_cash_adjustment_ap_other_currency_fx' ELSE NULL \
					END\
				 )    \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_EQ_cash_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_EQ_cash_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_cm_EQ_cash_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing + crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_cm_EQ_cash_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_EQ_cash_adjustment_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_EQ_cash_adjustment_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_cm_EQ_cash_adjustment_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCurrencyFXChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing - crbx7.amountCurrencyFX , 2) = False\
					THEN 'ar_other_inv_cm_EQ_cash_adjustment_ap_other_currency_fx' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND  \
					round(crbx7.amountInterest - crbx7.amountCash , 2) = False\
					THEN 'interest_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'interest_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'interest_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'interest_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND  \
					round(crbx7.amountInterest - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing - crbx7.amountCash , 2) = False\
					THEN 'interest_ar_other_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'interest_ar_other_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash  , 2) = False\
					THEN 'interest_ar_other_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash  , 2) = False\
					THEN 'interest_ar_other_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_ar_other_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_ar_other_inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_ar_other_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'interest_ar_other_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountInterest - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_inv_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_inv_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountInterest - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_inv_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_inv_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_inv_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_inv_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_inv_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherInterestChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInterest + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'interest_ar_other_inv_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND  \
					round(crbx7.amountExpenseSGA - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND  \
					round(crbx7.amountExpenseSGA - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 ) \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_ar_other_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_ar_other_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_ar_other_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'expense_sga_ar_other_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_ar_other_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_ar_other_inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_ar_other_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_ar_other_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )\
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountExpenseSGA - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_inv_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_inv_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountExpenseSGA - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_inv_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_inv_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_inv_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_inv_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_inv_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'expense_sga_ar_other_inv_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND  \
					round(crbx7.amountPrepayment - crbx7.amountCash , 2) = False\
					THEN 'prepayment_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountPrepayment + crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'prepayment_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountPrepayment + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'prepayment_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountPrepayment + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'prepayment_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND  \
					round(crbx7.amountPrepayment - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'prepayment_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountPrepayment + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'prepayment_inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountPrepayment + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'prepayment_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherPrepaymentChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountPrepayment + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'prepayment_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND   \
					round(crbx7.amountInvoice - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND   \
					round(crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_cm_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'ar_other_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_ar_other_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountCreditMemo + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'cm_ar_other_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_cm_ar_other_EQ_cash_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND   \
					round(crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND   \
					round(crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_cm_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'ar_other_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_ar_other_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isCreditMemoChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountCreditMemo + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2 ) = False\
					THEN 'cm_ar_other_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherAPChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo + crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment - crbx7.amountAPOtherClearing , 2 ) = False\
					THEN 'inv_cm_ar_other_EQ_cash_adjustment_ap_other' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash , 2) = False\
					THEN 'ar_other_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'ar_other_inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'ar_other_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash , 2) = False\
					THEN 'ar_other_inv_cm_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountAROtherClearing - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False \
					THEN 'ar_other_EQ_cash_adjustment' ELSE NULL\
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'ar_other_inv_EQ_cash_adjustment' ELSE NULL END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'ar_other_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND crbx7.isInvoiceChain = True AND crbx7.isCreditMemoChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'ar_other_inv_cm_EQ_cash_adjustment' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice - crbx7.amountCash , 2) = False\
					THEN 'inv_EQ_cash' ELSE NULL \
					END\
				 )  \
				,(\
					CASE WHEN crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice - crbx7.amountCash - crbx7.amountCashAdjustment , 2) = False\
					THEN 'inv_EQ_cash_adjustment' ELSE NULL \
					END\
				 ),NULL)  \
                ELSE crbx7.typeChain \
                END").alias('typeChain'))

      gen_L1_STG_ClearingBox_07_ChainInterpreted = gen_L1_STG_ClearingBox_07_ChainInterpreted.alias('crbx7')\
          .withColumn("typeChain",expr("CASE \
                WHEN (crbx7.typeChain IS NULL AND crbx7.isCreditMemoChain = True) \
                THEN coalesce (( \
					CASE WHEN crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo , 2) = False\
					THEN 'inv_EQ_cm' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCashAdjustment , 2) = False\
					THEN 'inv_EQ_cm_adjustment' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isInvoiceChain = True AND crbx7.isOtherAPChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_EQ_cm_ap_other' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isInvoiceChain = True AND crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountInvoice + crbx7.amountAROtherClearing + crbx7.amountCreditMemo , 2) = False\
					THEN 'inv_ar_other_EQ_cm' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isInvoiceChain = True AND crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True  AND  \
					round(crbx7.amountInvoice + crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'inv_ar_other_EQ_cm_ap_other' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo , 2) = False\
					THEN 'ar_other_EQ_cm' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isOtherTradeARChain = True AND crbx7.isOtherAPChain = True  AND  \
					round(crbx7.amountAROtherClearing + crbx7.amountCreditMemo - crbx7.amountAPOtherClearing , 2) = False\
					THEN 'ar_other_EQ_cm_ap_other' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountCreditMemo , 2) = False\
					THEN 'income_EQ_cm' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountCreditMemo - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_EQ_cm_adjustment' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice + crbx7.amountCreditMemo , 2) = False \
					THEN 'income_inv_EQ_cm' ELSE NULL \
					END\
					)  \
				 ,(\
					CASE WHEN crbx7.isOtherIncomeChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountIncome + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCashAdjustment , 2) = False\
					THEN 'income_inv_EQ_cm_adjustment' ELSE NULL \
					END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountCreditMemo , 2) = False\
					THEN 'expense_sga_EQ_cm' ELSE NULL END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountCreditMemo - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_EQ_cm_adjustment' ELSE NULL END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice + crbx7.amountCreditMemo , 2) = False\
					THEN 'expense_sga_inv_EQ_cm' ELSE NULL END\
				  )  \
				 ,(\
					CASE WHEN crbx7.isExpenseSGAChain = True AND crbx7.isInvoiceChain = True AND  \
					round(crbx7.amountExpenseSGA + crbx7.amountInvoice + crbx7.amountCreditMemo - crbx7.amountCashAdjustment , 2) = False\
					THEN 'expense_sga_inv_EQ_cm_adjustment' ELSE NULL END\
				  ),NULL)  \
                ELSE crbx7.typeChain \
                END").alias('typeChain'))

      gen_L1_STG_ClearingBox_07_ChainInterpreted = gen_L1_STG_ClearingBox_07_ChainInterpreted.alias('crbx7')\
          .withColumn("typeChain",expr("CASE \
                WHEN (crbx7.typeChain IS NULL AND crbx7.isValidReversalClearing =1) \
                THEN coalesce ((\
					CASE WHEN (crbx7.isRevenueExists = True OR crbx7.countRevenueInvoice > 0 \
					OR crbx7.countRevenueCreditMemo > 0 OR crbx7.countRevenueGL > 0)\
					THEN 'reversal_revenue'	ELSE NULL \
					END\
				) \
				,'reversal_without_rev')  \
                ELSE crbx7.typeChain \
                END").alias('typeChain'))

      gen_L1_STG_ClearingBox_07_ChainInterpreted = gen_L1_STG_ClearingBox_07_ChainInterpreted.alias('crbx7')\
          .withColumn("typeChain",expr("CASE \
                WHEN (crbx7.typeChain IS NULL) \
                THEN coalesce ((\
						CASE WHEN (crbx7.isARExists = False AND crbx7.isAPExists = False) AND crbx7.isRevenueExists = True		\
						THEN 'revenue_EQ_without_arap' ELSE NULL \
						END\
					 )  \
					,(\
						CASE WHEN (crbx7.isARExists = False AND crbx7.isAPExists = False) AND crbx7.isInvoiceChain = True		\
						THEN 'inv_EQ_without_arap' ELSE NULL \
						END\
					 )  \
					,(\
						CASE WHEN (crbx7.isARExists = False AND crbx7.isAPExists = False) AND crbx7.isInvoiceChain = False		\
						THEN 'other_EQ_without_arap' ELSE NULL \
						END\
					 ) \
					,(\
						CASE WHEN (crbx7.isOpenARExists = True OR crbx7.isOpenAPExists = True) AND crbx7.isInvoiceChain = True	\
						THEN 'inv_EQ_open_arap' ELSE NULL \
						END\
					 )\
					,(\
						CASE WHEN (crbx7.isOpenARExists = True OR crbx7.isOpenAPExists = True) AND crbx7.isRevenueExists = True	\
						THEN 'revenue_EQ_open_arap' ELSE NULL \
						END\
					 ) \
					,(\
						CASE WHEN (crbx7.isOpenARExists = True OR crbx7.isOpenAPExists = True) AND crbx7.isInvoiceChain = False	\
						THEN 'other_EQ_open_arap' ELSE NULL \
						END\
					 )  \
					,(\
						CASE WHEN crbx7.isInvoiceChain = True\
						THEN 'inv_EQ_other' ELSE NULL \
						END\
					 )   \
					,(\
						CASE WHEN crbx7.isRevenueExists = False\
						THEN 'arap_without_revenue' ELSE NULL \
						END\
					 )	\
					,'other_EQ_other')  \
                ELSE crbx7.typeChain \
                END").alias('typeChain'))

      gen_L1_STG_ClearingBox_07_ChainInterpreted = gen_L1_STG_ClearingBox_07_ChainInterpreted.alias('crbx7')\
		  .join (knw_LK_GD_ClearingBoxLookupTypeChain.alias('lkctc'),\
		  (col('crbx7.typeChain') == col('lkctc.typeChain')),how= 'left')\
          .select(col('crbx7.idGroupChain').alias('idGroupChain')\
			,col('crbx7.chainInterference').alias('chainInterference')\
			,when(col('crbx7.typeChain') ==	col('lkctc.typeChain'),col('lkctc.id'))\
			  .otherwise(col('crbx7.idClearingTypeChain')).alias('idClearingTypeChain')\
			,when(col('crbx7.typeChain') ==	col('lkctc.typeChain'),col('lkctc.typeGroup'))\
			  .otherwise(col('crbx7.typeGroupChain')).alias('typeGroupChain')\
			,col('crbx7.typeChain').alias('typeChain')\
			,col('crbx7.clearingBoxClientERP').alias('clearingBoxClientERP')\
			,col('crbx7.clearingBoxCompanyCode').alias('clearingBoxCompanyCode')\
			,col('crbx7.amountInvoice').alias('amountInvoice')\
			,col('crbx7.amountCreditMemo').alias('amountCreditMemo')\
			,col('crbx7.amountPrepayment').alias('amountPrepayment')\
			,col('crbx7.amountAROtherClearing').alias('amountAROtherClearing')\
			,col('crbx7.amountAPOtherClearing').alias('amountAPOtherClearing')\
			,col('crbx7.amountIncome').alias('amountIncome')\
			,col('crbx7.amountInterest').alias('amountInterest')\
			,col('crbx7.amountExpenseSGA').alias('amountExpenseSGA')\
			,col('crbx7.amountCurrencyFX').alias('amountCurrencyFX')\
			,col('crbx7.amountCash').alias('amountCash')\
			,col('crbx7.amountCashAdjustment').alias('amountCashAdjustment')\
			,col('crbx7.amountRevenue').alias('amountRevenue')\
			,col('crbx7.amountRevenueIntime').alias('amountRevenueIntime')\
			,col('crbx7.amountOtherIncomeExpense').alias('amountOtherIncomeExpense')\
			,col('crbx7.amountVat').alias('amountVat')\
			,col('crbx7.amountAROther').alias('amountAROther')\
			,col('crbx7.amountAPOther').alias('amountAPOther')\
			,col('crbx7.amountOtherClearing').alias('amountOtherClearing')\
			,col('crbx7.amountOtherPosting').alias('amountOtherPosting')\
			,col('crbx7.amountReversalClearing').alias('amountReversalClearing')\
			,col('crbx7.isValidReversalClearing').alias('isValidReversalClearing')\
			,col('crbx7.countInvoiceSL').alias('countInvoiceSL')\
			,col('crbx7.countInvoiceGL').alias('countInvoiceGL')\
			,col('crbx7.countIncome').alias('countIncome')\
			,col('crbx7.countInterest').alias('countInterest')\
			,col('crbx7.countExpenseSGA').alias('countExpenseSGA')\
			,col('crbx7.countCreditMemoSL').alias('countCreditMemoSL')\
			,col('crbx7.countCreditMemoGL').alias('countCreditMemoGL')\
			,col('crbx7.countAROtherClearing').alias('countAROtherClearing')\
			,col('crbx7.countAPOtherClearing').alias('countAPOtherClearing')\
			,col('crbx7.countRevenueInvoice').alias('countRevenueInvoice')\
			,col('crbx7.countRevenueCreditMemo').alias('countRevenueCreditMemo')\
			,col('crbx7.countRevenueGL').alias('countRevenueGL')\
			,col('crbx7.countOtherIncomeExpense').alias('countOtherIncomeExpense')\
			,col('crbx7.countCashDiscount').alias('countCashDiscount')\
			,col('crbx7.countBankCharge').alias('countBankCharge')\
			,col('crbx7.countCash').alias('countCash')\
			,col('crbx7.countBankStatement').alias('countBankStatement')\
			,col('crbx7.countChainInterference').alias('countChainInterference')\
			,col('crbx7.isCashChain').alias('isCashChain')\
			,col('crbx7.isInvoiceChain').alias('isInvoiceChain')\
			,col('crbx7.isCreditMemoChain').alias('isCreditMemoChain')\
			,col('crbx7.isOtherTradeARChain').alias('isOtherTradeARChain')\
			,col('crbx7.isOtherAPChain').alias('isOtherAPChain')\
			,col('crbx7.isOtherIncomeChain').alias('isOtherIncomeChain')\
			,col('crbx7.isOtherInterestChain').alias('isOtherInterestChain')\
			,col('crbx7.isExpenseSGAChain').alias('isExpenseSGAChain')\
			,col('crbx7.isCurrencyFXChain').alias('isCurrencyFXChain')\
			,col('crbx7.isOtherPrepaymentChain').alias('isOtherPrepaymentChain')\
			,col('crbx7.isMisinterpretedChain').alias('isMisinterpretedChain')\
			,col('crbx7.isRevenueExists').alias('isRevenueExists')\
			,col('crbx7.isARExists').alias('isARExists')\
			,col('crbx7.isOpenARExists').alias('isOpenARExists')\
			,col('crbx7.isAPExists').alias('isAPExists')\
			,col('crbx7.isOpenAPExists').alias('isOpenAPExists'))

      gen_L1_STG_ClearingBox_07_ChainInterpreted = objDataTransformation.gen_convertToCDMandCache\
              (gen_L1_STG_ClearingBox_07_ChainInterpreted,'gen','L1_STG_ClearingBox_07_ChainInterpreted',False)
                  
      executionStatus = "gen_L1_STG_ClearingBox_07_ChainInterpreted populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  
