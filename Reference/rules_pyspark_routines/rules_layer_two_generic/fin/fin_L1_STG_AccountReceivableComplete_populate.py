# Databricks notebook source
from pyspark.sql.functions import coalesce,dayofmonth
import sys
import traceback

def fin_L1_STG_AccountReceivableComplete_populate(): 
    try:
    
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
    
      global fin_L1_STG_AccountReceivableComplete
      
      df_L1_TMP_AllReceivableAccount_journal = fin_L1_MD_GLAccount.alias("glac1")\
        .join(fin_L1_STG_GLAccountMapping.alias("glam1"),col("glac1.accountNumber")==col("glam1.accountNumber"),how="inner")\
        .join(knw_LK_CD_ReportingSetup.alias("rpt"),how="cross")\
        .filter(col("glam1.isTradeAR")==True)\
        .select(col("rpt.clientCode").alias("clientCode")\
        ,col("rpt.companyCode").alias("companyCode")\
        ,col("glam1.accountNumber").alias("accountNumber")).distinct()

      df_L1_TMP_AllReceivableAccount_ar = fin_L1_TD_AccountReceivable.alias("ar")\
        .select(col("ar.accountReceivableClientCode").alias("clientCode")\
        ,col("ar.companyCode").alias("companyCode")\
        ,col("ar.accountNumber").alias("accountNumber")).distinct()

      df_L1_TMP_AllReceivableAccount = df_L1_TMP_AllReceivableAccount_journal.union(df_L1_TMP_AllReceivableAccount_ar)\
        .select("clientCode","companyCode","accountNumber").distinct()
      
      df_L1_TMP_AccountReceivableJournal = fin_L1_TD_Journal.alias("jou").join(df_L1_TMP_AllReceivableAccount.alias("rcac")\
        ,(col("jou.companyCode")==col("rcac.companyCode"))&(col("jou.accountNumber")==col("rcac.accountNumber")),how="inner")\
        .select(col("jou.clientCode").alias("accountReceivableJournalClientCode")\
        ,col("jou.companyCode").alias("companyCode")\
        ,col("jou.fiscalYear").alias("financialYear")\
        ,col("jou.customerNumber").alias("customerNumber")\
        ,col("jou.documentNumber").alias("GLJEDocumentNumber")\
        ,col("jou.lineItem").alias("GLJELineNumber")\
        ,col("jou.documentDate").alias("GLDocumentDate")\
        ,col("jou.accountNumber").alias("accountNumber")\
        ,col("jou.createdBy").alias("userName")\
        ,col("jou.referenceDocumentNumber").alias("referenceDocumentNumber")\
        ,col("jou.referenceSubledger").alias("referenceSubledger")\
        ,col("jou.referenceSubledgerDocumentNumber").alias("referenceSubledgerDocumentNumber")\
        ,col("jou.creationDate").alias("documentEntryDate")\
        ,when(col("jou.debitCreditIndicator")==lit("C"),col("jou.amountLC")*-1).otherwise(col("jou.amountLC")).alias("amountLC")\
        ,when(col("jou.debitCreditIndicator")==lit("C"),col("jou.amountDC")*-1).otherwise(col("jou.amountDC")).alias("amountDC")\
        ,col("jou.documentCurrency").alias("documentCurrency")\
        ,col("jou.debitCreditIndicator").alias("debitCreditIndicator")\
        ,col("jou.clearingDate").alias("documentClearingDate")\
        ,col("jou.clearingDocumentNumber").alias("clearingDocumentNumber")\
        ,col("jou.postingDate").alias("GLJEPostingDate")\
        ,col("jou.lineDescription").alias("postingDescription")\
        ,col("jou.documentStatus").alias("documentStatus")\
        ,col("jou.isReversal").alias("isCancelledFlag")\
        ,col("jou.assignmentNumber").alias("allocationNumber")\
        ,col("jou.cashDiscountAmount").alias("cashDiscountLocalCurrency")\
        ,col("jou.taxAmountLocalCurrency").alias("taxAmountLocalCurrency")\
        ,col("jou.GLSpecialIndicator").alias("GLSpecialIndicator")\
        ,col("jou.billingDocumentNumber").alias("billingDocumentNumber")\
        ,col("jou.documentType").alias("documentType")\
        ,col("jou.postingKey").alias("postingKey")\
        ,col("jou.paymentMethod").alias("paymentMethod")\
        ,col("jou.paymentTerm").alias("accountReceivableJournalPaymentTerm")\
        ,lit(0).alias("accountReceivableJournalPaymentTermDayLimit")\
        ,col("jou.calculationBaselineDate").alias("accountReceivableJournalCalculationBaselineDate")\
        ,col("jou.cashDiscountDayForPayment1").alias("accountReceivableJournalCashDiscountDayForPayment1")\
        ,col("jou.cashDiscountDayForPayment2").alias("accountReceivableJournalCashDiscountDayForPayment2")\
        ,col("jou.dayForNetPayment").alias("accountReceivableJournalDayForNetPayment")\
        ,col("jou.cashDiscountPercentageForPayment1").alias("accountReceivableJournalCashDiscountPercentageForPayment1")\
        ,col("jou.cashDiscountPercentageForPayment2").alias("accountReceivableJournalCashDiscountPercentageForPayment2")\
        ,col("jou.amountLC").alias("amountLocalCurrency")\
        ,col("jou.amountDC").alias("amountDocumentCurrency")\
        ,col("jou.eligibleDiscountAmountDocumentCurrency").alias("eligibleDiscountAmountDocumentCurrency")\
        ,lit("Journal Entry").alias("dataOrigin")\
        ,col("jou.accountType").alias("accountType")\
        ,col("jou.financialPeriod").alias("financialPeriod")\
        ,col("jou.exchangeDate").alias("exchangeDate")\
        ,col("jou.lineDescription").alias("accountReceivableLineitemDescription"))

      df_L1_STG_AccountReceivable_ar = fin_L1_TD_AccountReceivable.alias("acrv1").join(df_L1_TMP_AccountReceivableJournal.alias("arj1"),\
        (col("acrv1.GLJEDocumentNumber") == col("arj1.GLJEDocumentNumber"))&\
        (col("acrv1.GLJELineNumber") == col("arj1.GLJELineNumber"))&\
        (col("acrv1.financialYear")	== col("arj1.financialYear"))&\
        (col("acrv1.companyCode") == col("arj1.companyCode")),how="left")\
        .select(col("acrv1.accountReceivableClientCode").alias("accountReceivableClientCode")\
        ,col("acrv1.companyCode").alias("companyCode")\
        ,col("acrv1.financialYear").alias("financialYear")\
        ,col("acrv1.customerNumber").alias("customerNumber")\
        ,col("acrv1.GLJEDocumentNumber").alias("GLJEDocumentNumber")\
        ,col("acrv1.GLJELineNumber").alias("GLJELineNumber")\
        ,col("acrv1.GLDocumentDate").alias("GLDocumentDate")\
        ,col("acrv1.accountNumber").alias("accountNumber")\
        ,col("arj1.userName").alias("userName")\
        ,col("acrv1.referenceDocumentNumber").alias("referenceDocumentNumber")\
        ,col("arj1.referenceSubledger").alias("referenceSubledger")\
        ,col("arj1.referenceSubledgerDocumentNumber").alias("referenceSubledgerDocumentNumber")\
        ,col("acrv1.documentEntryDate").alias("documentEntryDate")\
        ,col("acrv1.amountLC").alias("amountLC")\
        ,col("acrv1.amountDC").alias("amountDC")\
        ,col("acrv1.documentCurrency").alias("documentCurrency")\
        ,col("acrv1.debitCreditIndicator").alias("debitCreditIndicator")\
        ,when((col("acrv1.clearingDocumentNumber")=="")&(col("arj1.clearingDocumentNumber")!=lit("")),col("arj1.documentClearingDate"))\
        .otherwise(col("acrv1.documentClearingDate")).alias("documentClearingDate")\
        ,when((col("acrv1.clearingDocumentNumber")=="")&(col("arj1.clearingDocumentNumber")!=lit("")),col("arj1.clearingDocumentNumber"))\
        .otherwise(col("acrv1.clearingDocumentNumber")).alias("clearingDocumentNumber")\
        ,col("acrv1.GLJEPostingDate").alias("GLJEPostingDate")\
        ,col("acrv1.postingDescription").alias("postingDescription")\
        ,col("acrv1.documentStatus").alias("documentStatus")\
        ,col("acrv1.isCancelledFlag").alias("isCancelledFlag")\
        ,col("acrv1.allocationNumber").alias("allocationNumber")\
        ,col("acrv1.cashDiscountLocalCurrency").alias("cashDiscountLocalCurrency")\
        ,col("acrv1.taxAmountLocalCurrency").alias("taxAmountLocalCurrency")\
        ,col("acrv1.GLSpecialIndicator").alias("GLSpecialIndicator")\
        ,col("acrv1.billingDocumentNumber").alias("billingDocumentNumber")\
        ,col("acrv1.documentType").alias("documentType")\
        ,col("acrv1.postingKey").alias("postingKey")\
        ,col("acrv1.paymentMethod").alias("paymentMethod")\
        ,col("acrv1.accountReceivablePaymentTerm").alias("accountReceivablePaymentTerm")\
        ,col("acrv1.accountReceivablePaymentTermDayLimit").alias("accountReceivablePaymentTermDayLimit")\
        ,col("acrv1.accountReceivableCalculationBaselineDate").alias("accountReceivableCalculationBaselineDate")\
        ,col("acrv1.accountReceivableCashDiscountDayForPayment1").alias("accountReceivableCashDiscountDayForPayment1")\
        ,col("acrv1.accountReceivableCashDiscountDayForPayment2").alias("accountReceivableCashDiscountDayForPayment2")\
        ,col("acrv1.accountReceivableDayForNetPayment").alias("accountReceivableDayForNetPayment")\
        ,col("acrv1.accountReceivableCashDiscountPercentageForPayment1").alias("accountReceivableCashDiscountPercentageForPayment1")\
        ,col("acrv1.accountReceivableCashDiscountPercentageForPayment2").alias("accountReceivableCashDiscountPercentageForPayment2")\
        ,col("acrv1.amountLocalCurrency").alias("amountLocalCurrency")\
        ,col("acrv1.amountDocumentCurrency").alias("amountDocumentCurrency")\
        ,col("acrv1.eligibleDiscountAmountDocumentCurrency").alias("eligibleDiscountAmountDocumentCurrency")\
        ,when((col("acrv1.clearingDocumentNumber")=="")&(col("arj1.clearingDocumentNumber")!=lit("")),col("arj1.dataOrigin"))\
        .otherwise(col("acrv1.dataOrigin")).alias("dataOrigin")\
        ,col("acrv1.accountType").alias("accountType")\
        ,col("acrv1.financialPeriod").alias("financialPeriod")\
        ,col("arj1.exchangeDate").alias("exchangeDate")\
        ,col("acrv1.accountReceivableLineitemDescription").alias("accountReceivableLineitemDescription"))

      df_L1_STG_AccountReceivable_jou = df_L1_TMP_AccountReceivableJournal.alias("arj1").join(fin_L1_TD_AccountReceivable.alias("acre"),\
        (col("arj1.companyCode")== col("acre.companyCode"))&\
        (col("arj1.financialYear")==col("acre.financialYear"))&\
        (col("arj1.GLJEDocumentNumber")== col("acre.GLJEDocumentNumber"))&\
        (col("arj1.GLJELineNumber")== col("acre.GLJELineNumber")),how="leftanti")\
        .select(col("arj1.accountReceivableJournalClientCode").alias("accountReceivableClientCode")\
        ,col("arj1.companyCode").alias("companyCode")\
        ,col("arj1.financialYear").alias("financialYear")\
        ,col("arj1.customerNumber").alias("customerNumber")\
        ,col("arj1.GLJEDocumentNumber").alias("GLJEDocumentNumber")\
        ,col("arj1.GLJELineNumber").alias("GLJELineNumber")\
        ,col("arj1.GLDocumentDate").alias("GLDocumentDate")\
        ,col("arj1.accountNumber").alias("accountNumber")\
        ,col("arj1.userName").alias("userName")\
        ,col("arj1.referenceDocumentNumber").alias("referenceDocumentNumber")\
        ,col("arj1.referenceSubledger").alias("referenceSubledger")\
        ,col("arj1.referenceSubledgerDocumentNumber").alias("referenceSubledgerDocumentNumber")\
        ,col("arj1.documentEntryDate").alias("documentEntryDate")\
        ,col("arj1.amountLC").alias("amountLC")\
        ,col("arj1.amountDC").alias("amountDC")\
        ,col("arj1.documentCurrency").alias("documentCurrency")\
        ,col("arj1.debitCreditIndicator").alias("debitCreditIndicator")\
        ,col("arj1.documentClearingDate").alias("documentClearingDate")\
        ,col("arj1.clearingDocumentNumber").alias("clearingDocumentNumber")\
        ,col("arj1.GLJEPostingDate").alias("GLJEPostingDate")\
        ,col("arj1.postingDescription").alias("postingDescription")\
        ,col("arj1.documentStatus").alias("documentStatus")\
        ,col("arj1.isCancelledFlag").alias("isCancelledFlag")\
        ,col("arj1.allocationNumber").alias("allocationNumber")\
        ,col("arj1.cashDiscountLocalCurrency").alias("cashDiscountLocalCurrency")\
        ,col("arj1.taxAmountLocalCurrency").alias("taxAmountLocalCurrency")\
        ,col("arj1.GLSpecialIndicator").alias("GLSpecialIndicator")\
        ,col("arj1.billingDocumentNumber").alias("billingDocumentNumber")\
        ,col("arj1.documentType").alias("documentType")\
        ,col("arj1.postingKey").alias("postingKey")\
        ,col("arj1.paymentMethod").alias("paymentMethod")\
        ,col("arj1.accountReceivableJournalPaymentTerm").alias("accountReceivablePaymentTerm")\
        ,col("arj1.accountReceivableJournalPaymentTermDayLimit").alias("accountReceivablePaymentTermDayLimit")\
        ,col("arj1.accountReceivableJournalCalculationBaselineDate").alias("accountReceivableCalculationBaselineDate")\
        ,col("arj1.accountReceivableJournalCashDiscountDayForPayment1").alias("accountReceivableCashDiscountDayForPayment1")\
        ,col("arj1.accountReceivableJournalCashDiscountDayForPayment2").alias("accountReceivableCashDiscountDayForPayment2")\
        ,col("arj1.accountReceivableJournalDayForNetPayment").alias("accountReceivableDayForNetPayment")\
        ,col("arj1.accountReceivableJournalCashDiscountPercentageForPayment1").alias("accountReceivableCashDiscountPercentageForPayment1")\
        ,col("arj1.accountReceivableJournalCashDiscountPercentageForPayment2").alias("accountReceivableCashDiscountPercentageForPayment2")\
        ,col("arj1.amountLocalCurrency").alias("amountLocalCurrency")\
        ,col("arj1.amountDocumentCurrency").alias("amountDocumentCurrency")\
        ,col("arj1.eligibleDiscountAmountDocumentCurrency").alias("eligibleDiscountAmountDocumentCurrency")\
        ,col("arj1.dataOrigin").alias("dataOrigin")\
        ,col("arj1.accountType").alias("accountType")\
        ,col("arj1.financialPeriod").alias("financialPeriod")\
        ,col("arj1.exchangeDate").alias("exchangeDate")\
        ,col("arj1.accountReceivableLineitemDescription").alias("accountReceivableLineitemDescription"))
      
      fin_L1_STG_AccountReceivableComplete = df_L1_STG_AccountReceivable_ar.union(df_L1_STG_AccountReceivable_jou)
      
      fin_L1_STG_AccountReceivableComplete  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_AccountReceivableComplete,'fin','L1_STG_AccountReceivableComplete',True)

      objGenHelper.gen_writeToFile_perfom(fin_L1_STG_AccountReceivableComplete,gl_CDMLayer2Path + "fin_L1_STG_AccountReceivableComplete.parquet" )

      executionStatus = "L1_STG_AccountReceivableComplete populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
