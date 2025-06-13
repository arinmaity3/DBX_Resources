# Databricks notebook source
##Final-fin.usp_L1_STG_ReceivableSubledger_populate
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when,sum,avg,max,concat,coalesce,abs
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType    
from datetime import datetime
from pyspark.sql.functions import *
   
def fin_L1_STG_ReceivableSubledger_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    #logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    #Variable
    global fin_L1_STG_ReceivableSubledger
    erpSAPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))
    startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
    extractionDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'EXTRACTION_DATE')).date()
    finYear=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
    periodEnd=int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END'))
    defaultDate='1900-01-01'
    defaultMaxDate='9999-12-31'
    defaultChainCashType= 'No information available'
    v_surrogateKey = row_number().over(Window().orderBy(lit('')))
    v_bitTrue=True
    v_bitFalse=False
    
    df_L1_TMP_RevenueARAllPeriod=gen_L1_MD_Period.alias('prod1')\
      .join(knw_LK_CD_ReportingSetup.alias('lkrs')\
              ,(col("prod1.companyCode")==col("lkrs.companyCode")),how="inner")\
      .filter((col('prod1.fiscalYear')==finYear)\
              &(col('prod1.financialPeriod') <=periodEnd))\
      .groupBy('lkrs.clientCode','prod1.companyCode','prod1.financialPeriod')\
      .agg(max('prod1.calendarDate').alias("CutOffDate")\
          ,max("lkrs.chartOfAccount").alias("ChartOfAccount")\
          ,max("lkrs.KPMGDataReportingLanguage").alias("ReportingLanguage")\
        )\

    #df_L1_TMP_RevenueARAllPeriod.display()
    
    df_L1_TMP_BillingSalesOrganization=otc_L1_TD_Billing.alias("bill")\
      .select(col('bill.billingDocumentNumber').alias('billingDocumentNumber')\
             ,col('bill.billingDocumentSalesOrganization').alias('billingDocumentSalesOrganization')\
             ,col('bill.billingDocumentDistributionChannel').alias('billingDocumentDistributionChannel')\
             ,col('bill.billingDocumentDivision').alias('billingDocumentDivision')\
             )
    #df_L1_TMP_BillingSalesOrganization.display()
    
    #1,1 arv1.accountReceivablePaymentTerm
    v_paymentTerm = when(col('arv1.accountReceivablePaymentTerm').isNull(),lit('#NA#'))\
                    .when(col('arv1.accountReceivablePaymentTerm')==lit(''),lit('#NA#'))\
                    .otherwise(col('arv1.accountReceivablePaymentTerm'))
    #2,2 arv1.accountReceivablePaymentTermDayLimit
    v_paymentTermDayLimit = when(col('arv1.accountReceivablePaymentTermDayLimit').isNull(),lit(0))\
                    .otherwise(col('arv1.accountReceivablePaymentTermDayLimit'))
    #3,3 arv1.accountReceivableCalculationBaselineDate
    v_paymentTermCalculationBaselineDate= coalesce(col('arv1.accountReceivableCalculationBaselineDate'),lit(defaultDate))

    #4,4 arv1.accountReceivableCashDiscountDayForPayment1
    v_paymentTermCashDiscountDayForPayment1=when(col('arv1.accountReceivableCashDiscountDayForPayment1').isNull(),lit(0))\
           .when(col('arv1.accountReceivableCashDiscountDayForPayment1')==lit(''),lit(0))\
           .otherwise(col('arv1.accountReceivableCashDiscountDayForPayment1'))
    #5,6
    v_paymentTermCashDiscountDayForPayment2=when(col('arv1.accountReceivableCashDiscountDayForPayment2')\
                                                 .isNull(),lit(0))\
           .when(col('arv1.accountReceivableCashDiscountDayForPayment2')==lit(''),lit(0))\
           .otherwise(col('arv1.accountReceivableCashDiscountDayForPayment2'))

    #6,#8 arv1.accountReceivableDayForNetPayment
    v_paymentTermDayForNetPayment=when(col('arv1.accountReceivableDayForNetPayment').isNull(),lit(0))\
           .when(col('arv1.accountReceivableDayForNetPayment')==lit(''),lit(0))\
           .otherwise(col('arv1.accountReceivableDayForNetPayment'))

    #7,#5 arv1.accountReceivableCashDiscountPercentageForPayment1
    v_paymentTermCashDiscountPercentageForPayment1=\
        coalesce(col('arv1.accountReceivableCashDiscountPercentageForPayment1'),lit(0))

    #8,#7 arv1.accountReceivableCashDiscountPercentageForPayment2
    v_paymentTermCashDiscountPercentageForPayment2=\
         coalesce(col('arv1.accountReceivableCashDiscountPercentageForPayment2'),lit(0))
    #9,
    #receivableSubledgerCashDiscountPercentageBestPossible=	tpt1.paymentTermCashDiscountPercentageBestPossible
    v_paymentTermCashDiscountPercentageBestPossible =\
        when(col('arv1.accountReceivableCashDiscountPercentageForPayment1')>0,\
              col('arv1.accountReceivableCashDiscountPercentageForPayment1'))\
        .when(col('arv1.accountReceivableCashDiscountPercentageForPayment2')>0,\
              col('arv1.accountReceivableCashDiscountPercentageForPayment2'))\
        .otherwise(lit(0))

    #10
    #receivableSubledgerNetPaymentDue=	tpt1.paymentTermNetPaymentDue
    v_paymentTermNetPaymentDue =\
        when(col('arv1.accountReceivableDayForNetPayment')>0,\
              col('arv1.accountReceivableDayForNetPayment'))\
        .when(col('arv1.accountReceivableCashDiscountDayForPayment2')>0,\
              col('arv1.accountReceivableCashDiscountDayForPayment2'))\
        .when(col('arv1.accountReceivableCashDiscountDayForPayment1')>0,\
              col('arv1.accountReceivableCashDiscountDayForPayment1'))\
        .otherwise(lit(0))
    
    #12
    # receivableSubledgerCashDiscountPaymentDue	=tpt1.paymentTermCashDiscountPaymentDue
    v_paymentTermCashDiscountPaymentDue =\
        when(col('arv1.accountReceivableCashDiscountDayForPayment1')>0,\
              col('arv1.accountReceivableCashDiscountDayForPayment1'))\
        .when(col('arv1.accountReceivableCashDiscountDayForPayment2')>0,\
              col('arv1.accountReceivableCashDiscountDayForPayment2'))\
        .otherwise(lit(0))
    #13
    #receivableSubledgerCashDiscountPercentage=	tpt1.paymentTermCashDiscountPercentage
    v_paymentTermCashDiscountPercentage=when((col('arv1.cashDiscountLocalCurrency')>0)&
                                             (col('arv1.amountLocalCurrency')>0),\
           ((col('arv1.cashDiscountLocalCurrency')/col('arv1.amountLocalCurrency'))*100.00))\
          .otherwise(lit(0.00))
    #14
    #isCashDiscountable = tpt1.isCashDiscountable
    v_isCashDiscountable=\
        when(col('arv1.accountReceivableCashDiscountPercentageForPayment1') !=0.0,\
              lit(1))\
        .when(col('arv1.accountReceivableCashDiscountPercentageForPayment2')!=0.0,\
               lit(1))\
        .otherwise(lit(0))
    #15
    #hasCashDiscount= tpt1.hasCashDiscount
    v_hasCashDiscount=when(col('arv1.cashDiscountLocalCurrency') >0.0,lit(1))\
        .otherwise(lit(0))

    #16 receivableSubledgerScenarioIDYE
    v_receivableSubledgerScenarioIDYE =\
        when(col('rec1.isOpenItemFrom')>lit(endDate),lit(4))\
        .when(col('rec1.isOpenItemTo')>lit(extractionDate),lit(2))\
        .when((col('rec1.isOpenItemTo')>lit(endDate)) & (col('rec1.isOpenItemTo')<=lit(extractionDate)),lit(1))\
        .when(col('rec1.isOpenItemTo') <=lit(endDate),lit(3))\
        .otherwise(lit(-1))

    #17 receivableSubledgerClearingStatus
    v_receivableSubledgerClearingStatus=\
        when((col('rap1.cutOffDate') >= col('rec1.isOpenItemFrom')) \
             & (col('rap1.cutOffDate')< col('rec1.isOpenItemTo')),lit('OP'))\
        .when((col('rap1.cutOffDate') >= col('rec1.isOpenItemFrom')) \
             & (col('rap1.cutOffDate') >= col('rec1.isOpenItemTo')),lit('CL'))\
        .when(col('rap1.cutOffDate') < col('rec1.isOpenItemFrom') ,lit('FU'))\
        .otherwise(lit('#NA#'))

    #18paymentTerm
    v_tpt2_paymentTerm= when(col('ctmr2.paymentTerm').isNull(),lit('#NA#'))\
                    .when(col('ctmr2.paymentTerm')==lit(''),lit('#NA#'))\
                    .otherwise(col('ctmr2.paymentTerm'))	
    #19paymentTerm
    v_tpt2_paymentTermDayLimit = when(col('pts1.paymentTermDayLimit').isNull(),lit(0))\
                    .otherwise(col('pts1.paymentTermDayLimit'))

    #20 paymentTermCashDiscountDayForPayment1
    v_tpt2_paymentTermCashDiscountDayForPayment1=when(col('pts1.paymentTermCashDiscountDayForPayment1').isNull(),lit(0))\
           .when(col('pts1.paymentTermCashDiscountDayForPayment1')==lit(''),lit(0))\
           .otherwise(col('pts1.paymentTermCashDiscountDayForPayment1'))

    #21 paymentTermCashDiscountDayForPayment2
    v_tpt2_paymentTermCashDiscountDayForPayment2=\
            when(col('pts1.paymentTermCashDiscountPercentageForPayment2').isNull(),lit(0))\
           .when(col('pts1.paymentTermCashDiscountPercentageForPayment2')==lit(''),lit(0))\
           .otherwise(col('pts1.paymentTermCashDiscountPercentageForPayment2'))
    #22  paymentTermDayForNetPayment
    v_tpt2_paymentTermDayForNetPayment=when(col('pts1.paymentTermDayForNetPayment').isNull(),lit(0))\
           .when(col('pts1.paymentTermDayForNetPayment')==lit(''),lit(0))\
           .otherwise(col('pts1.paymentTermDayForNetPayment'))

    df_L1_STG_ReceivableSubledger_TMP=fin_L1_STG_Receivable.alias('rec1')\
      .join(fin_L1_STG_JELineItem.alias('jel1')\
              ,((col("rec1.companyCode")==col("jel1.companyCode"))\
               & (col("rec1.fiscalYear")==col("jel1.financialYear"))\
               & (col("rec1.GLJEDocumentNumber")==col("jel1.GLJEDocumentNumber"))\
               & (col("rec1.GLJELineNumber")==col("jel1.GLJELineNumber"))\
               ),how="inner")\
      .join(fin_L1_TD_AccountReceivable.alias('arv1')\
              ,((col("rec1.client")==col("arv1.accountReceivableClientCode"))\
               & (col("rec1.companyCode")==col("arv1.companyCode"))\
               & (col("rec1.fiscalYear")==col("arv1.financialYear"))\
               & (col("rec1.GLJEDocumentNumber")==col("arv1.GLJEDocumentNumber"))\
               & (col("rec1.GLJELineNumber")==col("arv1.GLJELineNumber"))\
               ),how="inner")\
      .join(df_L1_TMP_RevenueARAllPeriod.alias('rap1')\
                ,((col("rec1.companyCode")==col("rap1.companyCode"))\
                 & (col("rec1.client")==col("rap1.clientCode"))\
                 ),how="inner")\
      .join(gen_L2_DIM_Organization.alias('ognt2')\
                    ,((col("rec1.companyCode")==col("ognt2.companyCode"))\
                     & (col("rec1.client")==col("ognt2.clientID"))\
                     ),how="left")\
      .join(otc_L2_DIM_Customer.alias('ctmr2')\
                    ,((col("ognt2.organizationUnitSurrogateKey")==col("ctmr2.organizationUnitSurrogateKey"))\
                     & (col("rec1.customerNumber")==col("ctmr2.customerNumber"))\
                     ),how="left")\
      .join(df_L1_TMP_BillingSalesOrganization.alias('bill')\
                    ,(col("rec1.billingDocumentNumber")==col("bill.billingDocumentNumber")\
                     ),how="left")\
      .join(fin_L1_STG_PaymentTerm.alias('pts1')\
                ,((col("rec1.client")==col("pts1.paymentTermClientCode"))\
                 & (col("ctmr2.paymentTerm")==col("pts1.paymentTerm"))\
                 & (col("arv1.accountReceivablePaymentTermDayLimit")==col("pts1.paymentTermDayLimit"))\
                 ),how="left")\
      .filter(coalesce(col('rap1.cutOffDate'),lit(defaultDate)) >=col('rec1.isOpenItemFrom'))\
      .select(col('jel1.idGLJELine').alias('IDGLJELine')\
        ,col('jel1.idGLJE').alias('idGLJE')\
        ,col('rec1.client').alias('receivableSubledgerERPClient')\
        ,col('rec1.companyCode').alias('receivableSubledgerCompanyCode')\
        ,col('rec1.fiscalYear').alias('receivableSubledgerFiscalYear')\
        ,col('rec1.financialPeriod').alias('receivableSubledgerFinancialPeriod')\
        ,col('rec1.GLJEDocumentNumber').alias('receivableSubledgerDocumentNumber')\
        ,col('rec1.GLDocumentDate').alias('receivableSubledgerDocumentDate')\
        ,col('rec1.GLDocumentPostingDate').alias('receivableSubledgerPostingDate')\
        ,col('rec1.GLJELineNumber').alias('receivableSubledgerDocumentLineItem')\
        ,col('rec1.glAccountNumber').alias('receivableSubledgerAccountNumber')\
        ,col('rec1.GLDocumentType').alias('receivableSubledgerDocumentType')\
        ,col('rec1.GLJEDocumentNumber').alias('receivableSubledgerGLBillingDocumentNumber')\
        ,col('rec1.customerNumber').alias('receivableSubledgerCustomerNumber')\
        ,col('rec1.isOpenItemFrom').alias('receivableSubledgerDateOpenSince')\
        ,col('rec1.isOpenItemTo').alias('receivableSubledgerDateOpenTill')\
        ,col('rec1.glClearingDocumentNumber').alias('receivableSubledgerClearingDocumentNumber')\
        ,col('rec1.glClearingDocumentPostingDate').alias('receivableSubledgerClearingDocumentPostingDate')\
        ,col('arv1.GLJEPostingDate').alias('receivableSubledgerInvoiceDate')\
        ,lit(v_paymentTerm).alias('receivableSubledgerPaymentTerm')\
        ,lit(v_paymentTermDayLimit).alias('receivableSubledgerPaymentTermDayLimit')\
        ,lit(v_paymentTermCalculationBaselineDate).alias('receivableSubledgerCalculationBaselineDate')\
        ,lit(v_paymentTermCashDiscountDayForPayment1).alias('receivableSubledgerCashDiscountDayForPayment1')\
        ,lit(v_paymentTermCashDiscountDayForPayment2).alias('receivableSubledgerCashDiscountDayForPayment2')\
        ,lit(v_paymentTermDayForNetPayment).alias('receivableSubledgerDayForNetPayment')\
        ,lit(v_paymentTermCashDiscountPercentageForPayment1)\
            .alias('receivableSubledgerCashDiscountPercentageForPayment1')\
        ,lit(v_paymentTermCashDiscountPercentageForPayment2)\
              .alias('receivableSubledgerCashDiscountPercentageForPayment2')\
        ,lit(v_paymentTermCashDiscountPercentageBestPossible)\
              .alias('receivableSubledgerCashDiscountPercentageBestPossible')\
  #       ,coalesce(lit(v_paymentTermPaymentDueDate),col('arv1.GLJEPostingDate'))\
  #             .alias('receivableSubledgerPaymentDueDate')\
        ,col('arv1.GLJEPostingDate').alias('_GLJEPostingDate')\
        ,col('arv1.accountReceivableCalculationBaselineDate')\
        ,lit(v_paymentTermCashDiscountPaymentDue).alias('receivableSubledgerCashDiscountPaymentDue')\
        ,lit(v_paymentTermCashDiscountPercentage).alias('receivableSubledgerCashDiscountPercentage')\
        ,lit('').alias('receivableSubledgerCashDiscountPercentageText')\
        ,lit(v_paymentTermNetPaymentDue).alias('receivableSubledgerNetPaymentDue')\
        ,lit(v_isCashDiscountable).alias('isCashDiscountable')\
        ,lit(v_hasCashDiscount).alias('hasCashDiscount')\
        ,col('rec1.amountReceivableTrade').alias('receivableSubledgerAmountReceivableTrade')\
        ,col('arv1.eligibleDiscountAmountDocumentCurrency').alias('receivableSubledgerEligibleDiscountAmountDC')\
        ,col('rec1.amountReceivableTrade').alias('receivableSubledgerTradeDiscountBestPossibleAmount')\
        ,col('arv1.cashDiscountLocalCurrency').alias('receivableSubledgerCashDiscountBestPossibleAmount')\
        ,lit(None).alias('receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount')\
        ,lit(None).alias('receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscountFull')\
        ,lit(None).alias('receivableSubledgerBestPossibleNetDue')\
        ,lit(None).alias('receivableSubledgerBestPossibleNetDueFull')\
        ,lit(None).alias('receivableSubledgerAgingBasedOnDueDateCashDiscount')\
        ,col('arv1.cashDiscountLocalCurrency').alias('receivableSubledgerCashDiscountLocalCurrency')\
        ,lit(None).alias('receivableSubledgerAgingBasedOnDueDateNetDue')\
        ,col('arv1.debitCreditIndicator').alias('receivableSubledgerDebitCreditIndicator')\
        ,col('arv1.postingDescription').alias('receivableSubledgerPostingdescription')\
        ,col('arv1.referenceDocumentNumber').alias('receivableSubledgerSubledgerDocumentNumber')\
        ,col('arv1.dataOrigin').alias('receivableSubledgerDataOrigin')\
        ,lit('').alias('receivableSubledgerCustomerScenarioID')\
        ,when(col('rec1.customerNumber').isin('MANUAL', '#NA#', ''),lit('1'))\
                  .otherwise(lit('3')).alias('receivableSubledgerCustomerMDStatusID')\
        ,lit(v_receivableSubledgerScenarioIDYE).alias('receivableSubledgerScenarioIDYE')\
        ,col('rap1.cutOffDate').alias('receivableSubledgerDatePerspective')\
        ,lit(v_receivableSubledgerClearingStatus).alias('receivableSubledgerClearingStatus')\
        ,lit(None).alias('receivableSubledgerAgingDeteriorationProfile')\
        ,col('arv1.postingKey').alias('receivableSubledgerPostingKey')\
        ,col('arv1.amountLC').alias('receivableSubledgerAmountLC')\
        ,lit(None).alias('receivableSubledgerAgingDueDateCashDicountPeriodEnd')\
        ,lit(None).alias('receivableSubledgerAgingInvoiceDatePeriodEndID')\
        ,lit('').alias('receivableSubledgerDueDateAgeID')\
        ,lit('').alias('receivableSubledgerDSOAgeID')\
        ,when(col('rec1.customerNumber').isNull(),lit('1'))\
         .when(col('rec1.customerNumber').isin('MANUAL', '#NA#', ''),lit('1'))\
         .otherwise(lit('2')).alias('receivableSubledgerMissingCustomerMDStatusID')\
        ,when(col('ctmr2.blockType').like('%Delivery Block%'),lit(1))\
          .otherwise(lit(0)).alias('receivableSubledgerISCustomerBlockedForDelivery')\
        ,when(col('ctmr2.blockType').like('%Invoice Block%'),lit(1))\
          .otherwise(lit(0)).alias('receivableSubledgerISCustomerBlockedForInvoices')\
        ,when(col('ctmr2.blockType').like('%Order Block%'),lit(1))\
          .otherwise(lit(0)).alias('receivableSubledgerISCustomerBlockedForOrders')\
        ,when(col('ctmr2.blockType').like('%Posting Block%'),lit(1))\
          .otherwise(lit(0)).alias('receivableSubledgerISCustomerBlockedForPosting')\
        ,col('ctmr2.creditLimitLC').alias('receivableSubledgerCreditLimitAmountLC')\
        ,lit('').alias('receivableSubledgerCreditLimitRange')\
        ,lit('').alias('receivableSubledgerCreditLimitRangeID')\
        ,coalesce(col('bill.billingDocumentSalesOrganization'),lit('#NA#')).alias('billingDocumentSalesOrganization')\
        ,coalesce(col('bill.billingDocumentDistributionChannel'),lit('#NA#')).alias('billingDocumentDistributionChannel')\
        ,coalesce(col('bill.billingDocumentDivision'),lit('#NA#')).alias('billingDocumentDivision')\
        ,col('arv1.GLJEPostingDate')\
        ,col('arv1.accountReceivablePaymentTerm').alias('paymentTerm')\
        ,col('arv1.accountReceivablePaymentTermDayLimit').alias('paymentTermDayLimit')\
        ,col('arv1.accountReceivableCalculationBaselineDate').alias('paymentTermCalculationBaselineDate')\
        ,col('arv1.accountReceivableCashDiscountDayForPayment1').alias('paymentTermCashDiscountDayForPayment1')\
        ,col('arv1.accountReceivableCashDiscountPercentageForPayment1')\
              .alias('paymentTermCashDiscountPercentageForPayment1')\
        ,col('arv1.accountReceivableCashDiscountDayForPayment2')\
              .alias('paymentTermCashDiscountDayForPayment2')\
        ,col('arv1.accountReceivableCashDiscountPercentageForPayment2')\
              .alias('paymentTermCashDiscountPercentageForPayment2')\
        ,col('arv1.accountReceivableDayForNetPayment').alias('paymentTermDayForNetPayment')\
        ,col('arv1.cashDiscountLocalCurrency').alias('cashDiscountAmountLC')\
        ,col('arv1.amountLocalCurrency').alias('amountLC')\
        ,lit(v_tpt2_paymentTerm).alias('tpt2_paymentTerm')\
        ,lit(v_tpt2_paymentTermDayLimit).alias('tpt2_paymentTermDayLimit')\
        ,lit(v_tpt2_paymentTermCashDiscountDayForPayment1).alias('tpt2_paymentTermCashDiscountDayForPayment1')\
        ,lit(v_tpt2_paymentTermCashDiscountDayForPayment2).alias('tpt2_paymentTermCashDiscountDayForPayment2')\
        ,lit(v_tpt2_paymentTermDayForNetPayment).alias('tpt2_paymentTermDayForNetPayment')\
             )
    
    ##Added missing column 
    #receivableSubledgerPaymentDueDate=	isnull(tpt1.paymentTermPaymentDueDate,arv1.GLJEPostingDate)
    #receivableSubledgerPaymentDueDateStatus
    v_paymentTermPaymentDueDate=when(col('paymentTermCalculationBaselineDate').isNull(),lit(None))\
        .when(col('paymentTermCalculationBaselineDate').isin(defaultMaxDate,defaultDate),lit(None))\
        .when(((col('paymentTermDayForNetPayment').isNotNull()) & (col('paymentTermDayForNetPayment') !=0 ))\
              ,expr("date_add(paymentTermCalculationBaselineDate , int(paymentTermDayForNetPayment))") )\
        .when(((col('paymentTermCashDiscountDayForPayment2').isNotNull())\
               &(col('paymentTermCashDiscountDayForPayment2')!=0 ))\
              ,expr("date_add(paymentTermCalculationBaselineDate , int(paymentTermCashDiscountDayForPayment2))") )\
        .when(((col('paymentTermCashDiscountDayForPayment1').isNotNull())\
               &(col('paymentTermCashDiscountDayForPayment1')!=0 ))\
              ,expr("date_add(paymentTermCalculationBaselineDate , int(paymentTermCashDiscountDayForPayment1))") )\
        .otherwise(col('paymentTermCalculationBaselineDate'))

    df_L1_STG_ReceivableSubledger_TMP=df_L1_STG_ReceivableSubledger_TMP\
          .withColumn("paymentTermPaymentDueDate",lit(v_paymentTermPaymentDueDate))

    v_receivableSubledgerPaymentDueDate=coalesce(col('paymentTermPaymentDueDate'),col('GLJEPostingDate'))
    v_receivableSubledgerPaymentDueDateStatus=when(col('paymentTermPaymentDueDate').isNull(),lit(0))\
          .otherwise(lit(1))

    # receivableSubledgerCashDiscountDueDateBestPossible	=	tpt1.paymentTermCashDiscountDueDateBestPossible
    v_paymentTermCashDiscountDueDateBestPossible=\
        when(col('paymentTermCalculationBaselineDate').isNull(),lit(defaultDate))\
        .when(col('paymentTermCalculationBaselineDate').isin(defaultMaxDate,defaultDate),lit(defaultDate))\
        .when(((col('paymentTermCashDiscountDayForPayment1').isNotNull())\
               &(col('paymentTermCashDiscountDayForPayment1')!=0 ))\
              ,expr("date_add(paymentTermCalculationBaselineDate , int(paymentTermCashDiscountDayForPayment1))") )\
        .when(((col('paymentTermCashDiscountDayForPayment2').isNotNull())\
               &(col('paymentTermCashDiscountDayForPayment2')!=0 ))\
              ,expr("date_add(paymentTermCalculationBaselineDate , int(paymentTermCashDiscountDayForPayment2))") )\
        .otherwise(col('paymentTermCalculationBaselineDate'))

    df_L1_STG_ReceivableSubledger_TMP=df_L1_STG_ReceivableSubledger_TMP\
          .withColumn("receivableSubledgerPaymentDueDate",v_receivableSubledgerPaymentDueDate)\
          .withColumn("receivableSubledgerPaymentDueDateStatus",v_receivableSubledgerPaymentDueDateStatus)\
          .withColumn("receivableSubledgerCashDiscountDueDateBestPossible"\
                                                    ,v_paymentTermCashDiscountDueDateBestPossible)

    #isClearedWithinDeadline
    v_dateadd=expr("date_add(receivableSubledgerPaymentDueDate , int(3))")
    v_isClearedWithinDeadline=  when(col('receivableSubledgerClearingDocumentPostingDate')<=lit(v_dateadd),lit(1))\
        .otherwise(lit(0))
    #receivableSubledgerDaysSalesOutstandingID
    v_receivableSubledgerDaysSalesOutstandingID=when(col('paymentTermPaymentDueDate').isNull(),\
                            (when(col('GLJEPostingDate').isNull(),lit('-100001'))\
                             .when(col('GLJEPostingDate')<lit(startDate),lit('-100001'))\
                             .otherwise(lit('100001'))\
                             ))\
                        .otherwise(lit('DSO_TYPE_1'))

    v_receivableSubledgerDatePerspectiveRankID= F.dense_rank().over(Window.partitionBy("idGLJE")\
                                                     .orderBy("receivableSubledgerDatePerspective"))

    #hasPaymentTermChangedFromMasterData
    v_hasPaymentTermChangedFromMasterData=\
        when(col('receivableSubledgerPaymentTerm') != col('tpt2_paymentTerm'),lit(1))\
       .when(col('receivableSubledgerPaymentTermDayLimit') != col('tpt2_paymentTermDayLimit'),lit(1))\
       .when(col('receivableSubledgerCashDiscountDayForPayment1') \
             != col('tpt2_paymentTermCashDiscountDayForPayment1'),lit(1))\
       .when(col('receivableSubledgerCashDiscountDayForPayment1') \
             != col('tpt2_paymentTermCashDiscountDayForPayment2'),lit(1))\
       .when(col('receivableSubledgerDayForNetPayment') \
             != col('tpt2_paymentTermDayForNetPayment'),lit(1))\
       .otherwise(lit(0))
    #dueDateChangeFromMasterDataInDays
    v_dueDateChangeFromMasterDataInDays=coalesce(lit(col('receivableSubledgerDayForNetPayment')-\
                                                    col('tpt2_paymentTermDayForNetPayment')),lit(0))
    #isCleared
    v_isCleared=\
        when(coalesce(col('receivableSubledgerClearingDocumentNumber'),lit('')) == lit(''),lit(0))\
       .when(col('receivableSubledgerClearingDocumentPostingDate').between(lit(startDate),lit(endDate)),lit(1))\
       .otherwise(lit(0))
    
    df_L1_STG_ReceivableSubledger_TMP=df_L1_STG_ReceivableSubledger_TMP\
      .withColumn("isClearedWithinDeadline",v_isClearedWithinDeadline)\
      .withColumn("receivableSubledgerDaysSalesOutstandingID",v_receivableSubledgerDaysSalesOutstandingID)\
      .withColumn("receivableSubledgerDatePerspectiveRankID",v_receivableSubledgerDatePerspectiveRankID)\
      .withColumn("hasPaymentTermChangedFromMasterData",v_hasPaymentTermChangedFromMasterData)\
      .withColumn("dueDateChangeFromMasterDataInDays",v_dueDateChangeFromMasterDataInDays)\
      .withColumn("isCleared",v_isCleared)\
    
    #receivableSubledgerScenarioID
    v_receivableSubledgerScenarioID=\
      when(col('receivableSubledgerDateOpenSince') > col('receivableSubledgerDatePerspective'),lit(5))\
      .when(col('receivableSubledgerDateOpenTill').between(lit(startDate),lit(endDate)),lit(1))\
      .when(col('receivableSubledgerDateOpenTill') > col('receivableSubledgerDatePerspective'),lit(7))\
      .when(col('receivableSubledgerDateOpenTill') <= col('receivableSubledgerDatePerspective'),lit(6))\
      .otherwise(lit(-1))

    #receivableSubledgerCustomerBlockStatusID
    v_receivableSubledgerCustomerBlockStatusID_0= \
      when(col('receivableSubledgerISCustomerBlockedForDelivery') ==1,lit(1)).otherwise(lit(0)) \
      + when(col('receivableSubledgerISCustomerBlockedForInvoices') ==1,lit(1)).otherwise(lit(0)) \
      + when(col('receivableSubledgerISCustomerBlockedForOrders') ==1,lit(1)).otherwise(lit(0)) \
      + when(col('receivableSubledgerISCustomerBlockedForPosting') ==1,lit(1)).otherwise(lit(0))

    v_receivableSubledgerCustomerBlockStatusID=\
      when(lit(v_receivableSubledgerCustomerBlockStatusID_0)==lit(1),lit('1'))\
      .when(lit(v_receivableSubledgerCustomerBlockStatusID_0)>lit(1),lit('2'))\
      .when(lit(v_receivableSubledgerCustomerBlockStatusID_0)==lit(0),lit('3'))\
      .otherwise(lit('4'))

    ##Updation From Reference SQL Script 
    #receivableSubledgerDaysSalesOutstanding
    v_rscdpDate_CLtmp=\
      when(col('receivableSubledgerClearingDocumentPostingDate').isNull()\
                           ,col('receivableSubledgerDatePerspective'))\
      .when(col('receivableSubledgerClearingDocumentPostingDate') == lit(defaultDate)\
              ,col('receivableSubledgerDatePerspective'))\
      .when(col('receivableSubledgerClearingDocumentPostingDate') > col('receivableSubledgerDatePerspective')\
              ,col('receivableSubledgerDatePerspective'))\
      .otherwise(col('receivableSubledgerClearingDocumentPostingDate') )

    v_rscdpDate_OPtmp=\
      when(col('receivableSubledgerClearingDocumentPostingDate').isNull()\
                           ,col('receivableSubledgerDatePerspective'))\
      .when(col('receivableSubledgerClearingDocumentPostingDate') == lit(defaultDate)\
            ,col('receivableSubledgerDatePerspective'))\
      .when(col('receivableSubledgerClearingDocumentPostingDate') > col('receivableSubledgerDatePerspective')\
            ,col('receivableSubledgerDatePerspective'))\
      .when((col('receivableSubledgerClearingDocumentPostingDate') < col('receivableSubledgerDateOpenTill'))\
            & (col('receivableSubledgerDateOpenTill') > col('receivableSubledgerDatePerspective'))\
            ,col('receivableSubledgerDatePerspective'))\
      .when((col('receivableSubledgerClearingDocumentPostingDate') < col('receivableSubledgerDateOpenTill'))\
          & (col('receivableSubledgerDateOpenTill') < col('receivableSubledgerDatePerspective'))\
          ,col('receivableSubledgerDateOpenTill'))\
      .otherwise(col('receivableSubledgerClearingDocumentPostingDate'))

    ##Update  based on receivableSubledgerClearingStatus = 'CL','OP'
    v_receivableSubledgerDaysSalesOutstanding=\
      when(col('receivableSubledgerClearingStatus')=='CL',\
        (\
          when((col('receivableSubledgerClearingDocumentPostingDate')< col('receivableSubledgerInvoiceDate'))\
              & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
               ,lit(0)\
              )\
         .otherwise(datediff(col('receivableSubledgerInvoiceDate'),lit(v_rscdpDate_CLtmp)))\
        )\
      )\
      .when(col('receivableSubledgerClearingStatus')=='OP',\
        (\
          when((col('receivableSubledgerClearingDocumentPostingDate')< col('receivableSubledgerInvoiceDate'))\
            & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
               ,lit(0)\
              )\
         .otherwise(datediff(col('receivableSubledgerInvoiceDate'),lit(v_rscdpDate_OPtmp)))\
        )\
      )\
      .otherwise(lit(None))

    v_receivableSubledgerDaysSalesOutstandingFull=\
      when(col('receivableSubledgerClearingStatus')=='CL',\
        (\
          when((col('receivableSubledgerClearingDocumentPostingDate')< col('receivableSubledgerDocumentDate'))\
              & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
               ,lit(0)\
              )\
         .when((col('receivableSubledgerDocumentDate') > col('receivableSubledgerDatePerspective'))\
               ,lit(0)\
              )\
         .otherwise(datediff(col('receivableSubledgerDocumentDate'),lit(v_rscdpDate_OPtmp)))\
        )\
        )\
      .when(col('receivableSubledgerClearingStatus')=='OP',\
        (\
          when((col('receivableSubledgerClearingDocumentPostingDate')< col('receivableSubledgerDocumentDate'))\
              & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
               ,lit(0)\
              )\
         .when((col('receivableSubledgerDocumentDate') > col('receivableSubledgerDatePerspective'))\
               ,lit(0)\
              )\
         .otherwise(datediff(col('receivableSubledgerDocumentDate'),lit(v_rscdpDate_OPtmp)))\
        )\
      )\
      .otherwise(lit(None))

    v_receivableSubledgerDaysDelinquentNetDue=\
      when(col('receivableSubledgerClearingStatus')=='CL',\
        (\
          when((col('receivableSubledgerClearingDocumentPostingDate')< col('receivableSubledgerPaymentDueDate'))\
              & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
               ,lit(0)\
              )\
          .when((col('receivableSubledgerPaymentDueDate') > col('receivableSubledgerDatePerspective'))\
               ,lit(0)\
              )\
         .otherwise(datediff(col('receivableSubledgerPaymentDueDate'),lit(v_rscdpDate_CLtmp)))\
        )\
      )\
      .when(col('receivableSubledgerClearingStatus')=='OP',\
        (\
          when((col('receivableSubledgerClearingDocumentPostingDate')< col('receivableSubledgerPaymentDueDate'))\
              & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
               ,lit(0)\
              )\
          .when((col('receivableSubledgerPaymentDueDate') > col('receivableSubledgerDatePerspective'))\
               ,lit(0)\
              )\
         .otherwise(datediff(col('receivableSubledgerPaymentDueDate'),lit(v_rscdpDate_OPtmp)))\
        )\
      )\
      .otherwise(lit(0))

    v_receivableSubledgerDaysDelinquentCashDiscount=\
      when(col('receivableSubledgerClearingStatus')=='CL',\
        (\
            when((col('receivableSubledgerClearingDocumentPostingDate')\
                  < col('receivableSubledgerCashDiscountDueDateBestPossible'))\
                & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
                 ,lit(0)\
                )\
           .when((col('receivableSubledgerCashDiscountDueDateBestPossible') \
                  > col('receivableSubledgerDatePerspective'))\
                 ,lit(0)\
                )\
           .otherwise(datediff(col('receivableSubledgerCashDiscountDueDateBestPossible'),lit(v_rscdpDate_CLtmp)))\
          )\
        )\
      .when(col('receivableSubledgerClearingStatus')=='OP',\
        (\
          when((col('receivableSubledgerClearingDocumentPostingDate')\
                < col('receivableSubledgerCashDiscountDueDateBestPossible'))\
              & (coalesce(col('receivableSubledgerClearingDocumentPostingDate'),lit(defaultDate)) >lit(defaultDate))\
               ,lit(0)\
              )\
         .when((col('receivableSubledgerCashDiscountDueDateBestPossible') \
                > col('receivableSubledgerDatePerspective'))\
               ,lit(0)\
              )\
         .otherwise(datediff(col('receivableSubledgerCashDiscountDueDateBestPossible'),lit(v_rscdpDate_OPtmp)))\
        )\
      )\
      .otherwise(lit(0))

    v_receivableSubledgerBestPossibleNetDue=\
      when(col('receivableSubledgerClearingStatus').isin('CL','OP')\
              ,datediff(col('receivableSubledgerInvoiceDate'),col('receivableSubledgerPaymentDueDate'))
           )\
      .otherwise(lit(None))
    
    v_receivableSubledgerBestPossibleNetDueFull=\
      when(col('receivableSubledgerClearingStatus').isin('CL','OP')\
               ,datediff(col('receivableSubledgerDocumentDate'),col('receivableSubledgerPaymentDueDate'))
           )\
      .otherwise(lit(None))
    
    v_receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount=\
      when((col('receivableSubledgerClearingStatus').isin('CL','OP'))\
          & (coalesce(col('receivableSubledgerCashDiscountDueDateBestPossible'),lit(defaultDate)) != lit(defaultDate))\
          ,datediff(col('receivableSubledgerInvoiceDate'),col('receivableSubledgerCashDiscountDueDateBestPossible'))
          )\
      .when(col('receivableSubledgerClearingStatus').isin('CL','OP'),lit(0))\
      .otherwise(lit(None))

    v_receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscountFull=\
      when((col('receivableSubledgerClearingStatus').isin('CL','OP'))\
          & (coalesce(col('receivableSubledgerCashDiscountDueDateBestPossible'),lit(defaultDate)) != lit(defaultDate))\
          ,datediff(col('receivableSubledgerDocumentDate'),col('receivableSubledgerCashDiscountDueDateBestPossible'))
          )\
      .when(col('receivableSubledgerClearingStatus').isin('CL','OP'),lit(0))\
      .otherwise(lit(None))

    #----
    #receivableSubledgerAgingDueDateNetDuePeriodEnd
    v_receivableSubledgerAgingDueDateNetDuePeriodEnd=\
      when(col('receivableSubledgerClearingStatus')=='OP',\
        datediff(col('receivableSubledgerPaymentDueDate'),col('receivableSubledgerDatePerspective'))
          )\
      .when(col('receivableSubledgerClearingStatus')=='CL',\
        datediff(col('receivableSubledgerPaymentDueDate'),col('receivableSubledgerClearingDocumentPostingDate'))
          )\
      .otherwise(lit(None))
    #receivableSubledgerAgingDueDateCashDicountPeriodEnd
    v_receivableSubledgerAgingDueDateCashDicountPeriodEnd=\
      when((col('receivableSubledgerClearingStatus')==lit('OP'))\
            & (coalesce(col('receivableSubledgerCashDiscountDueDateBestPossible'),lit(defaultDate)) \
               != lit(defaultDate))\
            ,datediff(col('receivableSubledgerCashDiscountDueDateBestPossible')\
                      ,col('receivableSubledgerDatePerspective'))
            )\
      .when(col('receivableSubledgerClearingStatus')==lit('OP'),lit(0))\
      .when((col('receivableSubledgerClearingStatus')==lit('CL'))\
            & (coalesce(col('receivableSubledgerCashDiscountDueDateBestPossible'),lit(defaultDate)) \
               != lit(defaultDate))\
            ,datediff(col('receivableSubledgerCashDiscountDueDateBestPossible'),\
                      col('receivableSubledgerClearingDocumentPostingDate'))\
            )\
      .when(col('receivableSubledgerClearingStatus')==lit('CL'),lit(0))\
      .otherwise(lit(None))

    #receivableSubledgerAgingInvoiceDatePeriodEndID
    v_receivableSubledgerAgingInvoiceDatePeriodEndID=\
      when(col('receivableSubledgerClearingStatus')=='OP',\
        datediff(col('receivableSubledgerInvoiceDate'),col('receivableSubledgerDatePerspective'))
          )\
      .when(col('receivableSubledgerClearingStatus')=='CL',\
        datediff(col('receivableSubledgerInvoiceDate'),col('receivableSubledgerClearingDocumentPostingDate'))
          )\
      .otherwise(lit(None))
    #receivableSubledgerTradePastDueAmount
    v_receivableSubledgerTradePastDueAmount=\
      when(coalesce(col('receivableSubledgerAgingDueDateNetDuePeriodEnd'),lit(0)) > lit(0)\
               ,col('receivableSubledgerAmountReceivableTrade'))\
      .otherwise(lit(0.0))

    df_L1_STG_ReceivableSubledger_TMP=df_L1_STG_ReceivableSubledger_TMP\
      .withColumn("receivableSubledgerScenarioID",v_receivableSubledgerScenarioID)\
      .withColumn("receivableSubledgerCustomerBlockStatusID",v_receivableSubledgerCustomerBlockStatusID)\
      .withColumn("rscdpDate_CLtmp",v_rscdpDate_CLtmp)\
      .withColumn("rscdpDate_OPtmp",v_rscdpDate_OPtmp)\
      .withColumn("receivableSubledgerDaysSalesOutstanding",v_receivableSubledgerDaysSalesOutstanding)\
      .withColumn("receivableSubledgerDaysSalesOutstandingFull",v_receivableSubledgerDaysSalesOutstandingFull)\
      .withColumn("receivableSubledgerDaysDelinquentNetDue",v_receivableSubledgerDaysDelinquentNetDue)\
      .withColumn("receivableSubledgerDaysDelinquentCashDiscount",v_receivableSubledgerDaysDelinquentCashDiscount)\
      .withColumn("receivableSubledgerBestPossibleNetDue",v_receivableSubledgerBestPossibleNetDue)\
      .withColumn("receivableSubledgerBestPossibleNetDueFull",v_receivableSubledgerBestPossibleNetDueFull)\
      .withColumn("receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount"\
                  ,v_receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount)\
      .withColumn("receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscountFull"\
                  ,v_receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscountFull)\
      .withColumn("receivableSubledgerAgingDueDateNetDuePeriodEnd"\
                  ,v_receivableSubledgerAgingDueDateNetDuePeriodEnd)\
      .withColumn("receivableSubledgerAgingDueDateCashDicountPeriodEnd"\
                  ,v_receivableSubledgerAgingDueDateCashDicountPeriodEnd)\
      .withColumn("receivableSubledgerAgingInvoiceDatePeriodEndID"\
                  ,v_receivableSubledgerAgingInvoiceDatePeriodEndID)\
      .withColumn("receivableSubledgerTradePastDueAmount"\
                  ,v_receivableSubledgerTradePastDueAmount)
    
    
    ##Calculate "receivableSubledgerAgingDeterioration"
    df_L1_TMP_RevenueARDSOAverage=df_L1_STG_ReceivableSubledger_TMP.alias("rsgr")\
      .filter((col('receivableSubledgerClearingStatus')=='OP')\
             & (col('receivableSubledgerDaysSalesOutstandingID')=='DSO_TYPE_1')\
             )\
      .groupBy("rsgr.receivableSubledgerCompanyCode"\
               ,"rsgr.receivableSubledgerCustomerNumber"\
               ,"rsgr.receivableSubledgerDatePerspective")\
      .agg(avg("receivableSubledgerDaysSalesOutstanding").alias("receivableSubledgerDaysSalesOutstandingAverage")\
         )

    df_L1_TMP_RevenueARDSOAverageMinMax=df_L1_TMP_RevenueARDSOAverage.alias("ardso")\
      .groupBy("ardso.receivableSubledgerCompanyCode"\
               ,"ardso.receivableSubledgerCustomerNumber"\
              )\
      .agg(min("receivableSubledgerDatePerspective").alias("receivableSubledgerDatePerspectiveMin")\
           ,max("receivableSubledgerDatePerspective").alias("receivableSubledgerDatePerspectiveMax")\
         )
    #df_L1_TMP_RevenueARDSOAverageMinMax.display()

    df_L1_TMP_RevenueARDSOAverageMin=df_L1_TMP_RevenueARDSOAverage.alias("rada1")\
      .join(df_L1_TMP_RevenueARDSOAverageMinMax.alias("radm1")\
          ,((col('rada1.receivableSubledgerCompanyCode') == col("radm1.receivableSubledgerCompanyCode"))\
            & (col('rada1.receivableSubledgerCustomerNumber') == col("radm1.receivableSubledgerCustomerNumber"))\
            & (col('rada1.receivableSubledgerDatePerspective') == col("radm1.receivableSubledgerDatePerspectiveMin"))\
           ),"inner")\
        .select(col('rada1.receivableSubledgerCompanyCode')\
           ,col('rada1.receivableSubledgerCustomerNumber')\
           ,col('rada1.receivableSubledgerDatePerspective')\
           ,col('rada1.receivableSubledgerDaysSalesOutstandingAverage')\
               )

    df_L1_TMP_RevenueARDSOAverageMax=df_L1_TMP_RevenueARDSOAverage.alias("rada1")\
      .join(df_L1_TMP_RevenueARDSOAverageMinMax.alias("radm2")\
          ,((col('rada1.receivableSubledgerCompanyCode') == col("radm2.receivableSubledgerCompanyCode"))\
            & (col('rada1.receivableSubledgerCustomerNumber') == col("radm2.receivableSubledgerCustomerNumber"))\
            & (col('rada1.receivableSubledgerDatePerspective') == col("radm2.receivableSubledgerDatePerspectiveMax"))\
           ),"inner")\
      .select(col('rada1.receivableSubledgerCompanyCode')\
         ,col('rada1.receivableSubledgerCustomerNumber')\
         ,col('rada1.receivableSubledgerDatePerspective')\
         ,col('rada1.receivableSubledgerDaysSalesOutstandingAverage')\
             )
    df_L1_TMP_RevenueARDSOTrend=df_L1_TMP_RevenueARDSOAverage.alias("rada1")\
      .join(df_L1_TMP_RevenueARDSOAverageMin.alias("radmn1")\
          ,((col('rada1.receivableSubledgerCompanyCode') == col("radmn1.receivableSubledgerCompanyCode"))\
            & (col('rada1.receivableSubledgerCustomerNumber') == col("radmn1.receivableSubledgerCustomerNumber"))\
            ),"inner")\
      .join(df_L1_TMP_RevenueARDSOAverageMax.alias("radmx1")\
          ,((col('rada1.receivableSubledgerCompanyCode') == col("radmx1.receivableSubledgerCompanyCode"))\
            & (col('rada1.receivableSubledgerCustomerNumber') == col("radmx1.receivableSubledgerCustomerNumber"))\
            ),"inner")\
      .groupBy(col("rada1.receivableSubledgerCompanyCode").alias("receivableSubledgerCompanyCode")\
           ,col("rada1.receivableSubledgerCustomerNumber").alias("receivableSubledgerCustomerNumber")\
           ,col("radmn1.receivableSubledgerDatePerspective").alias("receivableSubledgerEarliestDSOPeriod")\
           ,col("radmn1.receivableSubledgerDaysSalesOutstandingAverage").alias("receivableSubledgerEarliestDSO")\
           ,col("radmx1.receivableSubledgerDatePerspective").alias("receivableSubledgerLatestDSOPeriod")\
           ,col("radmx1.receivableSubledgerDaysSalesOutstandingAverage").alias("receivableSubledgerLatestDSO")\
          )\
      .agg(count(col('*')))\
      .select(col('receivableSubledgerCompanyCode')\
              ,col('receivableSubledgerCustomerNumber')\
              ,col('receivableSubledgerEarliestDSOPeriod')\
              ,col('receivableSubledgerEarliestDSO')\
              ,col('receivableSubledgerLatestDSOPeriod')\
              ,col('receivableSubledgerLatestDSO')\
              ,lit(col('receivableSubledgerLatestDSO')-col('receivableSubledgerEarliestDSO'))\
                .alias('receivableSubledgerDaySalesOutstandingDifferenceDays')
             )

    v_receivableSubledgerAgingDeterioration=\
      when(((col('radt1.receivableSubledgerCompanyCode').isNull()) \
             & (col('radt1.receivableSubledgerCustomerNumber').isNull())) ,lit(None) )\
      .otherwise(col('radt1.receivableSubledgerDaySalesOutstandingDifferenceDays'))

    df_L1_STG_ReceivableSubledger_TMP=df_L1_STG_ReceivableSubledger_TMP.alias("rst1")\
       .join(df_L1_TMP_RevenueARDSOTrend.alias("radt1")\
          ,((col('rst1.receivableSubledgerCompanyCode').eqNullSafe(col("radt1.receivableSubledgerCompanyCode")))\
           & (col('rst1.receivableSubledgerCustomerNumber').eqNullSafe(col("radt1.receivableSubledgerCustomerNumber")))\
           ),"left")\
      .select(col("rst1.*")\
              ,lit(v_receivableSubledgerAgingDeterioration).alias('receivableSubledgerAgingDeterioration')\
              ,lit(v_surrogateKey).alias('tmp_ID')\
             )     
    df_L1_STG_ReceivableSubledger_TMP.persist()  
    
    #[fin].[L1_TMP_RevenueProfile]
    v_avgreceivableSubledgerBestPossibleDaysSalesOutstanding=\
       when(col('rsgr.receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount') >\
                 col('rsgr.receivableSubledgerBestPossibleNetDue')\
             ,col('rsgr.receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount'))\
       .otherwise(col('rsgr.receivableSubledgerBestPossibleNetDue'))

    df_L1_TMP_RevenueProfile=df_L1_STG_ReceivableSubledger_TMP.alias("rsgr")\
       .filter(col('receivableSubledgerScenarioID').isin('1','7'))\
       .groupBy(col("rsgr.receivableSubledgerCompanyCode").alias("receivableSubledgerCompanyCode")\
             ,col("rsgr.receivableSubledgerCustomerNumber").alias("receivableSubledgerCustomerNumber")\
             ,col("rsgr.receivableSubledgerDatePerspective").alias("receivableSubledgerDatePerspective")\
            )\
       .agg(avg("rsgr.receivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount")\
                   .alias("avgreceivableSubledgerBestPossibleDaysSalesOutstandingCashDiscount")\
               ,avg("rsgr.receivableSubledgerBestPossibleNetDue")\
                   .alias("avgreceivableSubledgerBestPossibleNetDue")\
               ,avg(lit(v_avgreceivableSubledgerBestPossibleDaysSalesOutstanding))\
                   .alias("avgreceivableSubledgerBestPossibleDaysSalesOutstanding")
               ,avg("rsgr.receivableSubledgerDaysSalesOutstanding")\
                   .alias("avgreceivableSubledgerDaysSalesOutstanding")\
           )

    #receivableSubledgerPaymentTrend
    v_receivableSubledgerPaymentTrend=\
       col('avgreceivableSubledgerDaysSalesOutstanding')-col('avgreceivableSubledgerBestPossibleDaysSalesOutstanding')

    v_receivableSubledgerPaymentTrendProfile=\
       when(col('avgreceivableSubledgerDaysSalesOutstanding') \
               > col('avgreceivableSubledgerBestPossibleDaysSalesOutstanding')\
              ,lit('Deterioriating payment trend'))\
       .otherwise(lit('Not deteriorating payment trend'))

    df_L1_TMP_RevenueProfile=df_L1_TMP_RevenueProfile\
       .withColumn("receivableSubledgerPaymentTrend",v_receivableSubledgerPaymentTrend)\
       .withColumn("receivableSubledgerPaymentTrendProfile",v_receivableSubledgerPaymentTrendProfile)

    #df_L1_TMP_RevenueProfile.display()   
    df_L1_STG_ReceivableSubledger_TMPL1=df_L1_STG_ReceivableSubledger_TMP.alias("rsl1")\
       .join(df_L1_TMP_RevenueProfile.alias("rpf1")\
          ,((col('rsl1.receivableSubledgerCompanyCode')==col("rpf1.receivableSubledgerCompanyCode"))\
            & (col('rsl1.receivableSubledgerCustomerNumber')==col("rpf1.receivableSubledgerCustomerNumber"))\
            & (col('rsl1.receivableSubledgerDatePerspective')==col("rpf1.receivableSubledgerDatePerspective"))\
            ),"inner")\
       .select(col("rpf1.receivableSubledgerPaymentTrend").alias('receivableSubledgerPaymentTrend')\
           ,col("rpf1.receivableSubledgerPaymentTrendProfile").alias('receivableSubledgerPaymentTrendProfile')\
           ,lit(0).alias('receivableSubledgerAgingProfile')\
           ,col("rsl1.tmp_ID")\
               )
    #[fin].[L1_TMP_RevenueARbyCustomer] 
    df_L1_TMP_RevenueARbyCustomer=df_L1_STG_ReceivableSubledger_TMP.alias("rsl1")\
        .join(otc_L1_MD_Customer.alias("ctmr1")\
            ,((col('rsl1.receivableSubledgerCompanyCode').eqNullSafe(col("ctmr1.companyCode")))\
              & (col('rsl1.receivableSubledgerCustomerNumber').eqNullSafe(col("ctmr1.customerNumber")))\
              ),"left")\
        .filter((col('rsl1.receivableSubledgerScenarioID').isin('1','7'))\
                 & (coalesce(col('rsl1.receivableSubledgerDatePerspectiveRankID'),lit(0) )==1))\
        .groupBy(col('rsl1.receivableSubledgerERPClient'),col('rsl1.receivableSubledgerCompanyCode')\
                ,col('rsl1.receivableSubledgerCustomerNumber'),col('ctmr1.creditLimit')\
                ,col('ctmr1.customerCreditControlArea')\
                 ,when(col("ctmr1.blockType").isNull(),lit(-1))\
                 .when(col("ctmr1.blockType")==lit('Not Blocked'),lit(0))\
                 .otherwise(lit(1)).alias('customerBlocked')
                )\
        .agg(sum(col('receivableSubledgerTradeDiscountBestPossibleAmount'))\
                 .alias('receivableSubledgerTradeDiscountBestPossibleAmount')\
             ,sum(col('receivableSubledgerAmountReceivableTrade'))\
                 .alias('receivableSubledgerAmountReceivableTrade')\
            )\
        .select(col("receivableSubledgerERPClient").alias('receivableSubledgerERPClient')\
               ,col("receivableSubledgerCompanyCode").alias('receivableSubledgerCompanyCode')\
               ,col("receivableSubledgerCustomerNumber").alias('receivableSubledgerCustomerNumber')\
               ,col("customerCreditControlArea").alias('customerCreditControlArea')\
               ,col("creditLimit").alias('customerCreditLimit')\
               ,col("customerBlocked")\
               ,col('receivableSubledgerTradeDiscountBestPossibleAmount')\
               ,col('receivableSubledgerAmountReceivableTrade') 
               )

    # #ARCreditLimitRatio
    v_ARCreditLimitRatio=\
          when((col('receivableSubledgerTradeDiscountBestPossibleAmount') > lit(0.0))\
              & (col('customerCreditLimit') > lit(0.0)),\
               (col('receivableSubledgerTradeDiscountBestPossibleAmount')/col('customerCreditLimit')))\
          .otherwise(lit(0.0))
    v_ARBalanceStatus=\
           when(col('receivableSubledgerAmountReceivableTrade') < lit(0.0),lit(2))\
           .when(col('receivableSubledgerAmountReceivableTrade') == lit(0.0),lit(3))\
           .when(col('receivableSubledgerAmountReceivableTrade') > lit(0.0),lit(1))\
           .otherwise(lit(4))

    v_receivableSubledgerNetNegARStatusID=\
           when(col('receivableSubledgerAmountReceivableTrade') < lit(0.0),lit(1))\
           .when(col('receivableSubledgerAmountReceivableTrade') >= lit(0.0),lit(2))\
           .otherwise(lit(3))

    v_receivableSubledgerCustomerScenarioID=\
          when(col('receivableSubledgerCustomerNumber').isin('MANUAL','#NA#',''),lit(1))\
          .when(col('receivableSubledgerAmountReceivableTrade') < lit(0.0),lit(2))\
          .when(col('receivableSubledgerAmountReceivableTrade') == lit(0.0),lit(3))\
          .when(col('receivableSubledgerAmountReceivableTrade') > lit(0.0),lit(4))       

    df_L1_TMP_RevenueARbyCustomer=df_L1_TMP_RevenueARbyCustomer\
        .withColumn("ARCreditLimitRatio",v_ARCreditLimitRatio)\
        .withColumn("ARBalanceStatus",v_ARBalanceStatus)\
        .withColumn("receivableSubledgerNetNegARStatusID",v_receivableSubledgerNetNegARStatusID)\
        .withColumn("receivableSubledgerCustomerScenarioID",v_receivableSubledgerCustomerScenarioID)\

    df_L2_DIM_BracketAll=gen_L2_DIM_Bracket.alias('brc2')\
        .join(gen_L2_DIM_BracketText.alias("brt2")\
            ,((col('brc2.bracketSurrogateKey')==col('brt2.bracketSurrogateKey'))\
              & (col('brt2.bracketType') == lit('CREDIT_LIMIT_RATIO'))),"inner")\
        .select(col('brt2.bracketTextSurrogateKey'),col('brc2.lowerLimit'),col('brc2.upperLimit'))
    
    v_ARCreditLimitRatioID=coalesce(col('brct.bracketTextSurrogateKey'),lit(None))

    df_L1_TMP_RevenueARbyCustomer_final=df_L1_TMP_RevenueARbyCustomer.alias("rsl1")\
      .join(df_L2_DIM_BracketAll.alias("brct"),\
            ((col('rsl1.ARCreditLimitRatio') > col('brct.lowerLimit'))\
            & (col('rsl1.ARCreditLimitRatio') <= col('brct.upperLimit'))),"left")\
      .select(col('rsl1.*'),lit(v_ARCreditLimitRatioID).alias('ARCreditLimitRatioID'))

    v_receivableSubledgerCreditLimitStatusID=\
       when(col('rbc1.receivableSubledgerAmountReceivableTrade') < lit(0.0),lit('4'))\
      .when(col('rbc1.receivableSubledgerAmountReceivableTrade') > col('rbc1.customerCreditLimit'),lit('1'))\
      .when(col('rbc1.receivableSubledgerAmountReceivableTrade')<= col('rbc1.customerCreditLimit'),lit('2'))\
      .when(coalesce(col('rbc1.customerCreditLimit'),lit(0.0))==lit(0.0) ,lit('3'))\
      .otherwise(lit('5'))

    v_receivableSubledgerCreditLimitSetStatusID=\
      when(coalesce(col('rbc1.customerCreditLimit'),lit(0.0))==lit(0.0) ,lit('1'))\
      .otherwise(lit('2'))

    v_receivableSubledgerCreditLimitRatio=coalesce(col('rbc1.ARCreditLimitRatio'),lit(0.0))
    v_receivableSubledgerCreditLimitRatioID=coalesce(col('rbc1.ARCreditLimitRatioID'),lit(0))
    v_receivableSubledgerCustomerScenarioID=coalesce(col('rbc1.receivableSubledgerCustomerScenarioID'),lit(0))
    v_receivableSubledgerQualtiativeStatusID_0= \
      when(col('rbc1.customerBlocked') ==1,lit(1)).otherwise(lit(0)) \
      + when(col('rbc1.receivableSubledgerAmountReceivableTrade') > \
             col('rbc1.customerCreditLimit'),lit(1)).otherwise(lit(0))\
      + when(coalesce(col('rbc1.customerCreditLimit'),lit(0.0))==lit(0.0),lit(1)).otherwise(lit(0))\
      + when(coalesce(col('rsl1.receivableSubledgerAgingDeterioration'),lit(0))>lit(0),lit(1)).otherwise(lit(0))

    v_receivableSubledgerQualtiativeStatusID =when(lit(v_receivableSubledgerQualtiativeStatusID_0)==lit(1),lit('1'))\
      .when(lit(v_receivableSubledgerQualtiativeStatusID_0)>lit(1),lit('2'))\
      .when(lit(v_receivableSubledgerQualtiativeStatusID_0)==lit(0),lit('3'))\
      .otherwise(lit('4'))

    v_receivableSubledgerNetNegARStatusID=coalesce(col('rbc1.receivableSubledgerNetNegARStatusID'),lit(0))

    ##
    v_receivableSubledgerTransactionScenarioID=\
      when(col('rbc1.receivableSubledgerCustomerScenarioID') ==lit('1'),lit(2))\
      .when(col('rbc1.receivableSubledgerCustomerScenarioID') ==lit('2'),lit(1))\
      .otherwise(lit(3))

    df_L1_STG_ReceivableSubledger_TMPL2=df_L1_STG_ReceivableSubledger_TMP.alias("rsl1")\
      .join(df_L1_TMP_RevenueARbyCustomer_final.alias("rbc1")\
        ,((col('rsl1.receivableSubledgerCompanyCode')==col("rbc1.receivableSubledgerCompanyCode"))\
          & (col('rsl1.receivableSubledgerCustomerNumber')==col("rbc1.receivableSubledgerCustomerNumber"))\
          & (col('rsl1.receivableSubledgerERPClient')==col("rbc1.receivableSubledgerERPClient"))\
          ),"inner")\
      .select(coalesce(col('rbc1.ARBalanceStatus'),lit(None)).alias('receivableSubledgerBalanceStatusID')\
        ,lit(v_receivableSubledgerCreditLimitStatusID).alias('receivableSubledgerCreditLimitStatusID')\
        ,lit(v_receivableSubledgerCreditLimitSetStatusID).alias('receivableSubledgerCreditLimitSetStatusID')\
        ,lit(v_receivableSubledgerCreditLimitRatio).alias('receivableSubledgerCreditLimitRatio')\
        ,lit(v_receivableSubledgerCreditLimitRatioID).alias('receivableSubledgerCreditLimitRatioID')\
        ,lit(v_receivableSubledgerQualtiativeStatusID).alias('receivableSubledgerQualtiativeStatusID')\
        ,lit(v_receivableSubledgerNetNegARStatusID).alias('receivableSubledgerNetNegARStatusID')\
        ,lit(v_receivableSubledgerTransactionScenarioID).alias('receivableSubledgerTransactionScenarioID')\
        ,col("rsl1.tmp_ID")\
           )
      
    fin_L1_STG_ReceivableSubledger=df_L1_STG_ReceivableSubledger_TMP.alias("rsl0")\
      .join(df_L1_STG_ReceivableSubledger_TMPL1.alias("rsl1")\
            ,(col("rsl0.tmp_ID").eqNullSafe(col("rsl1.tmp_ID"))),"left")\
      .join(df_L1_STG_ReceivableSubledger_TMPL2.alias("rsl2")\
            ,(col("rsl0.tmp_ID").eqNullSafe(col("rsl2.tmp_ID"))),"left")\
      .select(col('rsl0.*')\
              ,col('rsl1.receivableSubledgerPaymentTrend')\
              ,col('rsl1.receivableSubledgerPaymentTrendProfile')\
              ,col('rsl1.receivableSubledgerAgingProfile')\
              ,col('rsl2.receivableSubledgerBalanceStatusID')\
              ,col('rsl2.receivableSubledgerCreditLimitStatusID')\
              ,col('rsl2.receivableSubledgerCreditLimitSetStatusID')\
              ,col('rsl2.receivableSubledgerCreditLimitRatio')\
              ,col('rsl2.receivableSubledgerCreditLimitRatioID')\
              ,col('rsl2.receivableSubledgerQualtiativeStatusID')\
              ,col('rsl2.receivableSubledgerNetNegARStatusID')\
              ,col('rsl2.receivableSubledgerTransactionScenarioID')\
             )
    
    fin_L1_STG_ReceivableSubledger =objDataTransformation.gen_convertToCDMandCache\
        (fin_L1_STG_ReceivableSubledger,'fin','L1_STG_ReceivableSubledger',True)
      
    executionStatus = "L1_STG_ReceivableSubledger populated sucessfully"
    #executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    #executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

