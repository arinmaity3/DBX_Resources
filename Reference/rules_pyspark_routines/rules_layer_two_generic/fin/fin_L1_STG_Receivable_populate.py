# Databricks notebook source
#fin_L1_STG_Receivable_populate
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when,sum,avg,max,concat,coalesce,abs,trim,floor
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType    
from datetime import datetime
from pyspark.sql.functions import *

def fin_L1_STG_Receivable_populate():
  try:
    global fin_L1_STG_Receivable
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    finYear=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
    periodStart=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START')
    startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    defaultDate='1900-01-01'
    finYear3=int(finYear)+3
    pmaxOpenDate=datetime.strptime(str(finYear3)+"-12-31", '%Y-%m-%d').date()
    v_ID = row_number().over(Window().orderBy(lit('')))

    pBeginOfYear=gen_L1_MD_Period.filter(col('fiscalYear')==finYear)\
      .describe("calendarDate").filter("summary = 'min'")\
      .select("calendarDate").first().asDict()['calendarDate']

    df_LK_CD_GLAccountToFinancialStructureDetail_Acnt_fSID=\
        knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot.alias("gla")\
        .select(col('gla.glAccountNumber'),col('gla.financialStructureDetailID'))\
        .distinct()
    df_LK_CD_GLAccountToFinancialStructureDetail_Acnt=\
        df_LK_CD_GLAccountToFinancialStructureDetail_Acnt_fSID.alias("gla")\
        .select(col('gla.glAccountNumber'))\
        .distinct()
    df_LK_CD_ReportingSetup=knw_LK_CD_ReportingSetup.alias("rpt")\
        .select(col('rpt.clientCode'),col('rpt.companyCode'),col('rpt.chartOfAccount')).distinct()

    df_L1_TD_GLBalance=fin_L1_TD_GLBalance.alias("bal")\
        .join(df_LK_CD_ReportingSetup.alias('rep1')\
           ,(col("bal.companyCode")==col("rep1.companyCode")),how="inner")\
        .filter(col('bal.fiscalYear')==finYear)\
        .select(col('rep1.clientCode')\
                ,col('rep1.companyCode')\
                ,col('bal.fiscalYear')\
                ,col('bal.accountNumber')\
               )\
        .distinct()

    df_glAccountNumber_Cross=df_LK_CD_GLAccountToFinancialStructureDetail_Acnt.alias("gla")\
        .crossJoin(df_LK_CD_ReportingSetup.alias("rpt"))\
        .select(col('gla.glAccountNumber'),col('rpt.clientCode'),col('rpt.companyCode'))

    df_L1_TMP_ReceivableAccountsToIgnore=df_glAccountNumber_Cross.alias("gla")\
        .join(df_L1_TD_GLBalance.alias("glab")\
            ,((col('gla.glAccountNumber').eqNullSafe(col('glab.accountNumber')))\
             & (col('gla.companyCode').eqNullSafe(col('glab.companyCode')))\
               ),how="left")\
        .filter(col('glab.clientCode').isNull())\
        .select(col('gla.clientCode')\
               ,col('gla.companyCode')\
               ,col('gla.glAccountNumber').alias('accountNumber'))\
        .distinct()

    df_GLAccountMapping = knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot.alias('gla')\
        .join(knw_LK_CD_FinancialStructureDetailSnapshot.alias('fsd'),\
              (col('gla.financialStructureDetailID') == col('fsd.financialStructureDetailID')) \
              ,how =('inner'))\
        .join(knw_LK_CD_KnowledgeAccountSnapshot.alias('kna'),\
              (col('fsd.knowledgeAccountNodeID') == col('kna.knowledgeAccountID')) \
              ,how =('inner'))\
        .join(fin_L1_STG_GLAccountMapping.alias('glam1'),\
              (col('gla.glAccountNumber') == col('glam1.accountNumber')) \
              ,how =('inner'))\
        .filter(col('glam1.isTradeAR') == 1)\
        .select(col('gla.glAccountNumber')\
               ,trim(coalesce(col('kna.displayName'),col('kna.knowledgeAccountName'))).alias('knowledgeAccountName')\
               ).distinct()

    # #ltrim(rtrim(ISNULL(kna.displayName,))

    df_GLAccountMapping_Cross=df_GLAccountMapping.alias("gla")\
        .crossJoin(df_LK_CD_ReportingSetup.alias("rpt"))\
        .select(col('gla.glAccountNumber'),col('rpt.chartOfAccount'),col('rpt.companyCode'),col('gla.knowledgeAccountName'))

    df_L1_TMP_ReceivableAccount=df_GLAccountMapping_Cross.alias("gla")\
        .join(df_L1_TMP_ReceivableAccountsToIgnore.alias("ign")\
            ,((col('gla.glAccountNumber').eqNullSafe(col('ign.accountNumber')))\
             & (col('gla.companyCode').eqNullSafe(col('ign.companyCode')))\
               ),how="left")\
        .filter(col('ign.accountNumber').isNull() )\
        .select(col('gla.companyCode')\
               ,col('gla.chartOfAccount').alias('chartofAccount')\
               ,col('gla.glAccountNumber').alias('accountNumber')\
               ,col('gla.knowledgeAccountName').alias('knowledgeAccountName'))\
        .distinct()
    #df_L1_TMP_ReceivableAccount.display()

    #isOpenItemTo
    v_isOpenItemTo =\
        when(col('arr.documentClearingDate')==lit(defaultDate),lit(pmaxOpenDate))\
        .otherwise(col('arr.documentClearingDate'))
    #isBillingFromSD
    v_isBillingFromSD =\
        when(col('arr.referenceSubledger')==lit('Sales S/L'),lit(1))\
        .otherwise(lit(0))
    v_billingDocumentNumber=F.split(F.col("arr.referenceSubledgerDocumentNumber"),"_").getItem(0)

    df_L1_TMP_ReceivableOpenItemsFromSubledger=fin_L1_TD_AccountReceivable.alias("arr")\
        .join(knw_LK_CD_ReportingSetup.alias("lkrs"),col('arr.companyCode')==col('lkrs.companyCode'),"inner")\
        .join(df_L1_TMP_ReceivableAccount.alias("rec"),\
              ((col('arr.companyCode')==col('rec.companyCode'))\
               & (col('arr.accountNumber')==col('rec.accountNumber'))),"inner")\
        .filter(col('arr.documentStatus').isin('N','C'))\
        .select(col('lkrs.clientCode').alias('client')\
                ,col('arr.companyCode').alias('companyCode')\
                ,col('arr.customerNumber').alias('customerNumber')\
                ,col('arr.documentClearingDate').alias('GLClearingDocumentPostingDate')\
                ,col('arr.clearingDocumentNumber').alias('glclearingDocumentNumber')\
                ,col('arr.financialYear').alias('fiscalYear')\
                ,col('arr.financialPeriod').alias('financialPeriod')\
                ,col('arr.GLJEDocumentNumber').alias('GLJEDocumentNumber')\
                ,col('arr.GLJELineNumber').alias('GLJELineNumber')\
                ,col('arr.GLDocumentDate').alias('GLDocumentDate')\
                ,col('arr.GLJEPostingDate').alias('GLDocumentPostingDate')\
                ,col('arr.documentType').alias('GLDocumentType')\
                ,col('arr.postingKey').alias('GLDocumentKey')\
                ,col('arr.debitCreditIndicator').alias('debitCreditKey')\
                ,col('arr.amountLocalCurrency').alias('amountLC')\
                ,col('arr.postingDescription').alias('GLDocumentSegmentDescription')\
                ,col('arr.accountNumber').alias('GLAccountNumber')\
                ,col('arr.documentStatus').alias('documentStatus')\
                ,col('arr.amountLC').alias('amountLCWithSign')\
                ,col('arr.GLJEPostingDate').alias('isOpenItemFrom')\
                ,lit(v_isOpenItemTo).alias('isOpenItemTo')\
                ,lit(v_isBillingFromSD).alias('isBillingFromSD')\
                ,lit(v_billingDocumentNumber).alias('billingDocumentNumber')\
               )


    v_isOpenItemPY=when(col('sub.GLDocumentPostingDate') < lit(startDate),\
          (\
              when((col('sub.glClearingDocumentPostingDate') >=  lit(startDate) )\
                | (coalesce(col('sub.glClearingDocumentPostingDate'),lit(defaultDate)) == lit(defaultDate))\
                   ,lit(1)\
                  )\
             .otherwise(lit(0))\
          )\
       )\
      .otherwise(lit(0))

    df_L1_STG_Receivable=df_L1_TMP_ReceivableOpenItemsFromSubledger.alias("sub")\
        .select(col('sub.client').alias('client')\
            ,col('sub.companyCode').alias('companyCode')\
            ,col('sub.customerNumber').alias('customerNumber')\
            ,col('sub.glClearingDocumentPostingDate').alias('GLClearingDocumentPostingDate')\
            ,col('sub.glClearingDocumentNumber').alias('glclearingDocumentNumber')\
            ,col('sub.fiscalYear').alias('fiscalYear')\
            ,col('sub.financialPeriod').alias('financialPeriod')\
            ,col('sub.GLJEDocumentNumber').alias('GLJEDocumentNumber')\
            ,col('sub.GLJELineNumber').alias('GLJELineNumber')\
            ,col('sub.GLDocumentDate').alias('GLDocumentDate')\
            ,col('sub.GLDocumentPostingDate').alias('GLDocumentPostingDate')\
            ,col('sub.GLDocumentType').alias('GLDocumentType')\
            ,col('sub.GLDocumentKey').alias('GLDocumentKey')\
            ,col('sub.debitCreditKey').alias('debitCreditKey')\
            ,col('sub.amountLC').alias('amountLC')\
            ,col('sub.GLDocumentSegmentDescription').alias('GLDocumentSegmentDescription')\
            ,col('sub.glAccountNumber').alias('GLAccountNumber')\
            ,col('sub.documentStatus').alias('documentStatus')\
            ,col('sub.amountLCWithSign').alias('amountReceivableTrade')\
            ,col('sub.isOpenItemFrom').alias('isOpenItemFrom')\
            ,col('sub.isOpenItemTo').alias('isOpenItemTo')\
            ,lit(v_isOpenItemPY).alias('isOpenItemPY')\
            ,lit(0).alias('isOpenItemYE')\
            ,col('sub.isBillingFromSD').alias('isBillingFromSD')\
            ,lit(0).alias('isSLBillingDocumentAmbiguous')\
            ,lit(0).alias('isSLBillingDocumentMissing')\
            ,col('sub.billingDocumentNumber').alias('billingDocumentNumber')\
            ,lit(v_ID).alias("ID")\
                )
    df_L1_STG_Receivable.persist()

    #To Calculate isSLBillingDocumentAmbiguous
    df_L1_TMP_AR_AmbiguousBilling=df_L1_TMP_ReceivableOpenItemsFromSubledger.alias('trev1')\
      .join(fin_L1_STG_JELineItem.alias('jel1')\
              ,((col("trev1.companyCode")==col("jel1.companyCode"))\
                & (col("trev1.fiscalYear")==col("jel1.financialYear"))\
                & (col("trev1.GLJEDocumentNumber")==col("jel1.GLJEDocumentNumber"))\
                & (col("trev1.GLJELineNumber")==col("jel1.GLJELineNumber"))\
               ),how="inner")\
      .groupBy('trev1.companyCode','trev1.billingDocumentNumber')\
      .agg(countDistinct('jel1.idGLJE').alias('glDocumentNumberCount')\
          )\
      .filter(col('glDocumentNumberCount') > 1)

    df_L1_TMP_AR_AmbiguousBillingMinFiscalYr=df_L1_TMP_ReceivableOpenItemsFromSubledger.alias('trev1')\
      .join(fin_L1_STG_JELineItem.alias('jel1')\
              ,((col("trev1.companyCode")==col("jel1.companyCode"))\
                & (col("trev1.fiscalYear")==col("jel1.financialYear"))\
                & (col("trev1.GLJEDocumentNumber")==col("jel1.GLJEDocumentNumber"))\
                & (col("trev1.GLJELineNumber")==col("jel1.GLJELineNumber"))\
               ),how="inner")\
      .join(df_L1_TMP_AR_AmbiguousBilling.alias('ambbill')\
              ,((col("trev1.companyCode")==col("ambbill.companyCode"))\
                & (col("trev1.billingDocumentNumber")==col("ambbill.billingDocumentNumber"))\
               ),how="inner")\
      .groupBy('trev1.companyCode','trev1.billingDocumentNumber','trev1.GLJEDocumentNumber')\
      .agg(min('jel1.financialYear').alias('minFiscalYear')\
          )

    v_isSLBillingDocumentAmbiguous=\
      when(((col('arr.GLJEDocumentNumber')==col('arr.billingDocumentNumber')) \
         & (col('arr.fiscalYear')==col('minfsc.minFiscalYear'))) ,lit(0))\
      .otherwise(lit(1))

    df_L1_STG_Receivable_Billing_V1=df_L1_STG_Receivable.alias("arr")\
        .join(df_L1_TMP_AR_AmbiguousBilling.alias("ambbill")\
              ,(col('arr.companyCode')==col('ambbill.companyCode'))\
              & (col('arr.billingDocumentNumber')==col('ambbill.billingDocumentNumber')),"inner")\
        .join(df_L1_TMP_AR_AmbiguousBillingMinFiscalYr.alias("minfsc"),\
              ((col('arr.companyCode')==col('minfsc.companyCode'))\
               & (col('arr.billingDocumentNumber')==col('minfsc.billingDocumentNumber'))\
               & (col('arr.GLJEDocumentNumber')==col('minfsc.GLJEDocumentNumber'))\
              ),"inner")\
        .select(col('arr.ID').alias('ID')\
                ,lit(v_isSLBillingDocumentAmbiguous).alias("isSLBillingDocumentAmbiguous")\
               )
    #To Calculate isSLBillingDocumentMissing
    df_L1_TMP_AR_SLBillingDocumentMissing=df_L1_TMP_ReceivableOpenItemsFromSubledger.alias("arsub")\
        .filter(col('arsub.isBillingFromSD')==lit(1))\
        .select(col('billingDocumentNumber'))\
        .distinct()\
        .subtract(otc_L1_TD_Billing.select('billingDocumentNumber').distinct())

    #isSLBillingDocumentMissing	
    df_L1_STG_Receivable_Billing_V2=df_L1_STG_Receivable.alias("arr")\
        .join(df_L1_TMP_AR_SLBillingDocumentMissing.alias("billmss")\
              ,(col('arr.billingDocumentNumber')==col('billmss.billingDocumentNumber')),"inner")\
        .select(col('arr.ID').alias('ID')\
               ,lit(1).alias('isSLBillingDocumentMissing')\
               ,col('arr.billingDocumentNumber').alias('billingDocumentNumber')\
               )

    #L1_TMP_ReceivableBalancedClearing           
    df_L1_TMP_ReceivableBalancedClearing=df_L1_STG_Receivable.alias("rec1")\
        .filter(coalesce(col('rec1.glClearingDocumentNumber'),lit(''))!=lit(''))\
        .groupBy(col('rec1.client'),col('rec1.companyCode')\
             ,col('rec1.glClearingDocumentPostingDate'),col('rec1.glClearingDocumentNumber')\
                )\
        .agg(sum("rec1.amountReceivableTrade").alias("amountReceivableTrade")\
            )\
        .select(col('client')\
           ,col('companyCode')\
           ,col('glClearingDocumentPostingDate')\
           ,col('glClearingDocumentNumber')\
           ,col('amountReceivableTrade')\
           ,round(col('amountReceivableTrade'),6).alias("amountReceivableTrade_Round")
           ,when(round(col('amountReceivableTrade'),6)==0,lit(1)).otherwise(lit(0)).alias('isBalancedClearing')\
               )

    #L1_TMP_ReceivableAccountWithoutCarriedForward
    df_L1_TMP_ReceivableAccountWithoutCarriedForward=fin_L1_TD_GLBalance.alias("glbal")\
        .join(df_L1_TMP_ReceivableAccount.alias("rec"),\
              ((col('glbal.accountNumber')==col('rec.accountNumber')) \
               & (col('glbal.companyCode')==col('rec.companyCode')) )\
             ,"inner")\
        .filter((col('glbal.fiscalYear')== lit(finYear)) \
                & (col('glbal.financialPeriod') < lit(periodStart) ))\
        .groupBy(col('glbal.companyCode'),col('glbal.accountNumber'))\
        .agg(max('glbal.fiscalYear').alias("fiscalYear")\
            ,max('glbal.financialPeriod').alias("financialPeriod")\
            ,sum('glbal.beginningBalanceLC').alias("beginningBalanceLC")\
            ,sum('glbal.endingBalanceLC').alias("endingBalanceLC")\
            )\
        .select(col('companyCode')\
           ,col('fiscalYear')\
           ,col('financialPeriod')\
           ,col('accountNumber')\
           ,col('beginningBalanceLC')\
           ,col('endingBalanceLC')\
               )\
        .filter(round('beginningBalanceLC',2)==0)

    #Calulate the value for the column "beginningTransactionLC"

    df_L1_STG_Receivable_tmp1=df_L1_STG_Receivable.alias("arr")\
      .filter(col('arr.isOpenItemPY') == 1)\
      .groupBy('arr.companyCode','arr.glAccountNumber')\
      .agg(sum('arr.amountReceivableTrade').alias('amountReceivableTrade')\
          )\

    v_beginningTransactionLC =\
        when(col('ar.glAccountNumber').isNotNull(),col('ar.amountReceivableTrade'))\
        .otherwise(lit(0))

    df_L1_TMP_ReceivableAccountWithoutCarriedForward=df_L1_TMP_ReceivableAccountWithoutCarriedForward.alias("t")\
      .join(df_L1_STG_Receivable_tmp1.alias("ar"),\
              (col('t.accountNumber').eqNullSafe(col('ar.glAccountNumber')) \
               & col('t.companyCode').eqNullSafe(col('ar.companyCode') ))\
             ,"left")\
      .select(col('t.*')\
             ,lit(v_beginningTransactionLC).alias("beginningTransactionLC")\
             ,lit(startDate).alias("startDate")
             )

    #df_L1_TMP_ReceivableAccountWithoutCarriedForward.display()

    #isOpenItemPY,isOpenItemTo	
    v_isOpenItemPY=when((col('rblc1.isBalancedClearing')==1) \
                        & ( col('srec1.glClearingDocumentPostingDate') < lit(startDate)),(lit(0)))\
          .otherwise(col('srec1.isOpenItemPY'))

    v_isOpenItemTo=when(col('rblc1.isBalancedClearing')==1 ,col('srec1.glClearingDocumentPostingDate'))\
      .when(col('srec1.glClearingDocumentPostingDate') < lit(startDate),col('srec1.glClearingDocumentPostingDate'))\
      .otherwise(lit(pmaxOpenDate))


    df_L1_STG_Receivable_Billing_V3=df_L1_STG_Receivable.alias("srec1")\
        .join(df_L1_TMP_ReceivableBalancedClearing.alias("rblc1")\
              ,((col('srec1.client')==col('rblc1.client'))\
                & (col('srec1.companyCode')==col('rblc1.companyCode'))\
                & (col('srec1.glClearingDocumentNumber')==col('rblc1.glClearingDocumentNumber'))\
                & (col('srec1.glClearingDocumentPostingDate')==col('rblc1.glClearingDocumentPostingDate'))\
               ),"inner")\
        .select(col('srec1.ID').alias('ID')\
              ,lit(v_isOpenItemPY).alias("isOpenItemPY")\
               ,lit(v_isOpenItemTo).alias("isOpenItemTo")\
               )

    df_L1_STG_Receivable_Billing_V31=df_L1_STG_Receivable.alias("srec1")\
       .join(df_L1_STG_Receivable_Billing_V3.alias("srec13")\
              ,(col('srec1.ID').eqNullSafe(col('srec13.ID'))) ,"left")\
      .select(col('srec1.ID')\
             ,col('srec1.glAccountNumber')\
             ,col('srec1.companyCode')\
             ,when(col('srec13.ID').isNull(),col('srec1.isOpenItemPY')).otherwise(col('srec13.isOpenItemPY')).alias('isOpenItemPY')\
             ,when(col('srec13.ID').isNull(),col('srec1.isOpenItemTo')).otherwise(col('srec13.isOpenItemTo')).alias('isOpenItemTo')\
             ,when(col('srec13.ID').isNull(),lit('Old')).otherwise(lit('New')).alias('source')\
             ,col('srec1.glClearingDocumentNumber')\
             ,col('srec1.glClearingDocumentPostingDate')\
             ,col('srec1.client')\
             ,col('srec1.GLDocumentPostingDate')\
             ,col('srec1.isOpenItemYE')\
             )

    #df_L1_STG_Receivable_Billing_V31.display()

    df_L1_STG_Receivable_Billing_V4=df_L1_STG_Receivable_Billing_V31.alias("srec1")\
        .join(df_L1_TMP_ReceivableAccountWithoutCarriedForward.alias("wcfv")\
              ,((col('wcfv.accountNumber')==col('srec1.glAccountNumber'))\
                & (col('wcfv.companyCode')==col('srec1.companyCode'))\
               ),"inner")\
        .filter((col('srec1.isOpenItemPY')!=0) \
                & (round('wcfv.beginningTransactionLC',2)!=0)\
               )\
        .select(col('srec1.ID').alias('ID')\
               ,expr("date_add(startDate , int(-1))").alias("isOpenItemTo")\
               ,lit(None).alias("isOpenItemPY")\
               )

    #df_L1_STG_Receivable_Billing_V4.display()

    df_L1_STG_Receivable_Billing_V41=df_L1_STG_Receivable_Billing_V31.alias("srec1")\
       .join(df_L1_STG_Receivable_Billing_V4.alias("srec14")\
              ,(col('srec1.ID').eqNullSafe(col('srec14.ID'))) ,"left")\
      .select(col('srec1.ID')\
             ,col('srec1.glAccountNumber')\
             ,col('srec1.companyCode')\
             ,when(col('srec14.ID').isNull(),col('srec1.isOpenItemPY')).otherwise(col('srec14.isOpenItemPY')).alias('isOpenItemPY')\
             ,when(col('srec14.ID').isNull(),col('srec1.isOpenItemTo')).otherwise(col('srec14.isOpenItemTo')).alias('isOpenItemTo')\
             ,when(col('srec14.ID').isNull(),lit('Old')).otherwise(lit('New')).alias('source')\
             ,col('srec1.glClearingDocumentNumber')\
             ,col('srec1.glClearingDocumentPostingDate')\
             ,col('srec1.client')\
             ,col('srec1.GLDocumentPostingDate')\
             ,col('srec1.isOpenItemYE')\
             )
    #df_L1_STG_Receivable_Billing_V41.display()

    df_L1_TMP_ReceivableCorrectedClearingAfterPY=df_L1_STG_Receivable.alias("srec1")\
        .filter(  (col('srec1.isOpenItemPY').isNull()) \
                & (col('srec1.glClearingDocumentNumber') !=lit(''))\
                & (col('srec1.glClearingDocumentPostingDate') >= lit(startDate))\
               )\
        .groupBy(col('srec1.client')\
                 ,col('srec1.companyCode')\
                 ,col('srec1.glAccountNumber')\
                 ,col('srec1.glClearingDocumentNumber')\
                 ,col('srec1.glClearingDocumentPostingDate')\
                )\
        .agg(max('srec1.GLDocumentPostingDate').alias("GLDocumentPostingDateMAX")\
            )

    v_amountInTimeFrame=\
       when(col('srec1.GLDocumentPostingDate').between(lit(startDate),lit(endDate))\
                 ,col('srec1.amountReceivableTrade'))\
      .otherwise(lit(0))

    df_L1_TMP_ReceivableClearingAfterPYCorrected=df_L1_STG_Receivable.alias("srec1")\
      .join(df_L1_TMP_ReceivableCorrectedClearingAfterPY.alias("rccap1"),\
            ( (col('srec1.client')==col('rccap1.client'))\
             & (col('srec1.companyCode')==col('rccap1.companyCode'))\
             & (col('srec1.glAccountNumber')==col('rccap1.glAccountNumber'))\
             & (col('srec1.glClearingDocumentPostingDate')==col('rccap1.glClearingDocumentPostingDate'))\
             & (col('srec1.glClearingDocumentNumber')==col('rccap1.glClearingDocumentNumber'))\
            ),"inner")\
      .groupBy(col('srec1.client')\
                 ,col('srec1.companyCode')\
                 ,col('srec1.glAccountNumber')\
                 ,col('srec1.glClearingDocumentNumber')\
                 ,col('srec1.glClearingDocumentPostingDate')\
                )\
      .agg(sum('srec1.amountReceivableTrade').alias("amountReceivableTrade")\
            ,sum(lit(v_amountInTimeFrame)).alias("amountInTimeFrame")\
            )\
      .select(col('companyCode')\
         ,col('client')\
         ,col('glAccountNumber')\
         ,col('glClearingDocumentPostingDate')\
         ,col('glClearingDocumentNumber')\
         ,col('amountReceivableTrade')\
         ,col('amountInTimeFrame')\
             )
    v_isBalancedClearing= when(round(col('amountInTimeFrame'),6)==0,1).otherwise(lit(0))

    df_L1_TMP_ReceivableClearingAfterPYCorrected=df_L1_TMP_ReceivableClearingAfterPYCorrected\
            .withColumn("isBalancedClearing",v_isBalancedClearing)

    #df_L1_TMP_ReceivableClearingAfterPYCorrected.display()
    #isOpenItemTo        
    df_L1_STG_Receivable_Billing_V5=df_L1_STG_Receivable_Billing_V41.alias("srec1")\
        .join(df_L1_TMP_ReceivableClearingAfterPYCorrected.alias("rcapc1")\
              ,((col('srec1.client')==col('rcapc1.client'))\
                & (col('srec1.companyCode')==col('rcapc1.companyCode'))\
                & (col('srec1.glClearingDocumentNumber')==col('rcapc1.glClearingDocumentNumber'))\
                & (col('srec1.glClearingDocumentPostingDate')==col('rcapc1.glClearingDocumentPostingDate'))\
                & (col('srec1.glAccountNumber')==col('rcapc1.glAccountNumber'))\
               ),"inner")\
        .filter((col('srec1.isOpenItemYE')==0)\
                & (col('srec1.GLDocumentPostingDate').between(lit(startDate),lit(endDate)))\
               )\
        .select(col('srec1.ID').alias('ID')\
               ,lit(pmaxOpenDate).alias("isOpenItemTo")\
               )
    #df_L1_STG_Receivable_Billing_V5.display()


    df_L1_STG_Receivable_Billing_V51=df_L1_STG_Receivable_Billing_V41.alias("srec1")\
      .join(df_L1_STG_Receivable_Billing_V5.alias("srec15"),(col('srec1.ID').eqNullSafe(col('srec15.ID'))) ,"left")\
      .select(col('srec1.ID')\
        ,when(col('srec15.ID').isNull(),col('srec1.isOpenItemTo')).otherwise(col('srec15.isOpenItemTo')).alias('isOpenItemTo')\
        ,when(col('srec1.isOpenItemPY').isNull(),lit(0)).otherwise(col('srec1.isOpenItemPY')).alias('isOpenItemPY')\
        ,when(col('srec15.ID').isNull(),lit('Old')).otherwise(lit('New')).alias('source')\
             )

    df_L1_STG_Receivable=df_L1_STG_Receivable.alias("srec")\
      .join(df_L1_STG_Receivable_Billing_V51.alias("srec5"),(col('srec.ID').eqNullSafe(col('srec5.ID'))) ,"inner")\
      .join(df_L1_STG_Receivable_Billing_V1.alias("srec1"),(col('srec.ID').eqNullSafe(col('srec1.ID'))) ,"left")\
      .join(df_L1_STG_Receivable_Billing_V2.alias("srec2"),(col('srec.billingDocumentNumber')\
                                                        .eqNullSafe(col('srec2.billingDocumentNumber'))) ,"left")\
      .select(col('srec.ID')\
        ,col('srec.client')\
        ,col('srec.companyCode')\
        ,col('srec.customerNumber')\
        ,col('srec.GLClearingDocumentPostingDate')\
        ,col('srec.glclearingDocumentNumber')\
        ,col('srec.fiscalYear')\
        ,col('srec.financialPeriod')\
        ,col('srec.GLJEDocumentNumber')\
        ,col('srec.GLJELineNumber')\
        ,col('srec.GLDocumentDate')\
        ,col('srec.GLDocumentPostingDate')\
        ,col('srec.GLDocumentType')\
        ,col('srec.GLDocumentKey')\
        ,col('srec.debitCreditKey')\
        ,col('srec.amountLC')\
        ,col('srec.GLDocumentSegmentDescription')\
        ,col('srec.GLAccountNumber')\
        ,col('srec.documentStatus')\
        ,col('srec.amountReceivableTrade')\
        ,col('srec.isOpenItemFrom')\
        ,col('srec.isBillingFromSD')\
        ,col('srec.billingDocumentNumber')\
        ,col('srec.isOpenItemYE')\
        ,col('srec5.isOpenItemTo').alias('isOpenItemTo')\
        ,col('srec5.isOpenItemPY').alias('isOpenItemPY')\
        ,when(col('srec1.ID').isNull(),col('srec.isSLBillingDocumentAmbiguous'))\
              .otherwise(col('srec1.isSLBillingDocumentAmbiguous')).alias('isSLBillingDocumentAmbiguous')\
        ,when(col('srec2.billingDocumentNumber').isNull(),col('srec.isSLBillingDocumentMissing'))\
              .otherwise(col('srec2.isSLBillingDocumentMissing')).alias('isSLBillingDocumentMissing')\
             )

    v_isOpenItemPY=when(col('srec1.isOpenItemPY').isNull(),lit(0)).otherwise(col('srec1.isOpenItemPY'))
    v_isOpenItemYE=when(col('srec1.isOpenItemTo') > lit(endDate),\
                            (when(col('srec1.isOpenItemPY')==1,lit(1))\
                             .when(col('srec1.isOpenItemFrom').between(lit(pBeginOfYear),lit(endDate)),lit(1))\
                             .otherwise(col('srec1.isOpenItemYE'))\
                             ))\
                   .otherwise(col('srec1.isOpenItemYE'))

    fin_L1_STG_Receivable=df_L1_STG_Receivable.alias("srec1")\
        .select(col('client')\
            ,col('companyCode')\
            ,col('customerNumber')\
            ,col('GLClearingDocumentPostingDate')\
            ,col('glclearingDocumentNumber')\
            ,col('fiscalYear')\
            ,col('GLJEDocumentNumber')\
            ,col('GLJELineNumber')\
            ,col('GLDocumentDate')\
            ,col('GLDocumentPostingDate')\
            ,col('GLDocumentType')\
            ,col('GLDocumentKey')\
            ,col('debitCreditKey')\
            ,col('amountLC')\
            ,col('GLDocumentSegmentDescription')\
            ,col('GLAccountNumber')\
            ,col('documentStatus')\
            ,col('amountReceivableTrade')\
            ,col('isOpenItemFrom')\
            ,col('isOpenItemTo')\
            ,lit(v_isOpenItemPY).alias('isOpenItemPY')\
            ,lit(v_isOpenItemYE).alias('isOpenItemYE')\
            ,col('financialPeriod')\
            ,col('isBillingFromSD')\
            ,col('billingDocumentNumber')\
            ,col('isSLBillingDocumentAmbiguous')\
            ,col('isSLBillingDocumentMissing')\
                )
    
    fin_L1_STG_Receivable  = objDataTransformation.gen_convertToCDMandCache \
			(fin_L1_STG_Receivable,'fin','L1_STG_Receivable',False)

    executionStatus = "L1_STG_Receivable populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
