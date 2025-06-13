# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,when

def fin_L2_DIM_JEDocumentClassification_populate():  
    """ Populate L2_DIM_JEDocumentClassification """
    try:
     
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      
      global fin_L1_STG_JEDocumentClassification,fin_L2_DIM_JEDocumentClassification
      
      L1_TMP_JEDocumentClassification = fin_L1_STG_JEDocumentClassification_02_prepare.alias('jva1') \
         .join(fin_L1_TD_Journal.alias('joi1'),(col('joi1.journalSurrogateKey')	==	col('jva1.journalSurrogateKey')),how =('inner')) \
         .join(gen_L2_DIM_Organization.alias('dou2'),(col('jva1.companyCode')	==	col('DOU2.companyCode')) \
              & (col('jva1.companyCode')	!= lit('#NA#')),how =('inner')) \
         .join(gen_L2_DIM_CalendarDate.alias('wkd'),(col('wkd.organizationUnitSurrogateKey').eqNullSafe(col('dou2.organizationUnitSurrogateKey'))) \
              & ((col('wkd.calendarDate').cast('date')).eqNullSafe(col('jva1.shiftedDateOfEntry').cast('Date'))),how =('left')) \
         .join(fin_L1_STG_JEDocumentClassification_03_prepare.alias('jeMisDesc'),(col('jeMisDesc.transactionIDbyJEDocumnetNumber')\
                 .eqNullSafe(col('jva1.transactionIDbyJEDocumnetNumber'))),how =('left')) \
         .join(fin_L1_STG_JEDocumentClassification_01_prepare.alias('jeRounded'),(col('jeRounded.transactionIDbyJEDocumnetNumber')\
                 .eqNullSafe(col('jva1.transactionIDbyJEDocumnetNumber'))),how =('left')) \
         .join(fin_L1_STG_JEDocumentClassification_04_prepare.alias('jdce1'),(col('jdce1.transactionIDbyJEDocumnetNumber')\
                 .eqNullSafe(col('jva1.transactionIDbyJEDocumnetNumber'))),how =('left')) \
         .select(col('jva1.transactionIDbyJEDocumnetNumber'),col('joi1.journalSurrogateKey'),col('dou2.organizationUnitSurrogateKey'), \
                col('jva1.companyCode'),col('jva1.postingDate'),col('jva1.creationDate'),col('joi1.debitCreditIndicator'), \
                col('jva1.jePostPeriodDays'),col('jva1.isJEPostingInTimeFrame'),col('wkd.isWeekend').alias('JEWeekendPosting'), \
                col('jva1.documentStatus'),col('jva1.documentStatusDetail'),when(col('jeMisDesc.transactionIDbyJEDocumnetNumber').isNull(),lit('No')) \
                .otherwise(lit('Yes')).alias('isJEWithMissingDescription'), \
                when(col('jeRounded.transactionIDbyJEDocumnetNumber').isNull(),lit('No')).otherwise(lit('Yes')).alias('isJEWithRoundedAmount'), \
                col('jva1.isJEPostedInPeriodEndDate'),col('jva1.postingKey'),col('jva1.transactionCode'),col('jva1.documentSource')
               ,col('jva1.documentSubSource'),col('jva1.documentPreSystem'),col('jva1.isBatchInput'),col('jva1.isSubledgerPosting')
               ,col('jva1.reversalStatus'),col('jva1.isManualPosting'),col('jva1.manualPostingAttribute1'),col('jva1.manualPostingAttribute2')
               ,col('jva1.manualPostingAttribute3'),col('jva1.manualPostingAttribute4'), \
                when(col('jdce1.IsJEWithMultipleUser').isNull(),lit(False)).otherwise(col('jdce1.IsJEWithMultipleUser')).alias('IsJEWithMultipleUser'), \
                when(col('jdce1.JEHolidayPosting').isNull(),lit(False)).otherwise(col('jdce1.JEHolidayPosting')).alias('JEHolidayPosting'), \
                when(col('jdce1.IsJEWithMissingUser').isNull(),lit(False)).otherwise(col('jdce1.IsJEWithMissingUser')).alias('IsJEWithMissingUser'), \
                when(col('jdce1.IsOutofBalance').isNull(),lit(False)).otherwise(col('jdce1.IsOutofBalance')).alias('IsOutofBalance'), \
                col('jva1.isJEInAnalysisPeriod'),col('jva1.postingDateToDocumentDate'),col('jva1.postingDateToDocuemntEntryDate'), \
                col('jva1.isJEWithCritalComment'),lit(0).alias('hasSLBillingDocumentLink'),lit(0).alias('hasSLPurchaseDocumentLink'), \
                col('jva1.isClearing'),col('jva1.isInterCoFlag'),col('jva1.isNegativePostingIndicator'),col('jva1.isJEWithLargeAmount'), \
                lit(0).alias('isSLBillingDocumentMissing'),lit(0).alias('isPurchaseInvoiceDocumentMissing'))
      
      
      
      L1_TMP_JEDocumentClassification.createOrReplaceTempView("L1_TMP_JEDocumentClassification")
      sqlContext.cacheTable("L1_TMP_JEDocumentClassification")
      
      
      L1_TMP_DistinctJEDocumentClassification = L1_TMP_JEDocumentClassification.select(col('companyCode'),col('organizationUnitSurrogateKey'),col('isJEPostingInTimeFrame')\
                  ,col('JEWeekendPosting'),col('debitCreditIndicator'),col('documentStatus'),col('documentStatusDetail')\
                  ,col('isJEWithMissingDescription'),col('isJEWithRoundedAmount'),col('isJEPostedInPeriodEndDate')\
                  ,col('postingKey'),col('transactionCode'),\
                  concat(col('companyCode'),lit('-'),col('postingKey'),lit('-'),col('transactionCode'),lit(' - ')\
                        ,when(col('documentSource').isNull(),lit('N/A')).otherwise(col('documentSource')),lit(' - ')\
                        ,when(col('documentSubSource').isNull(),lit('N/A')).otherwise(col('documentSubSource')),lit(' - ')\
                        ,when(col('isSubledgerPosting').isNull(),lit('N/A')).otherwise(col('isSubledgerPosting'))).alias('documentClassificationName')\
                   ,col('documentSource'),col('documentSubSource'),col('documentPreSystem'),col('isBatchInput'),col('isSubledgerPosting')\
                   ,col('reversalStatus'),col('isManualPosting'),col('manualPostingAttribute1'),col('manualPostingAttribute2')\
                   ,col('manualPostingAttribute3'),col('manualPostingAttribute4'),col('IsJEWithMultipleUser'),col('JEHolidayPosting')\
                   ,col('IsJEWithMissingUser'),col('IsOutofBalance'),col('isJEInAnalysisPeriod'),col('postingDateToDocumentDate')\
                   ,col('postingDateToDocuemntEntryDate'),col('isJEWithCritalComment'),col('hasSLBillingDocumentLink'),col('hasSLPurchaseDocumentLink')\
                   ,col('isClearing'),col('isInterCoFlag'),col('isNegativePostingIndicator'),col('isJEWithLargeAmount'),col('isSLBillingDocumentMissing')\
                   ,col('isPurchaseInvoiceDocumentMissing')\
                  ).distinct()
      
      L1_TMP_DistinctJEDocumentClassification.createOrReplaceTempView("L1_TMP_DistinctJEDocumentClassification")
      sqlContext.cacheTable("L1_TMP_DistinctJEDocumentClassification")
      
      
      L1_TMP_JEDocumentClassificationValue = L1_TMP_DistinctJEDocumentClassification.select(col('organizationUnitSurrogateKey'),col('isJEPostingInTimeFrame'),col('JEWeekendPosting')\
                       ,col('debitCreditIndicator'),col('documentStatus'),col('documentStatusDetail')\
                       ,col('isJEWithMissingDescription'),col('isJEWithRoundedAmount'),col('isJEPostedInPeriodEndDate')\
                       ,col('postingKey'),col('transactionCode'),col('documentClassificationName'),col('documentSource')\
                       ,col('documentSubSource'),col('documentPreSystem'),col('isBatchInput'),col('isSubledgerPosting')\
                       ,col('reversalStatus'),col('isManualPosting'),col('manualPostingAttribute1')\
                       ,col('manualPostingAttribute2'),col('manualPostingAttribute3'),col('manualPostingAttribute4')\
                       ,col('companyCode'),col('IsJEWithMultipleUser'),col('JEHolidayPosting'),col('IsJEWithMissingUser')\
                       ,when(col('IsOutofBalance').isNull(),lit(False)).otherwise(col('IsOutofBalance')).alias('IsOutofBalance')\
                       ,col('isJEInAnalysisPeriod'),col('postingDateToDocumentDate'),col('postingDateToDocuemntEntryDate')\
                       ,when(col('isJEWithCritalComment').isNull(),lit(0)).otherwise(col('isJEWithCritalComment'))\
                         .alias('isJEWithCritalComment')\
                       ,col('hasSLBillingDocumentLink'),col('hasSLPurchaseDocumentLink'),col('isClearing')\
                       ,col('isInterCoFlag'),col('isNegativePostingIndicator'),col('isJEWithLargeAmount')\
                        ,col('isSLBillingDocumentMissing'),col('isPurchaseInvoiceDocumentMissing'))
      
      w = Window().orderBy(lit('JEDocumentClassificationSurrogateKey'))
      L1_TMP_JEDocumentClassificationValue = L1_TMP_JEDocumentClassificationValue\
                   .withColumn("JEDocumentClassificationSurrogateKey", row_number().over(w))
      
      L1_TMP_JEDocumentClassificationValue.createOrReplaceTempView("L1_TMP_JEDocumentClassificationValue")
      sqlContext.cacheTable("L1_TMP_JEDocumentClassificationValue")
      
      
      new_join_BPTD1 = when(col('attrValue.isManualPosting').isNull(), lit(False) ) \
                              .otherwise(col('attrValue.isManualPosting'))

      new_join_BPTD2 = when(col('jnrlAttr1.isManualPosting').isNull(), lit(False) ) \
                              .otherwise(col('jnrlAttr1.isManualPosting'))
      
      fin_L1_STG_JEDocumentClassification = L1_TMP_JEDocumentClassification.alias('jnrlAttr1')\
             .join(L1_TMP_JEDocumentClassificationValue.alias('attrValue'),\
             (col('attrValue.isJEPostingInTimeFrame') ==	col('jnrlAttr1.isJEPostingInTimeFrame'))\
             & (col('attrValue.JEWeekendPosting')              ==	col('jnrlAttr1.JEWeekendPosting'))\
             & (col('attrValue.debitCreditIndicator')          ==	col('jnrlAttr1.debitCreditIndicator'))\
             & (col('attrValue.documentStatus')                ==	col('jnrlAttr1.documentStatus'))
             & (col('attrValue.isJEWithMissingDescription')    ==	col('jnrlAttr1.isJEWithMissingDescription'))\
             & (col('attrValue.isJEWithRoundedAmount')         ==	col('jnrlAttr1.isJEWithRoundedAmount'))\
             & (col('attrValue.isJEPostedInPeriodEndDate')	 == col('jnrlAttr1.isJEPostedInPeriodEndDate'))\
             & (col('attrValue.organizationUnitSurrogateKey')  == col('jnrlAttr1.organizationUnitSurrogateKey'))\
             & (col('attrValue.postingKey')					 == col('jnrlAttr1.postingKey'))\
             & (col('attrValue.transactionCode')				 == col('jnrlAttr1.transactionCode'))\
             & (col('attrValue.documentSource')				 == col('jnrlAttr1.documentSource'))\
             & (col('attrValue.documentSubSource')			 == col('jnrlAttr1.documentSubSource'))\
             & (col('attrValue.documentPreSystem')			 == col('jnrlAttr1.documentPreSystem'))\
             & (col('attrValue.isBatchInput')				     == col('jnrlAttr1.isBatchInput'))\
             & (col('attrValue.isSubledgerPosting')			 == col('jnrlAttr1.isSubledgerPosting'))\
             & (col('attrValue.reversalStatus')				 == col('jnrlAttr1.reversalStatus'))\
             & (lit(new_join_BPTD1)                            == lit(new_join_BPTD2))\
             & ( when(col('attrValue.manualPostingAttribute1').isNull(), lit(0)).otherwise(col('attrValue.manualPostingAttribute1')) == \
                 when(col('jnrlAttr1.manualPostingAttribute1').isNull(), lit(0)).otherwise(col('jnrlAttr1.manualPostingAttribute1')))\
             & ( when(col('attrValue.manualPostingAttribute2').isNull(), lit(0)).otherwise(col('attrValue.manualPostingAttribute2')) == \
                 when(col('jnrlAttr1.manualPostingAttribute2').isNull(), lit(0)).otherwise(col('jnrlAttr1.manualPostingAttribute2')))\
             & ( when(col('attrValue.manualPostingAttribute3').isNull(), lit(0)).otherwise(col('attrValue.manualPostingAttribute3')) == \
                 when(col('jnrlAttr1.manualPostingAttribute3').isNull(), lit(0)).otherwise(col('jnrlAttr1.manualPostingAttribute3')))\
             & ( when(col('attrValue.IsJEWithMultipleUser').isNull(), lit(False)).otherwise(col('attrValue.IsJEWithMultipleUser')) == \
                 when(col('jnrlAttr1.IsJEWithMultipleUser').isNull(), lit(False)).otherwise(col('jnrlAttr1.IsJEWithMultipleUser')))\
             & ( when(col('attrValue.JEHolidayPosting').isNull(), lit(False)).otherwise(col('attrValue.JEHolidayPosting')) == \
                 when(col('jnrlAttr1.JEHolidayPosting').isNull(), lit(False)).otherwise(col('jnrlAttr1.JEHolidayPosting')))\
             & ( when(col('attrValue.IsJEWithMissingUser').isNull(), lit(False)).otherwise(col('attrValue.IsJEWithMissingUser')) == \
                 when(col('jnrlAttr1.IsJEWithMissingUser').isNull(), lit(False)).otherwise(col('jnrlAttr1.IsJEWithMissingUser')))\
             & ( when(col('attrValue.IsOutofBalance').isNull(), lit(False)).otherwise(col('attrValue.IsOutofBalance')) == \
                 when(col('jnrlAttr1.IsOutofBalance').isNull(), lit(False)).otherwise(col('jnrlAttr1.IsOutofBalance')))\
             & (col('attrValue.isJEInAnalysisPeriod')				 == col('jnrlAttr1.isJEInAnalysisPeriod'))\
             & ( when(col('attrValue.postingDateToDocumentDate').isNull(), lit(-99999999)).otherwise(col('attrValue.postingDateToDocumentDate')) == \
                 when(col('jnrlAttr1.postingDateToDocumentDate').isNull(), lit(-99999999)).otherwise(col('jnrlAttr1.postingDateToDocumentDate')))\
             & ( when(col('attrValue.postingDateToDocuemntEntryDate').isNull(), lit(0)).otherwise(col('attrValue.postingDateToDocuemntEntryDate')) == \
                 when(col('jnrlAttr1.postingDateToDocuemntEntryDate').isNull(), lit(0)).otherwise(col('jnrlAttr1.postingDateToDocuemntEntryDate')))
             & ( when(col('attrValue.isJEWithCritalComment').isNull(), lit(0)).otherwise(col('attrValue.isJEWithCritalComment')) == \
                 when(col('jnrlAttr1.isJEWithCritalComment').isNull(), lit(0)).otherwise(col('jnrlAttr1.isJEWithCritalComment')))\
             & ( when(col('attrValue.hasSLBillingDocumentLink').isNull(), lit(0)).otherwise(col('attrValue.hasSLBillingDocumentLink')) == \
                 when(col('jnrlAttr1.hasSLBillingDocumentLink').isNull(), lit(0)).otherwise(col('jnrlAttr1.hasSLBillingDocumentLink')))\
             & ( when(col('attrValue.hasSLPurchaseDocumentLink').isNull(), lit(0)).otherwise(col('attrValue.hasSLPurchaseDocumentLink')) == \
                 when(col('jnrlAttr1.hasSLPurchaseDocumentLink').isNull(), lit(0)).otherwise(col('jnrlAttr1.hasSLPurchaseDocumentLink')))\
             & ( when(col('attrValue.isClearing').isNull(), lit(False)).otherwise(col('attrValue.isClearing')) == \
                 when(col('jnrlAttr1.isClearing').isNull(), lit(False)).otherwise(col('jnrlAttr1.isClearing')))\
             & ( when(col('attrValue.isInterCoFlag').isNull(), lit(False)).otherwise(col('attrValue.isInterCoFlag')) == \
                 when(col('jnrlAttr1.isInterCoFlag').isNull(), lit(False)).otherwise(col('jnrlAttr1.isInterCoFlag')))\
             & ( when(col('attrValue.isNegativePostingIndicator').isNull(), lit(False)).otherwise(col('attrValue.isNegativePostingIndicator')) == \
                 when(col('jnrlAttr1.isNegativePostingIndicator').isNull(), lit(False)).otherwise(col('jnrlAttr1.isNegativePostingIndicator')))\
             & ( when(col('attrValue.isJEWithLargeAmount').isNull(), lit(False)).otherwise(col('attrValue.isJEWithLargeAmount')) == \
                 when(col('jnrlAttr1.isJEWithLargeAmount').isNull(), lit(False)).otherwise(col('jnrlAttr1.isJEWithLargeAmount')))\
             & ( when(col('attrValue.isSLBillingDocumentMissing').isNull(), lit(0)).otherwise(col('attrValue.isSLBillingDocumentMissing')) == \
                 when(col('jnrlAttr1.isSLBillingDocumentMissing').isNull(), lit(0)).otherwise(col('jnrlAttr1.isSLBillingDocumentMissing')))\
             & ( when(col('attrValue.isPurchaseInvoiceDocumentMissing').isNull(), lit(0)).otherwise(col('attrValue.isPurchaseInvoiceDocumentMissing')) == \
                 when(col('jnrlAttr1.isPurchaseInvoiceDocumentMissing').isNull(),lit(0)).otherwise(col('jnrlAttr1.isPurchaseInvoiceDocumentMissing')))\
                   ,how='inner')\
             .select(col('journalSurrogateKey'),\
             col('attrValue.JEDocumentClassificationSurrogateKey').alias('JEDocumentClassificationKey'),\
             col('jnrlAttr1.jePostPeriodDays'),\
             col('jnrlAttr1.isNegativePostingIndicator'))  
                                           
                  
      fin_L1_STG_JEDocumentClassification  = objDataTransformation.gen_convertToCDMandCache \
          (fin_L1_STG_JEDocumentClassification,'fin','L1_STG_JEDocumentClassification',False)
      
      
      fin_L2_DIM_JEDocumentClassification = L1_TMP_JEDocumentClassificationValue.select(col('JEDocumentClassificationSurrogateKey'),col('organizationUnitSurrogateKey'),col('isJEPostingInTimeFrame')\
                                       ,col('JEWeekendPosting'),col('debitCreditIndicator'),col('documentStatus'),col('documentStatusDetail')\
                                       ,when (col('isJEWithMissingDescription')==lit('Yes'),lit(1)).otherwise(lit(0)).alias('isJEWithMissingDescription')\
                                       ,when (col('isJEWithRoundedAmount')==lit('Yes'),lit(1)).otherwise(lit(0)).alias('isJEWithRoundedAmount')\
                                       ,when (col('isJEPostedInPeriodEndDate')==lit('Yes'),lit(1)).otherwise(lit(0)).alias('isJEPostedInPeriodEndDate')\
                                       ,col('documentClassificationName'),col('documentSource'),col('documentSubSource'),col('documentPreSystem')\
                                       ,col('isBatchInput'),col('isSubledgerPosting'),col('reversalStatus'),col('isManualPosting'),col('manualPostingAttribute1')\
                                       ,col('manualPostingAttribute2'),col('manualPostingAttribute3'),col('manualPostingAttribute4')\
                                       ,when (col('IsOutofBalance').isNull(),lit(False)).otherwise(col('IsOutofBalance')).alias('IsOutofBalance')\
                                       ,when (col('IsJEWithMultipleUser').isNull(),lit(False)).otherwise(col('IsJEWithMultipleUser')).alias('IsJEWithMultipleUser')\
                                       ,when (col('IsJEWithMissingUser').isNull(),lit(False)).otherwise(col('IsJEWithMissingUser')).alias('IsJEWithMissingUser')\
                                       ,when (col('JEHolidayPosting').isNull(),lit(False)).otherwise(col('JEHolidayPosting')).alias('JEHolidayPosting')\
                                       ,col('isJEInAnalysisPeriod'),col('postingDateToDocumentDate'),col('postingDateToDocuemntEntryDate')\
                                       ,when (col('isJEWithCritalComment').isNull(),lit(0)).otherwise(col('isJEWithCritalComment')).alias('isJEWithCritalComment')\
                                       ,col('hasSLBillingDocumentLink'),col('hasSLPurchaseDocumentLink')\
                                       ,when (col('isClearing').isNull(),lit(False)).otherwise(col('isClearing')).alias('isClearing'),col('isInterCoFlag')\
                             ,when (col('isNegativePostingIndicator').isNull(),lit(False)).otherwise(col('isNegativePostingIndicator')).alias('isNegativePostingIndicator')\
                                      ,col('isJEWithLargeAmount'),\
                             when (col('isSLBillingDocumentMissing').isNull(),lit(1)).otherwise(col('isSLBillingDocumentMissing')).alias('isSLBillingDocumentMissing')\
                                      ,col('isPurchaseInvoiceDocumentMissing'))
     
      
      fin_L2_DIM_JEDocumentClassification  = objDataTransformation.gen_convertToCDMandCache \
          (fin_L2_DIM_JEDocumentClassification,'fin','L2_DIM_JEDocumentClassification',True)
      
      executionStatus = "fin_L2_DIM_JEDocumentClassification populated successfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    finally:
      spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_JEDocumentClassification")
      spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_DistinctJEDocumentClassification")
      spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_JEDocumentClassificationValue")
      
      
