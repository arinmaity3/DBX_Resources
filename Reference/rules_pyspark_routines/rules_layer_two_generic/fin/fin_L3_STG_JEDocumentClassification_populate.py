# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce,trim
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

def fin_L3_STG_JEDocumentClassification_populate():
    """Populate L3_STG_JEDocumentClassification"""

    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L3_STG_JEDocumentClassification
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        erpSystemIDGeneric = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
        if erpSystemIDGeneric is None:
            erpSystemIDGeneric = 10
   
        df_reportingLanguage=knw_LK_CD_ReportingSetup.select(col('KPMGDataReportingLanguage')).alias('languageCode')\
                                                     .distinct()
        reportingLanguage=df_reportingLanguage.first()[0]
        if reportingLanguage is None:
            reportingLanguage='EN'     

        new_join_lkbd1 = when(col('jnrlAttr2.isManualPosting')==0, lit('0') )\
                           .when(col('jnrlAttr2.isManualPosting')==1, lit('1'))\
                           .otherwise(lit('NONE'))

        new_col_isManualPosting = when(col('lkbd1.targetSystemValue')=='1', lit('Manual'))\
                                 .when(col('lkbd1.targetSystemValue')=='0', lit('Automated'))\
                                 .otherwise(lit('NONE'))

        new_col_isManualPostingPrecise = when(col('lkbd3.targetSystemValue').isin('1','2'), col('lkbd3.targetSystemValueDescription'))\
                                       .otherwise(lit('NONE'))

        new_col_documentStatus =when(col('jnrlAttr2.documentStatus')=='N', lit('Normal'))\
                                 .when(col('jnrlAttr2.documentStatus')=='P', lit('Parked'))\
                                 .otherwise(lit('Other'))

        new_col_intervalBetweenPostingDateTDocumentDate =  \
                                            when(col('jnrlAttr2.postingDateToDocumentDate') == -99999999,lit(-1))\
                                           .when(col('jnrlAttr2.postingDateToDocumentDate') < -90, lit(1))\
                                           .when(col('jnrlAttr2.postingDateToDocumentDate') < -30, lit(2))\
                                           .when(col('jnrlAttr2.postingDateToDocumentDate') <  0,  lit(3))\
                                           .when(col('jnrlAttr2.postingDateToDocumentDate') <= 30, lit(4))\
                                           .when(col('jnrlAttr2.postingDateToDocumentDate') <= 60, lit(5))\
                                           .when(col('jnrlAttr2.postingDateToDocumentDate') <= 90, lit(6))\
                                           .when(col('jnrlAttr2.postingDateToDocumentDate') > 90,  lit(7))\
                                           .otherwise(lit(-1))                
        new_col_intervalBetweenPostingDateToCreationDate = when(col('jnrlAttr2.postingDateToDocuemntEntryDate') < -90, lit(1))\
                                          .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') < -30, lit(2))\
                                          .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <  0,  lit(3))\
                                          .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <= 30, lit(4))\
                                          .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <= 60, lit(5))\
                                          .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <= 90, lit(6))\
                                          .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') > 90,  lit(7))\
                                          .otherwise(lit(-1))

        new_col_BetPDateTDocDate_Desc = \
                       when(col('jnrlAttr2.postingDateToDocumentDate') == -99999999, \
                                             lit('No Interval available'))\
                      .when(col('jnrlAttr2.postingDateToDocumentDate') < -90, \
                                             lit('More than 90 days ahead of document date'))\
                      .when(col('jnrlAttr2.postingDateToDocumentDate') < -30, \
                                           lit('90-31 days ahead of document date'))\
                      .when(col('jnrlAttr2.postingDateToDocumentDate') <  0, \
                                           lit('30-1 days ahead of document date'))\
                      .when(col('jnrlAttr2.postingDateToDocumentDate') <= 30, \
                                           lit('0-30 days later than document date'))\
                      .when(col('jnrlAttr2.postingDateToDocumentDate') <= 60, \
                                           lit('31-60 days later than document date'))\
                      .when(col('jnrlAttr2.postingDateToDocumentDate') <= 90,\
                                           lit('61-90 days later than document date'))\
                      .when(col('jnrlAttr2.postingDateToDocumentDate') > 90,  \
                                           lit('More than 90 days later than document date'))\
                      .otherwise(lit('No Interval available'))

        new_col_BetPDateTCreDate_Desc=when(col('jnrlAttr2.postingDateToDocuemntEntryDate') < -90, \
                                           lit('More than 90 days ahead of creation date'))\
                      .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') < -30,\
                                           lit('90-31 days ahead of creation date'))\
                      .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <  0,\
                                           lit('30-1 days ahead of creation date'))\
                      .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <= 30,\
                                           lit('0-30 days later than creation date'))\
                      .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <= 60,\
                                           lit('31-60 days later than creation date'))\
                      .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') <= 90,\
                                           lit('61-90 days later than creation date'))\
                      .when(col('jnrlAttr2.postingDateToDocuemntEntryDate') > 90,\
                                           lit('More than 90 days later than creation date'))\
                      .otherwise(lit('No Interval available')) 

        fin_L3_STG_JEDocumentClassification = fin_L2_DIM_JEDocumentClassification.alias('jnrlAttr2')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd1'),\
                    ((col('lkbd1.businessDatatype')==lit('Account Posting Status'))\
                     & (col('lkbd1.sourceSystemValue')==new_join_lkbd1 )\
                     & (col('lkbd1.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd1.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd2'),\
                    ((col('lkbd2.businessDatatype')==lit('Sub-ledger Posting Status'))\
                     & (col('lkbd2.sourceSystemValue')==lit(when(col('jnrlAttr2.isSubledgerPosting')==0\
                                                            ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd2.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd2.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd3'),\
                    ((col('lkbd3.businessDatatype')==lit('Batch Input Status'))\
                     & (col('lkbd3.sourceSystemValue').eqNullSafe(trim(col('jnrlAttr2.isBatchInput').cast('String'))) )\
                     & (col('lkbd3.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd3.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd4'),\
                    ((col('lkbd4.businessDatatype')==lit('Reversal Flag'))\
                     & (col('lkbd4.sourceSystemValue').eqNullSafe(trim(col('jnrlAttr2.reversalStatus').cast('String'))) )\
                     & (col('lkbd4.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd4.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd5'),\
                    ((col('lkbd5.businessDatatype')==lit('JE With CritalComment'))\
                     & (col('lkbd5.sourceSystemValue')==lit(when(col('jnrlAttr2.isJEWithCritalComment')==0\
                                                            ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd5.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd5.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd6'),\
                    ((col('lkbd6.businessDatatype')==lit('Out of Balance'))\
                     & (col('lkbd6.sourceSystemValue')==lit(when(col('jnrlAttr2.IsOutofBalance')==0\
                                                            ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd6.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd6.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd7'),\
                    ((col('lkbd7.businessDatatype')==lit('JE Posting In Time Frame'))\
                     & (col('lkbd7.sourceSystemValue').eqNullSafe(col('jnrlAttr2.isJEPostingInTimeFrame')))\
                     & (col('lkbd7.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd7.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd8'),\
                    ((col('lkbd8.businessDatatype')==lit('JE Weekend Posting'))\
                     & (col('lkbd8.sourceSystemValue').eqNullSafe(trim(col('jnrlAttr2.JEWeekendPosting').cast('String'))) )\
                     & (col('lkbd8.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd8.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd9'),\
                    ((col('lkbd9.businessDatatype')==lit('JE With Missing Description'))\
                     & (col('lkbd9.sourceSystemValue')==when(col('jnrlAttr2.isJEWithMissingDescription')==0\
                                                         ,lit('0')).otherwise(lit('1')))\
                     & (col('lkbd9.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd9.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd10'),\
                    ((col('lkbd10.businessDatatype')==lit('JE With Rounded Amount'))\
                     & (col('lkbd10.sourceSystemValue')==lit(when(col('jnrlAttr2.isJEWithRoundedAmount')==0\
                                                                   ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd10.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd10.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd11'),\
                    ((col('lkbd11.businessDatatype')==lit('JE Posted In Period EndDate'))\
                     & (col('lkbd11.sourceSystemValue')==when(col('jnrlAttr2.isJEPostedInPeriodEndDate')==0\
                                                            ,lit('0')).otherwise(lit('1')))\
                     & (col('lkbd11.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd11.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd12'),\
                    ((col('lkbd12.businessDatatype')==lit('JE With Multiple User'))\
                     & (col('lkbd12.sourceSystemValue')==lit(when(col('jnrlAttr2.isJEWithMultipleUser')==0\
                                                            ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd12.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd12.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd13'),\
                    ((col('lkbd13.businessDatatype')==lit('JE With Missing User'))\
                     & (col('lkbd13.sourceSystemValue')==lit(when(col('jnrlAttr2.IsJEWithMissingUser')==0\
                                                            ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd13.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd13.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd14'),\
                    ((col('lkbd14.businessDatatype')==lit('Holiday Posting'))\
                     & (col('lkbd14.sourceSystemValue')==lit(when(col('jnrlAttr2.JEHolidayPosting')==0\
                                                            ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd14.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd14.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd15'),\
                    ((col('lkbd15.businessDatatype')==lit('JE In Analysis Period'))\
                     & (col('lkbd15.sourceSystemValue')==lit(when(col('jnrlAttr2.isJEInAnalysisPeriod')==0\
                                                                 ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd15.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd15.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd16'),\
                    ((col('lkbd16.businessDatatype')==lit('ClearingDocument'))\
                     & (col('lkbd16.sourceSystemValue')==lit(when(col('jnrlAttr2.isClearing')==1\
                                                         ,lit('1')).otherwise(lit('0'))))\
                     & (col('lkbd16.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd16.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd17'),\
                    ((col('lkbd17.businessDatatype')==lit('isInterCoFlag'))\
                     & (col('lkbd17.sourceSystemValue')==lit(when(col('jnrlAttr2.isInterCoFlag')==1\
                                                           ,lit('1')).otherwise(lit('0'))))\
                     & (col('lkbd17.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd17.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd18'),\
                    ((col('lkbd18.businessDatatype')==lit('Negative Posting Indicator'))\
                     & (col('lkbd18.sourceSystemValue')==when(col('jnrlAttr2.isNegativePostingIndicator')==0\
                                                             ,lit('0')).otherwise(lit('1')))\
                     & (col('lkbd18.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd18.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd19'),\
                    ((col('lkbd19.businessDatatype')==lit('JE With Large Amount'))\
                     & (col('lkbd19.sourceSystemValue')==lit(when(col('jnrlAttr2.isJEWithLargeAmount')==0\
                                                               ,lit('0')).otherwise(lit('1'))))\
                     & (col('lkbd19.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd19.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd20'),\
                    ((col('lkbd20.businessDatatype')==lit('SL Billing Document Missing'))\
                     & (col('lkbd20.sourceSystemValue')==when(col('jnrlAttr2.isSLBillingDocumentMissing')==0\
                                                              ,lit('0')).otherwise(lit('1')))\
                     & (col('lkbd20.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd20.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd21'),\
                    ((col('lkbd21.businessDatatype')==lit('SL Purchase Document Link'))\
                     & (col('lkbd21.sourceSystemValue')==when(col('jnrlAttr2.hasSLPurchaseDocumentLink')==0\
                                                           ,lit('0')).otherwise(lit('1')))\
                     & (col('lkbd21.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd21.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd22'),\
                    ((col('lkbd22.businessDatatype')==lit('Purchase Invoice Document Missing'))\
                     & (col('lkbd22.sourceSystemValue')==when(col('jnrlAttr2.isPurchaseInvoiceDocumentMissing')==0\
                                                              ,lit('0')).otherwise(lit('1')))\
                     & (col('lkbd22.targetERPSystemID')==lit(erpSystemIDGeneric))\
                     & (col('lkbd22.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                    ),how='left')\
                     .select(col('jnrlAttr2.JEDocumentClassificationSurrogateKey').alias('JEDocumentClassificationSurrogateKey')\
                             ,col('jnrlAttr2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                             ,col('jnrlAttr2.documentClassificationName').alias('documentClassificationName')\
                             ,col('jnrlAttr2.documentSource').alias('documentSource')\
                             ,col('jnrlAttr2.documentSubSource').alias('documentSubSource')\
                             ,col('jnrlAttr2.documentPreSystem').alias('documentPreSystem')\
                             ,col('lkbd2.targetSystemValueDescription').alias('subLedgerPosting')\
                             ,col('lkbd5.targetSystemValueDescription').alias('JEWithCriticalComment')\
                             ,col('lkbd6.targetSystemValueDescription').alias('unBalancedDocument')\
                             ,lit(new_col_isManualPosting).alias('manualPosting')\
                             ,col('lkbd1.targetSystemValueDescription').alias('manualPostingPrecise')\
                             ,lit(new_col_isManualPostingPrecise).alias('batchInput')\
                            ,col('lkbd4.targetSystemValue').alias('JEReversed')\
                            ,col('lkbd4.targetSystemValueDescription').alias('JEReversedDesc')\
                            ,col('jnrlAttr2.manualPostingAttribute1').alias('manualPostingAttribute1')\
                            ,col('jnrlAttr2.manualPostingAttribute2').alias('manualPostingAttribute2')\
                            ,col('jnrlAttr2.manualPostingAttribute3').alias('manualPostingAttribute3')\
                            ,col('jnrlAttr2.manualPostingAttribute4').alias('manualPostingAttribute4')\
                            ,col('lkbd7.targetSystemValueDescription').alias('JEPostingInTimeFrame')\
                            ,col('lkbd8.targetSystemValueDescription').alias('JEWeekendPosting')\
                            ,col('jnrlAttr2.debitCreditIndicator').alias('debitCreditIndicator')\
                            ,col('lkbd9.targetSystemValueDescription').alias('JEWithMissingDescription')\
                            ,col('lkbd10.targetSystemValueDescription').alias('JEWithRoundedAmount')\
                            ,col('lkbd11.targetSystemValueDescription').alias('JEPostedInPeriodEndDate')\
                            ,col('lkbd12.targetSystemValueDescription').alias('JEWithMultipleUser')\
                            ,col('lkbd13.targetSystemValueDescription').alias('JEWithMissingUser')\
                            ,col('lkbd14.targetSystemValueDescription').alias('JEHolidayPosting')\
                            ,col('lkbd15.targetSystemValueDescription').alias('JEInAnalysisPeriod')\
                            ,col('lkbd16.targetSystemValueDescription').alias('clearing')\
                            ,col('lkbd17.targetSystemValueDescription').alias('InterCompany')\
                            ,when(col('jnrlAttr2.postingDateToDocumentDate').isNull(),lit(0))\
                                .otherwise(col('jnrlAttr2.postingDateToDocumentDate')).alias('differenceInPostingDateToDocumentDate')\
                            ,when(col('jnrlAttr2.postingDateToDocuemntEntryDate').isNull(),lit(0))\
                                .otherwise(col('jnrlAttr2.postingDateToDocuemntEntryDate')).alias('differenceInPostingdateToCreationDate')\
                            ,lit(new_col_documentStatus).alias('documentStatus')\
                            ,col('jnrlAttr2.documentStatusDetail').alias('documentStatusDetail')\
                            ,when(col('jnrlAttr2.hasSLBillingDocumentLink')==1,lit('Subledger Billing Doc Link - Yes'))\
                                .otherwise(lit('Subledger Billing Doc Link - No')).alias('hasSubledgerLink')\
                            ,lit(new_col_intervalBetweenPostingDateTDocumentDate).alias('intervalBetweenPostingDateTDocumentDate')\
                            ,lit(new_col_intervalBetweenPostingDateToCreationDate).alias('intervalBetweenPostingDateToCreationDate')\
                            ,lit(new_col_BetPDateTDocDate_Desc).alias('intervalBetweenPostingDateTDocumentDateDesc')\
                            ,lit(new_col_BetPDateTCreDate_Desc).alias('intervalBetweenPostingDateToCreationDateDesc')\
                            ,col('lkbd18.targetSystemValueDescription').alias('negativePostingIndicator')\
                            ,col('lkbd19.targetSystemValueDescription').alias('JEWithLargeAmount')\
                            ,col('lkbd20.targetSystemValueDescription').alias('SLBillingDocumentMissing')\
                            ,col('lkbd21.targetSystemValueDescription').alias('hasSubledgerPurchaseLink')\
                            ,when(col('lkbd22.targetSystemValueDescription').isNotNull(),col('lkbd22.targetSystemValueDescription'))\
                                .otherwise(lit('NONE')).alias('purchaseInvoiceDocumentMissing')\
                             )
       
        unknown_list1=[[0,0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'\
                ,'NONE','NONE','NONE','NONE','NONE','C','NONE','NONE','NONE','NONE','NONE','NONE',\
                'Is JE In Analysis Period No','NONE','NONE',0,0,\
                'Other','NONE','Subledger Billing Doc Link - No', 0,0,\
                'NONE','NONE','NONE','NONE','NONE','NONE','NONE'    
              ]]
        unknown_list2=[[-1,0,'NONE','NONE','NONE','NONE','Is Subledger Posting - No','NONE','NONE',\
                    'Manual','NONE','NONE','NONE','Not Reversed','NONE','NONE','NONE','NONE'\
                    ,'Is JE Posting In Time Frame No','NONE','C','NONE','NONE','NONE','NONE','NONE'\
                    ,'Is JE Holiday Posting Yes','Is JE In Analysis Period No','NONE','NONE',0,0,\
                    'Normal','NONE','Subledger Billing Doc Link - No', 0,0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE'    
                   ]]
        unknown_list3=[(-2,0,'NONE','NONE','NONE','NONE','Is Subledger Posting - Yes','NONE','NONE',\
                    'Automated','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'\
                    ,'C','NONE','NONE','NONE','NONE','NONE','NONE'\
                    ,'Is JE In Analysis Period No','NONE','NONE',0,0,'Other','NONE'\
                    ,'Subledger Billing Doc Link - Yes', 0,0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE'    
                  )]

        unknown_listAll=[unknown_list1]
        unknown_df1 = spark.createDataFrame(unknown_list1)
        unknown_df2 = spark.createDataFrame(unknown_list2)
        unknown_df3 = spark.createDataFrame(unknown_list3)

        unknown_df1 = unknown_df1.union(unknown_df2)
        unknown_df1 = unknown_df1.union(unknown_df3)

        fin_L3_STG_JEDocumentClassification=fin_L3_STG_JEDocumentClassification.union(unknown_df1)
        
        fin_L3_STG_JEDocumentClassification = objDataTransformation.gen_convertToCDMandCache \
            (fin_L3_STG_JEDocumentClassification,'fin','L3_STG_JEDocumentClassification',True)
       
        vw_DIM_JEDocumentClassification = fin_L3_STG_JEDocumentClassification.alias('DC').\
			             select (col('DC.analysisID').alias('analysisID') ,\
                         col('DC.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey') ,\
                         col('DC.JEDocumentClassificationSurrogateKey').alias('JEDocumentClassificationSurrogateKey') ,\
                         col('DC.documentSource').alias('documentSource') ,\
                         col('DC.documentSubSource').alias('documentSubSource') ,\
                         col('DC.documentPreSystem').alias('documentPreSystem') ,\
                         col('DC.subLedgerPosting').alias('subLedgerPosting') ,\
                         col('DC.JEWithCriticalComment').alias('JEWithCriticalComment') ,\
                         col('DC.unBalancedDocument').alias('unBalancedDocument') ,\
                         col('DC.manualPosting').alias('manualPosting') ,\
                         col('DC.batchInput').alias('batchInput') ,\
                         col('DC.JEReversedDesc').alias('JEReversedDescription') ,\
                         col('DC.JEPostingInTimeFrame').alias('isJECreatedByPeriodEndDate') ,\
                         col('DC.JEWeekendPosting').alias('JEWeekendPosting') ,\
                         col('DC.debitCreditIndicator').alias('debitCreditIndicator') ,\
                         col('DC.JEWithMissingDescription').alias('JEWithMissingDescription') ,\
                         col('DC.JEWithRoundedAmount').alias('JEWithRoundedAmount') ,\
                         col('DC.JEPostedInPeriodEndDate').alias('JEPostedInPeriodEndDate') ,\
                         col('DC.JEWithMultipleUser').alias('JEWithMultipleUser') ,\
                         col('DC.JEWithMissingUser').alias('JEWithMissingUser') ,\
                         col('DC.JEHolidayPosting').alias('JEHolidayPosting') ,\
                         col('DC.JEInAnalysisPeriod').alias('JEInAnalysisPeriod') ,\
                         col('DC.clearing').alias('clearing') ,\
                         col('DC.InterCompany').alias('InterCompany') ,\
                         when(col('DC.differenceInPostingDateToDocumentDate')==lit(-99999999),lit(0))\
                          .otherwise(col('DC.differenceInPostingDateToDocumentDate')).alias('differenceInPostingDateToDocumentDate') ,\
                         col('DC.differenceInPostingdateToCreationDate').alias('differenceInPostingdateToCreationDate') ,\
                         col('DC.documentStatus').alias('documentStatus') ,\
                         col('DC.documentStatusDetail').alias('documentStatusDetail') ,\
                         col('DC.hasSubledgerLink').alias('hasSubledgerLink') ,\
                         col('DC.intervalBetweenPostingDateTDocumentDate').alias('intervalBetweenPostingDateTDocumentDate') ,\
                         col('DC.intervalBetweenPostingDateToCreationDate').alias('intervalBetweenPostingDateToCreationDate') ,\
                         col('DC.intervalBetweenPostingDateTDocumentDateDesc').alias('intervalBetweenPostingDateTDocumentDateDescription') ,\
                         col('DC.intervalBetweenPostingDateToCreationDateDesc').alias('intervalBetweenPostingDateToCreationDateDescription') ,\
                         col('DC.negativePostingIndicator').alias('negativePostingIndicator') ,\
                         col('DC.JEWithLargeAmount').alias('JEWithLargeAmount') ,\
                         col('DC.SLBillingDocumentMissing').alias('SLBillingDocumentMissing') ,\
                         col('DC.purchaseInvoiceDocumentMissing').alias('purchaseInvoiceDocumentMissing') ,\
                         col('DC.hasSubledgerPurchaseLink').alias('hasSubledgerPurchaseLink') )

        dwh_vw_DIM_JEDocumentClassification = objDataTransformation.gen_convertToCDMStructure_generate(vw_DIM_JEDocumentClassification,\
                                                      'dwh','vw_DIM_JEDocumentClassification',False)[0]
        objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_JEDocumentClassification,\
                                                      gl_CDMLayer2Path + "fin_L2_DIM_JEDocumentClassification.parquet" )
      
        executionStatus = "L3_STG_JEDocumentClassification populated successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
        


