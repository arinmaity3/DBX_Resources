# Databricks notebook source
from pyspark.sql.functions import trim
from pyspark.sql.window import Window
import sys
import traceback

def fin_L2_FACT_Journal_populate():
    """Populate L2_FACT_Journal"""
    
    try:
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global fin_L2_FACT_Journal
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      w = Window().orderBy(lit(''))

      JETransactions = fin_L1_TD_Journal.select(col("journalSurrogateKey"),\
                        col("companyCode"),\
                        col("documentNumber"),col("lineItem"),\
                        col("assignmentNumber"),col("headerDescription"),\
                        col("lineDescription"),col("reversalDocumentNumber"),\
                        col("clearingDocumentNumber"),col("amountDC"),\
                        col("amountLC"),col("quantity"),\
                        col("referenceDocumentNumber"),col("referenceSubledgerDocumentNumber"),\
                        col("transactionType"),col("transactionTypeDescription"),\
                        col("debitCreditIndicator"),col("fiscalYear"),col("reversalID"))

      fileNamePath = gl_Layer2Staging_temp + "JETransactions.delta"
      JETransactions.write.format("delta").mode("overwrite").save(fileNamePath)
      JETransactions = objGenHelper.gen_readFromFile_perform(fileNamePath)
 
      JEAdditionalAttribute = fin_L2_STG_JEAdditionalAttribute.\
                                select(col("journalSurrogateKey"),\
                                col("JEAdditionalAttributeSurrogateKey"))      

      fileNamePath = gl_Layer2Staging_temp + "JEAdditionalAttribute.delta"
      JEAdditionalAttribute.write.format("delta").mode("overwrite").save(fileNamePath)
      JEAdditionalAttribute = objGenHelper.gen_readFromFile_perform(fileNamePath)


      JEDocumentNumberWithCompanyCode = JETransactions.\
                                        select(col("companyCode"),\
                                        col("fiscalYear"),\
                                        col("documentNumber")).distinct().\
                                        withColumn("documentNumberWithCompanyCode", row_number().over(w))

      fileNamePath = gl_Layer2Staging_temp + "JEDocumentNumberWithCompanyCode.delta"
      JEDocumentNumberWithCompanyCode.write.format("delta").mode("overwrite").save(fileNamePath)
      JEDocumentNumberWithCompanyCode = objGenHelper.gen_readFromFile_perform(fileNamePath)

      if JETransactions.count() > 0:
          maxLineLength = JETransactions.withColumn("len_Description",length(col("lineItem"))).\
                    groupBy().max("len_Description").\
                    first()[0]+1
      else :
          maxLineLength = 0
      zeroString = maxLineLength*'0'
      
      dr_window = Window.partitionBy().orderBy("dou2.organizationUnitSurrogateKey","jou1.documentNumber")

      new_col_amountLCFirstDigit = when(abs(col('jou1.amountLC')) < 1, \
                                      substring(regexp_replace(regexp_replace(abs(col('jou1.amountLC')),'0',''),'\\.',''),1,1))\
                                .otherwise(substring(abs(col('jou1.amountLC')),1,1))

      new_col_amountDCFirstDigit = when(abs(col('jou1.amountDC')) < 1, \
                                      substring(regexp_replace(regexp_replace(abs(col('jou1.amountDC')),'0',''),'\\.',''),1,1))\
                                .otherwise(substring(abs(col('jou1.amountDC')),1,1))

      fin_L2_FACT_Journal=  broadcast(gen_L2_DIM_Organization).alias('dou2')\
              .join(JETransactions.alias('jou1'),\
               ((col('dou2.companyCode')== col('jou1.companyCode'))\
                & (col('dou2.companyCode') !=  lit('#NA#'))),how='inner')\
              .join(JEAdditionalAttribute.alias('jrnad'),\
                ((col('jrnad.journalSurrogateKey')== col('jou1.journalSurrogateKey')))\
                 ,how='inner')\
              .join(fin_L2_STG_Journal_05_prepare.alias('jrnl05'),\
                ((col('jrnl05.journalSurrogateKey')== col('jou1.journalSurrogateKey'))\
                ),how='inner')\
              .join(fin_L2_STG_Journal_02_prepare.alias('jrnl02'),\
                 ((col('jrnl02.journalSurrogateKey')== col('jou1.journalSurrogateKey'))\
                 ),how='inner')\
             .join(fin_L1_STG_JEDocumentClassification.alias('jat1'),\
                 ((col('jat1.journalSurrogateKey')== col('jou1.journalSurrogateKey'))\
                 ),how='inner')\
             .join(fin_L2_STG_Journal_01_prepare.alias('jrnl01'),\
                 ((col('jrnl01.journalSurrogateKey')== col('jou1.journalSurrogateKey'))\
                 ),how='inner')\
             .join(fin_L2_STG_Journal_03_prepare.alias('jrnl03'),\
                 ((col('jrnl03.journalSurrogateKey')== col('jou1.journalSurrogateKey'))\
                  ),how='inner')\
             .join(fin_L2_STG_Journal_04_prepare.alias('jrnl04'),\
                   ((col('jrnl04.journalSurrogateKey')== col('jou1.journalSurrogateKey'))\
                   ),how='inner')\
             .join(JEDocumentNumberWithCompanyCode.alias('jrnlDoc'),\
                  ((col('jrnlDoc.companyCode') == col('jou1.companyCode'))\
                  & (col('jrnlDoc.fiscalYear') == col('jou1.fiscalYear')) \
                  & (col('jrnlDoc.documentNumber') == col('jou1.documentNumber'))\
                  ),how = 'inner')\
             .select(col('jou1.journalSurrogateKey').alias('journalSurrogateKey')\
                       ,col('dou2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                       ,col('jrnl01.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
                       ,(when(col('jou1.debitCreditIndicator')=='C', col('jrnl01.glAccountSurrogateKey'))\
                          .otherwise(lit(0))).alias('creditGlAccountSurrogateKey')\
                        ,(when(col('jou1.debitCreditIndicator')=='D', col('jrnl01.glAccountSurrogateKey'))\
                          .otherwise(lit(0))).alias('debitGlAccountSurrogateKey')\
                       ,col('jrnl01.vendorSurrogateKey').alias('vendorSurrogateKey')\
                       ,col('jrnl01.customerSurrogateKey').alias('customerSurrogateKey')\
                       ,col('jrnl01.documentTypeSurrogateKey').alias('documentTypeSurrogateKey')\
                       ,col('jrnl01.transactionCodeSurrogateKey').alias('transactionCodeSurrogateKey')\
                       ,col('jrnl01.postingSurrogateKey').alias('postingSurrogateKey')\
                       ,col('jrnl01.businessTransactionSurrogateKey').alias('businessTransactionSurrogateKey')\
                       ,col('jrnl01.referenceTransactionSurrogateKey').alias('referenceTransactionSurrogateKey')\
                       ,col('jrnl01.customAmount').alias('customAmount')\
                       ,col('jrnl01.customDate').alias('customDate')\
                       ,col('jrnl01.customText').alias('customText')\
                       ,col('jrnl01.customText02').alias('customText02')\
                       ,col('jrnl01.customText03').alias('customText03')\
                       ,col('jrnl01.customText04').alias('customText04')\
                       ,col('jrnl01.customText05').alias('customText05')\
                       ,col('jrnl01.customText06').alias('customText06')\
                       ,col('jrnl01.customText07').alias('customText07')\
                       ,col('jrnl01.customText08').alias('customText08')\
                       ,col('jrnl01.customText09').alias('customText09')\
                       ,col('jrnl01.customText10').alias('customText10')\
                       ,col('jrnl01.customText11').alias('customText11')\
                       ,col('jrnl01.customText12').alias('customText12')\
                       ,col('jrnl01.customText13').alias('customText13')\
                       ,col('jrnl01.customText14').alias('customText14')\
                       ,col('jrnl01.customText15').alias('customText15')\
                       ,col('jrnl01.customText16').alias('customText16')\
                       ,col('jrnl01.customText17').alias('customText17')\
                       ,col('jrnl01.customText18').alias('customText18')\
                       ,col('jrnl01.customText19').alias('customText19')\
                       ,col('jrnl01.customText20').alias('customText20')\
                       ,col('jrnl01.customText21').alias('customText21')\
                       ,col('jrnl01.customText22').alias('customText22')\
                       ,col('jrnl01.customText23').alias('customText23')\
                       ,col('jrnl01.customText24').alias('customText24')\
                       ,col('jrnl01.customText25').alias('customText25')\
                       ,col('jrnl01.customText26').alias('customText26')\
                       ,col('jrnl01.customText27').alias('customText27')\
                       ,col('jrnl01.customText28').alias('customText28')\
                       ,col('jrnl01.customText29').alias('customText29')\
                       ,col('jrnl01.customText30').alias('customText30')\
                       ,col('jrnl02.lineRangeSurrogateKey').alias('lineRangeSurrogateKey')\
                       ,when(col('jrnl02.amountRangeSurrogateKey').isNotNull(),\
                              col('jrnl02.amountRangeSurrogateKey')).otherwise(lit(-1)).alias('amountRangeSurrogateKey')\
                       ,when(col('jrnl02.holidaySurrogateKey').isNotNull(),\
                              col('jrnl02.holidaySurrogateKey')).otherwise(lit(-1)).alias('holidaySurrogateKey')\
                       ,col('jrnl02.je_LinePatternID').alias('lineStatisticsSurrogateKey')\
                       ,col('jrnl02.je_footPrintPatternID').alias('footPrintSurrogateKey')\
                       ,col('jrnl03.userSurrogateKey').alias('userSurrogateKey')\
                       ,col('jrnl03.productSurrogateKey').alias('productSurrogateKey')\
                       ,col('jrnl03.documentCurrencySurrogateKey').alias('documentCurrencySurrogateKey')\
                       ,col('jrnl03.shiftedAccountingTimeSurrogateKey').alias('shiftedAccountingTimeSurrogateKey')\
                       ,col('jrnl03.shiftedDateOfEntrySurrogateKey').alias('shiftedDateOfEntrySurrogateKey')\
                       ,col('jrnl03.approvedByuserSurrogateKey').alias('approvedByuserSurrogateKey')\
                       ,col('jrnl03.shiftedApprovalDateSurrogateKey').alias('shiftedApprovalDateSurrogateKey')\
                       ,col('jat1.JEDocumentClassificationKey').alias('JEDocumentClassificationSurrogateKey')\
                       ,col('jat1.JEPostPeriodDays').alias('postPeriodDays')\
                       ,col('jrnl04.firstDigitOfAmount').alias('firstDigitOfAmount')\
                       ,col('jrnl04.consistentEndingNumberOfDigit').alias('consistentEndingNumberOfDigit')\
                       ,col('jrnl04.quantityInSign').alias('quantityInSign')\
                       ,col('jrnl04.amountLCInSign').alias('amountLCInSign')\
                       ,col('jrnl04.JECriticalCommentSurrogateKey').alias('JECriticalCommentSurrogateKey')\
                       ,when(col('jat1.isNegativePostingIndicator').isNotNull(),\
                              col('jat1.isNegativePostingIndicator')).otherwise(lit(False)).alias('isNegativePostingIndicator')\
                       ,col('jrnl05.postingDateSurrogateKey').alias('postingDateSurrogateKey')\
                       ,col('jrnl05.documentCreationDateSurrogateKey').alias('documentCreationDateSurrogateKey')\
                       ,col('jrnl05.clearingDateSurrogateKey').alias('clearingDateSurrogateKey')\
                       ,when(col('jrnl05.documentDateSurrogateKey').isNotNull(),\
                              col('jrnl05.documentDateSurrogateKey')).otherwise(lit(-1)).alias('documentDateSurrogateKey')\
                       ,col('jrnl05.documentReversalFiscalYear').alias('documentReversalFiscalYear')\
                       ,col('jrnl05.approvalDateSurrogateKey').alias('approvalDateSurrogateKey')\
                       ,col('jrnad.JEAdditionalAttributeSurrogateKey').alias('JEAdditionalAttributeSurrogateKey')\
                       ,col('jou1.documentNumber').alias('documentNumber')\
                       ,col('jou1.lineItem').alias('documentLineNumber')\
                       ,col('jou1.assignmentNumber').alias('assignmentNumber')\
                       ,col('jou1.headerDescription').alias('documentHeaderDescription')\
                       ,col('jou1.lineDescription').alias('documentLineDescription')\
                       ,col('jou1.reversalDocumentNumber').alias('reverseDocumentNumber')\
                       ,col('jou1.clearingDocumentNumber').alias('clearingDocumentNumber')\
                       ,col('jou1.amountDC').alias('amountDC')\
                       ,col('jou1.amountLC').alias('amountLC')\
                       ,col('jou1.quantity').alias('quantity')\
                       ,col('jou1.referenceDocumentNumber').alias('referenceDocumentNumber')\
                       ,col('jou1.referenceSubledgerDocumentNumber').alias('referenceSubledgerDocumentNumber')\
                       ,when(col('jou1.reversalID').isNotNull(),col('jou1.reversalID')).otherwise(lit(0)).alias('reversalID')\
                       ,substring(concat(lit(zeroString),col('jou1.lineItem')),-maxLineLength,maxLineLength)\
                           .alias('documentLineNumberSort')\
                       #,F.dense_rank().over(dr_window).alias('documentNumberWithCompanyCode')\
                       ,col('jrnlDoc.documentNumberWithCompanyCode').alias('documentNumberWithCompanyCode')\
                       ,lit(new_col_amountLCFirstDigit).alias('amountLCFirstDigit')\
                       ,lit(new_col_amountDCFirstDigit).alias('amountDCFirstDigit')\
                       ,col('jou1.transactionType').alias('transactionType')\
                       ,col('jou1.transactionTypeDescription').alias('transactionTypeDescription')\
                       ,col('jrnl04.lineItemCount').alias('lineItemCount')
                      )
     
      fin_L2_FACT_Journal = objDataTransformation.gen_convertToCDMandCache \
          (fin_L2_FACT_Journal,'fin','L2_FACT_Journal',True)

      recordCount2 = fin_L2_FACT_Journal.count()
      
      if gl_countJET != recordCount2:
        executionStatus="Record counts are not reconciled [fin].[L2_FACT_Journal]"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      else:
          executionStatus = "L2_FACT_Journal populated successfully"
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
          return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
     executionStatus = objGenHelper.gen_exceptionDetails_log()
     print(executionStatus)
     executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
     return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


