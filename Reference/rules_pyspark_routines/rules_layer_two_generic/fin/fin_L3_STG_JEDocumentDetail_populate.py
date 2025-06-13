# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim

def fin_L3_STG_JEDocumentDetail_populate():
    try:
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global fin_vw_FACT_JEDocumentDetail
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      vw_FACT_JEDocumentDetail = fin_L2_FACT_Journal.alias('jrnl2')\
		  .join(fin_L2_DIM_JEDocumentClassification.alias('docCl2')\
						,((col('jrnl2.JEDocumentClassificationSurrogateKey') ==\
                           col('docCl2.JEDocumentClassificationSurrogateKey'))\
                        & (col('docCl2.isJEInAnalysisPeriod') == lit('1').cast("string")) ) \
                               ,'inner')\
                .join(gen_L2_DIM_Currency.alias('cur2')\
							, (col('cur2.currencySurrogateKey') == col('jrnl2.documentCurrencySurrogateKey')),'inner')\
				.select(col('jrnl2.journalSurrogateKey').alias('journalSurrogateKey')\
					   ,col('jrnl2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
					,col('jrnl2.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
					,col('jrnl2.creditGlAccountSurrogateKey').alias('creditGlAccountSurrogateKey')\
					,col('jrnl2.debitGlAccountSurrogateKey').alias('debitGlAccountSurrogateKey')\
					,col('jrnl2.vendorSurrogateKey').alias('vendorSurrogateKey')\
					,col('jrnl2.customerSurrogateKey').alias('customerSurrogateKey')\
					,col('jrnl2.documentTypeSurrogateKey').alias('documentTypeSurrogateKey')\
					,col('jrnl2.transactionCodeSurrogateKey').alias('transactionCodeSurrogateKey')\
					,col('jrnl2.userSurrogateKey').alias('userSurrogateKey')\
					,col('jrnl2.productSurrogateKey').alias('productSurrogateKey')\
					,col('jrnl2.holidaySurrogateKey').alias('holidaySurrogateKey')\
					,col('jrnl2.documentCurrencySurrogateKey').alias('documentCurrencySurrogateKey')\
					,col('jrnl2.postingDateSurrogateKey').alias('postingDateSurrogateKey')\
					,col('jrnl2.documentCreationDateSurrogateKey').alias('documentCreationDateSurrogateKey')\
					,col('jrnl2.clearingDateSurrogateKey').alias('clearingDateSurrogateKey')\
					,col('jrnl2.documentDateSurrogateKey').alias('documentDateSurrogateKey')\
					,col('jrnl2.shiftedDateOfEntrySurrogateKey').alias('shiftedDateOfEntrySurrogateKey')\
					,col('jrnl2.shiftedAccountingTimeSurrogateKey').alias('shiftedAccountingTimeSurrogateKey')\
					,col('jrnl2.documentReversalFiscalYear').alias('documentReversalFiscalYear')\
					,col('jrnl2.approvalDateSurrogateKey').alias('approvalDateSurrogateKey')\
					,col('jrnl2.approvedByuserSurrogateKey').alias('approvedByuserSurrogateKey')\
					,col('jrnl2.shiftedApprovalDateSurrogateKey').alias('shiftedApprovalDateSurrogateKey')\
					,col('jrnl2.lineRangeSurrogateKey').alias('lineRangeSurrogateKey')\
					,col('jrnl2.amountRangeSurrogateKey').alias('amountRangeSurrogateKey')\
					,col('jrnl2.JEDocumentClassificationSurrogateKey').alias('JEDocumentClassificationSurrogateKey')\
					,col('jrnl2.postingSurrogateKey').alias('postingSurrogateKey')\
					,col('jrnl2.businessTransactionSurrogateKey').alias('businessTransactionSurrogateKey')\
					,col('jrnl2.referenceTransactionSurrogateKey').alias('referenceTransactionSurrogateKey')\
					,col('jrnl2.JECriticalCommentSurrogateKey').alias('JECriticalCommentSurrogateKey')\
					,col('jrnl2.lineStatisticsSurrogateKey').alias('lineStatisticsSurrogateKey')\
					,col('jrnl2.footPrintSurrogateKey').alias('footPrintSurrogateKey')\
					,col('jrnl2.JEAdditionalAttributeSurrogateKey').alias('jeAdditionalAttributeSurrogateKey')\
					,col('jrnl2.documentNumber').alias('documentNumber')\
					,col('jrnl2.documentLineNumber').alias('documentLineNumber')\
					,concat(col('jrnl2.documentNumber'),lit('-'),col('jrnl2.documentLineNumber')).alias('documentNumberLineNumber')\
					,col('jrnl2.assignmentNumber').alias('assignmentNumber')\
					,col('jrnl2.postPeriodDays').alias('postPeriodDays')\
					,when (trim(col('jrnl2.documentHeaderDescription')).isNull(),lit(''))\
					    .otherwise(trim(col('jrnl2.documentHeaderDescription'))).alias('documentHeaderDescription')\
					,when(trim(col('jrnl2.documentLineDescription')).isNull(),lit(''))\
					    .otherwise(trim(col('jrnl2.documentLineDescription'))).alias('documentLineDescription')\
					,col('jrnl2.reverseDocumentNumber').alias('reverseDocumentNumber')\
					,col('jrnl2.referenceDocumentNumber').alias('referenceDocumentNumber')\
					,col('jrnl2.referenceSubledgerDocumentNumber').alias('referenceSubledgerDocumentNumber')\
					,col('jrnl2.clearingDocumentNumber').alias('clearingDocumentNumber')\
					,when(col('jrnl2.reversalID').isNull(),lit(0))\
					    .otherwise(col('jrnl2.reversalID')).alias('reversalID')\
					,when(col('jrnl2.firstDigitOfAmount').isNull(),lit(0))\
					    .otherwise(col('jrnl2.firstDigitOfAmount')).alias('firstDigitOfAmount')\
					,when(col('jrnl2.consistentEndingNumberOfDigit').isNull(),lit(0))\
					    .otherwise(col('jrnl2.consistentEndingNumberOfDigit')).alias('consistentEndingNumberOfDigit')\
					,col('jrnl2.quantityInSign').alias('quantityInSign')\
					,col('jrnl2.amountLCInSign').alias('amountLCInSign')\
					,col('jrnl2.amountDC').alias('amountDC')\
					,col('jrnl2.amountLC').alias('amountLC')\
					,col('jrnl2.amountRC').alias('amountRC')\
					,col('jrnl2.quantity').alias('quantity')\
					,col('jrnl2.customDate').alias('customDate')\
					,col('jrnl2.customAmount').alias('customAmount')\
					,concat((when (trim(col('jrnl2.documentHeaderDescription')).isNull(),lit(''))\
					    .otherwise(trim(col('jrnl2.documentHeaderDescription')))),lit(''),(when(trim(col('jrnl2.documentLineDescription')).isNull(),lit(''))\
					    .otherwise(trim(col('jrnl2.documentLineDescription'))))).alias('header_line_description')\
					,lit('1').alias('partitionKeyForGLACube')\
					,when(col('docCl2.debitCreditIndicator')=='C',col('jrnl2.amountLCInSign'))\
					    .otherwise(lit(0)).alias('amountCredit')\
					,when(col('docCl2.debitCreditIndicator')=='D',col('jrnl2.amountLCInSign'))\
					    .otherwise(lit(0)).alias('amountDebit')\
					,col('jrnl2.documentNumberWithCompanyCode').alias('documentNumberWithCompanyCode')\
					,col('cur2.currencyISOCode').alias('documentCurrency')\
					,col('jrnl2.lineItemCount').alias('lineItemCount')\
					,col('jrnl2.documentLineNumberSort').alias('documentLineNumberSort')\
					,col('docCl2.documentStatus').alias('documentStatus')\
					,col('jrnl2.amountLCFirstDigit').alias('amountLCFirstDigit')\
					,col('jrnl2.amountDCFirstDigit').alias('amountDCFirstDigit')\
					,col('jrnl2.transactionType').alias('transactionType')\
					,col('jrnl2.transactionTypeDescription').alias('transactionTypeDescription')\
					,col('jrnl2.customText').alias('customText')\
					,col('jrnl2.customText02').alias('customText02')\
					,col('jrnl2.customText03').alias('customText03')\
					,col('jrnl2.customText04').alias('customText04')\
					,col('jrnl2.customText05').alias('customText05')\
					,col('jrnl2.customText06').alias('customText06')\
					,col('jrnl2.customText07').alias('customText07')\
					,col('jrnl2.customText08').alias('customText08')\
					,col('jrnl2.customText09').alias('customText09')\
					,col('jrnl2.customText10').alias('customText10')\
					,col('jrnl2.customText11').alias('customText11')\
					,col('jrnl2.customText12').alias('customText12')\
					,col('jrnl2.customText13').alias('customText13')\
					,col('jrnl2.customText14').alias('customText14')\
					,col('jrnl2.customText15').alias('customText15')\
					,col('jrnl2.customText16').alias('customText16')\
					,col('jrnl2.customText17').alias('customText17')\
					,col('jrnl2.customText18').alias('customText18')\
					,col('jrnl2.customText19').alias('customText19')\
					,col('jrnl2.customText20').alias('customText20')\
					,col('jrnl2.customText21').alias('customText21')\
					,col('jrnl2.customText22').alias('customText22')\
					,col('jrnl2.customText23').alias('customText23')\
					,col('jrnl2.customText24').alias('customText24')\
					,col('jrnl2.customText25').alias('customText25')\
					,col('jrnl2.customText26').alias('customText26')\
					,col('jrnl2.customText27').alias('customText27')\
					,col('jrnl2.customText28').alias('customText28')\
					,col('jrnl2.customText29').alias('customText29')\
					,col('jrnl2.customText30').alias('customText30'))

      dummy_list = [[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,\
		             '0000000000','00','000000000000','NONE',0,'NONE','NONE','NONE','NONE','NONE','NONE',\
					 0,0,0,0,0,0,0,0,0,'1900-01-01',0,'NONE',-1,0,0,0,'NA',0,'NONE','O',0,0,'NONE','NONE',\
					 'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'\
                    ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'\
                    ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE']]

      dummy_df = spark.createDataFrame(dummy_list)
      fin_vw_FACT_JEDocumentDetail = vw_FACT_JEDocumentDetail.union(dummy_df)
      
      fin_vw_FACT_JEDocumentDetail  = objDataTransformation.gen_convertToCDMandCache \
		  (fin_vw_FACT_JEDocumentDetail,'fin','vw_FACT_JEDocumentDetail',False)

      executionStatus = "fin_vw_FACT_JEDocumentDetail populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


