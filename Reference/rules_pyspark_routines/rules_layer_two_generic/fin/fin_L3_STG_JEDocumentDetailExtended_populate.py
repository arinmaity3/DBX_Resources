# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim

class fin_L3_STG_JEDocumentDetailExtended_populate():
	@staticmethod
	def dwhvwFACTJEDocumentDetail_populate():
		try:  
			objGenHelper = gen_genericHelper()
			objDataTransformation = gen_dataTransformation()
			global dwh_vw_FACT_JEDocumentDetail
			logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
			
			vwFACT_JEDocumentDetail = fin_vw_FACT_JEDocumentDetail.alias('jrnl2')\
	                .select(col('jrnl2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
				    ,col('jrnl2.journalSurrogateKey').alias('journalSurrogateKey')\
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
					,col('jrnl2.jeAdditionalAttributeSurrogateKey').cast("int").alias('jeAdditionalAttributeSurrogateKey')\
					,col('jrnl2.quantityInSign').alias('quantityInSign')\
					,col('jrnl2.amountLCInSign').alias('amountLCInSign')\
					,col('jrnl2.amountDC').alias('amountDC')\
					,col('jrnl2.amountLC').alias('amountLC')\
					,col('jrnl2.amountRC').alias('amountRC')\
					,col('jrnl2.quantity').alias('quantity')\
					,col('jrnl2.customAmount').alias('customAmount')\
					,col('jrnl2.amountCredit').alias('amountCredit')\
					,col('jrnl2.amountDebit').alias('amountDebit')\
					,col('jrnl2.documentNumberWithCompanyCode').alias('documentNumberWithCompanyCode')\
					,col('jrnl2.documentStatus').alias('documentStatus')\
					,col('jrnl2.lineItemCount').alias('lineItemCount')\
					,col('jrnl2.amountLCFirstDigit').alias('amountLCFirstDigit')\
					,col('jrnl2.amountDCFirstDigit').alias('amountDCFirstDigit'))
			
			dwh_vw_FACT_JEDocumentDetail = objDataTransformation.gen_convertToCDMStructure_generate(\
				                           vwFACT_JEDocumentDetail,'dwh','vw_FACT_JEDocumentDetail',True)[0]
			objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_JEDocumentDetail,\
				                            gl_CDMLayer2Path + "fin_L2_FACT_Journal.parquet" )
			
			executionStatus = "dwh_vw_FACT_JEDocumentDetail populated sucessfully"
			executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
			return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
		
		except Exception as err:
			executionStatus = objGenHelper.gen_exceptionDetails_log()
			executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
			return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

	@staticmethod
	def dwhvwFACTJournalExtended_populate():
		try:
			
			objGenHelper = gen_genericHelper()
			objDataTransformation = gen_dataTransformation()
			global dwh_vw_FACT_JournalExtended
			logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
			
			vwFACT_JournalExtended = fin_vw_FACT_JEDocumentDetail.alias('jrnl2')\
	                      .select(col('jrnl2.journalSurrogateKey').alias('journalSurrogateKey')\
							,col('jrnl2.documentHeaderDescription').alias('documentHeaderDescription')\
							,col('jrnl2.documentLineDescription').alias('documentLineDescription')\
							,col('jrnl2.documentNumber').alias('documentNumber')\
							,col('jrnl2.documentLineNumber').alias('documentLineNumber')\
							,col('jrnl2.assignmentNumber').alias('assignmentNumber')\
							,col('jrnl2.reverseDocumentNumber').alias('reverseDocumentNumber')\
							,col('jrnl2.referenceDocumentNumber').alias('referenceDocumentNumber')\
							,col('jrnl2.referenceSubledgerDocumentNumber').alias('referenceSubledgerDocumentNumber')\
							,col('jrnl2.clearingDocumentNumber').alias('clearingDocumentNumber')\
							,col('jrnl2.documentCurrency').alias('documentCurrency')\
							,col('jrnl2.documentLineNumberSort').alias('documentLineNumberSort')\
							,col('jrnl2.documentReversalFiscalYear').alias('documentReversalFiscalYear')\
							,col('jrnl2.consistentEndingNumberOfDigit').alias('consistentEndingNumberOfDigit')\
							,col('jrnl2.transactionType').alias('transactionType')\
							,col('jrnl2.transactionTypeDescription').alias('transactionTypeDescription')\
							,col('jrnl2.customDate').alias('customDate')\
							,col('jrnl2.firstDigitOfAmount').alias('firstDigitOfAmount')\
							,col('jrnl2.postPeriodDays').alias('postPeriodDays')\
							,col('jrnl2.reversalID').alias('reversalID')\
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
			
			dwh_vw_FACT_JournalExtended = objDataTransformation.gen_convertToCDMStructure_generate(vwFACT_JournalExtended,'dwh',\
				                      'vw_FACT_JournalExtended',isIncludeAnalysisID=True,isSqlFormat=True)[0]
			objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_JournalExtended,gl_CDMLayer2Path + "fin_L2_FACT_JournalExtended.parquet" )

			executionStatus = "dwh_vw_FACT_JournalExtended populated sucessfully"
			executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
			return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
		
		except Exception as err:
			executionStatus = objGenHelper.gen_exceptionDetails_log()
			executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
			return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
		
	@staticmethod
	def dwhvwFACTJournalAggregated_populate():
		try:
			objGenHelper = gen_genericHelper()
			objDataTransformation = gen_dataTransformation()
			global dwh_vw_FACT_JournalAggregated
			logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
			
			vwFACT_JournalAggregated = fin_vw_FACT_JEDocumentDetail.alias('jrnl2')\
	                                .groupBy(col('jrnl2.organizationUnitSurrogateKey')\
									    ,col('jrnl2.glAccountSurrogateKey')\
										,col('jrnl2.userSurrogateKey')\
										,col('jrnl2.approvedByuserSurrogateKey')\
										,col('jrnl2.postingDateSurrogateKey')\
										,col('jrnl2.clearingDateSurrogateKey')\
										,col('jrnl2.documentDateSurrogateKey')\
										,col('jrnl2.shiftedApprovalDateSurrogateKey')\
										,col('jrnl2.shiftedDateOfEntrySurrogateKey')\
										,col('jrnl2.shiftedAccountingTimeSurrogateKey')\
										,col('jrnl2.amountRangeSurrogateKey')\
										,col('jrnl2.JEDocumentClassificationSurrogateKey')\
										,col('jrnl2.jeAdditionalAttributeSurrogateKey')\
										,col('jrnl2.footPrintSurrogateKey')\
										,col('jrnl2.documentNumberWithCompanyCode')\
										,col('jrnl2.documentStatus'))\
										.agg(sum(col('jrnl2.amountLCInSign')).alias('amountLCInSign')\
										,sum(col('jrnl2.amountLC')).alias('amountLC')\
										,sum(col('jrnl2.amountDC')).alias('amountDC')\
										,sum(col('jrnl2.amountCredit')).alias('amountCredit')\
										,sum(col('jrnl2.amountDebit')).alias('amountDebit')\
										,sum(col('jrnl2.customAmount')).alias('customAmount')\
										,count(lit(1)).alias('RowCount'))
			
			dwh_vw_FACT_JournalAggregated = objDataTransformation.gen_convertToCDMStructure_generate(\
				                            vwFACT_JournalAggregated,'dwh','vw_FACT_JournalAggregated',True)[0]
			objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_JournalAggregated,\
				                            gl_CDMLayer2Path + "fin_L2_FACT_JournalAggregated.parquet" )

			executionStatus = "dwh_vw_FACT_JournalAggregated populated sucessfully"
			executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
			return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
		
		except Exception as err:
			executionStatus = objGenHelper.gen_exceptionDetails_log()
			executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
			return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
			
		
