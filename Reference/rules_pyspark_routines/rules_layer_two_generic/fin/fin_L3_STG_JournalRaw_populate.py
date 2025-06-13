# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim
from pyspark.sql.functions import monotonically_increasing_id as mi
from dateutil.parser import parse 

def fin_L3_STG_JournalRaw_populate():
	try:
		logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
		objGenHelper = gen_genericHelper()
		objDataTransformation = gen_dataTransformation()
		global dwh_vw_FACT_Journal_Raw

		dwh_vw_FACT_Journal_Raw = fin_vw_FACT_JEDocumentDetail.alias('jd')\
	                .join(fin_L2_FACT_Journal_Extended.alias('je')\
							,(col('jd.journalSurrogateKey').eqNullSafe(col('je.journalSurrogateKey'))),'left')\
					.join(fin_L2_FACT_Journal_Extended_Link.alias('jl')\
							,(col('jd.journalSurrogateKey').eqNullSafe(col('jl.journalSurrogateKey'))),'left')\
					.select(col('jd.journalSurrogateKey').alias('journalSurrogateKey')\
							,col('jd.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
							,col('jd.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
							,col('jd.creditGlAccountSurrogateKey').alias('creditGlAccountSurrogateKey')\
							,col('jd.debitGlAccountSurrogateKey').alias('debitGlAccountSurrogateKey')\
							,col('jd.vendorSurrogateKey').alias('vendorSurrogateKey')\
							,col('jd.customerSurrogateKey').alias('customerSurrogateKey')\
							,col('jd.documentTypeSurrogateKey').alias('documentTypeSurrogateKey')\
							,col('jd.transactionCodeSurrogateKey').alias('transactionCodeSurrogateKey')\
							,col('jd.userSurrogateKey').alias('userSurrogateKey')\
							,col('jd.productSurrogateKey').alias('productSurrogateKey')\
							,col('jd.holidaySurrogateKey').alias('holidaySurrogateKey')\
							,col('jd.documentCurrencySurrogateKey').alias('documentCurrencySurrogateKey')\
							,col('jd.postingDateSurrogateKey').alias('postingDateSurrogateKey')\
							,col('jd.documentCreationDateSurrogateKey').alias('documentCreationDateSurrogateKey')\
							,col('jd.clearingDateSurrogateKey').alias('clearingDateSurrogateKey')\
							,col('jd.documentDateSurrogateKey').alias('documentDateSurrogateKey')\
							,col('jd.shiftedDateOfEntrySurrogateKey').alias('shiftedDateOfEntrySurrogateKey')\
							,col('jd.shiftedAccountingTimeSurrogateKey').alias('shiftedAccountingTimeSurrogateKey')\
							,col('jd.documentReversalFiscalYear').alias('documentReversalFiscalYear')\
							,col('jd.approvalDateSurrogateKey').alias('approvalDateSurrogateKey')\
							,col('jd.approvedByuserSurrogateKey').alias('approvedByuserSurrogateKey')\
							,col('jd.shiftedApprovalDateSurrogateKey').alias('shiftedApprovalDateSurrogateKey')\
							,col('jd.lineRangeSurrogateKey').alias('lineRangeSurrogateKey')\
							,col('jd.amountRangeSurrogateKey').alias('amountRangeSurrogateKey')\
							,col('jd.JEDocumentClassificationSurrogateKey').alias('JEDocumentClassificationSurrogateKey')\
							,col('jd.postingSurrogateKey').alias('postingSurrogateKey')\
							,col('jd.businessTransactionSurrogateKey').alias('businessTransactionSurrogateKey')\
							,col('jd.referenceTransactionSurrogateKey').alias('referenceTransactionSurrogateKey')\
							,col('jd.JECriticalCommentSurrogateKey').alias('JECriticalCommentSurrogateKey')\
							,col('jd.lineStatisticsSurrogateKey').alias('lineStatisticsSurrogateKey')\
							,col('jd.footPrintSurrogateKey').alias('footPrintSurrogateKey')\
							,col('jd.jeAdditionalAttributeSurrogateKey').alias('jeAdditionalAttributeSurrogateKey')\
							,col('jd.quantityInSign').alias('quantityInSign')\
							,col('jd.amountLCInSign').alias('amountLCInSign')\
							,col('jd.amountDC').alias('amountDC')\
							,col('jd.amountLC').alias('amountLC')\
							,col('jd.amountRC').alias('amountRC')\
							,col('jd.quantity').alias('quantity')\
							,col('jd.customAmount').alias('customAmount')\
							,col('jd.partitionKeyForGLACube').alias('partitionKeyForGLACube')\
							,col('jd.amountCredit').alias('amountCredit')\
							,col('jd.amountDebit').alias('amountDebit')\
							,col('jd.documentNumberWithCompanyCode').alias('documentNumberWithCompanyCode')\
							,col('jd.lineItemCount').alias('lineItemCount')\
							,col('jd.documentStatus').alias('documentStatus')\
							,col('jd.amountLCFirstDigit').alias('amountLCFirstDigit')\
							,col('jd.amountDCFirstDigit').alias('amountDCFirstDigit')\
							,col('je.financialPeriod').alias('financialPeriod')\
                            ,col('jl.detailLinkID').alias('detailLinkID')\
                            ,col('je.documentLineItemID').alias('documentLineItemID')\
                            ,col('jl.customLinkID').alias('customLinkID'))
		
		dwh_vw_FACT_Journal_Raw = objDataTransformation.gen_convertToCDMStructure_generate \
			(dwh_vw_FACT_Journal_Raw,'dwh','vw_FACT_Journal_Raw',True)[0]
		
		#objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_Journal_Raw,gl_CDMLayer2Path + "fin_L2_FACT_Journal_Raw.parquet" )

		dwh_vw_FACT_Journal_Raw.write.mode("overwrite").partitionBy("financialPeriod")\
			.parquet(gl_CDMLayer2Path + "fin_L2_FACT_Journal_Raw.parquet")

		executionStatus = "dwh_vw_FACT_Journal_Raw populated sucessfully"
		executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
		return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
	
	except Exception as err:
		executionStatus = objGenHelper.gen_exceptionDetails_log()
		executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
		return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


