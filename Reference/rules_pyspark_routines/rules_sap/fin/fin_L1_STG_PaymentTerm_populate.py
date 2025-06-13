# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import col

def fin_L1_STG_PaymentTerm_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()

        logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
        global  fin_L1_STG_PaymentTerm
        
        fin_L1_STG_PaymentTerm = fin_L1_MD_PaymentTerm.alias('pmt1')\
	    			.select(col('pmt1.paymentTermClientCode').alias('paymentTermClientCode')\
	    				,col('pmt1.paymentTerm').alias('paymentTerm')\
	    				,col('pmt1.paymentTermDayLimit').alias('paymentTermDayLimit')\
	    				,col('pmt1.paymentTermMinDayLimit').alias('paymentTermMinDayLimit')\
	    				,col('pmt1.paymentTermMaxDayLimit').alias('paymentTermMaxDayLimit')\
                        ,col('pmt1.paymentTermCashDiscountPercentageForPayment1')\
                            .alias('paymentTermCashDiscountPercentageForPayment1')\
	    	            ,col('pmt1.paymentTermCashDiscountDayForPayment1').alias('paymentTermCashDiscountDayForPayment1')\
	    	            ,col('pmt1.paymentTermCashDiscountPercentageForPayment2')\
                            .alias('paymentTermCashDiscountPercentageForPayment2')\
	    	            ,col('pmt1.paymentTermCashDiscountDayForPayment2').alias('paymentTermCashDiscountDayForPayment2')\
	    	            ,col('pmt1.paymentTermDayForNetPayment').alias('paymentTermDayForNetPayment')\
	    	            ,col('pmt1.isPaymentTermDayLimitDependent').alias('isPaymentTermDayLimitDependent')\
	    	            ,col('pmt1.isApplicableForCustomerGLAccount').alias('isApplicableForCustomerGLAccount')\
	    	            ,col('pmt1.isApplicableForVendorGLAccount').alias('isApplicableForVendorGLAccount'))\
	    				.distinct()

        fin_L1_STG_PaymentTerm =  objDataTransformation.gen_convertToCDMandCache \
               (fin_L1_STG_PaymentTerm,'fin','L1_STG_PaymentTerm',targetPath=gl_CDMLayer1Path)
        
        executionStatus = "L1_STG_PaymentTerm populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
