# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when

def ptp_SAP_L1_MD_Vendor_populate(): 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    erpSAPSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    reportingLanguage = knw_KPMGDataReportingLanguage.first()["languageCode"]
    
    global ptp_L1_MD_Vendor 
    
    df_vendor_erp_LFA1 =erp_LFA1.alias("lfa1")\
        .join (erp_LFB1.alias("lfb1"),(col("lfa1.MANDT")==col("lfb1.MANDT")) & (col("lfa1.LIFNR")==col("lfb1.LIFNR")), "left")\
        .join (erp_T008.alias("t008"),(col("lfb1.MANDT")==col("t008.MANDT")) & (col("lfb1.ZAHLS")==col("t008.ZAHLS")), "left")\
        .join (knw_LK_GD_BusinessDatatypeValueMapping.alias("bdt0"),(col("lfa1.LAND1")==col("bdt0.sourceSystemValue"))\
               & (col("bdt0.businessDatatype")== lit('Country Code'))\
               & (col("bdt0.targetLanguageCode")== lit(reportingLanguage))\
               & (col("bdt0.sourceERPSystemID")== lit(erpSAPSystemID))\
               & (col("bdt0.targetERPSystemID")== lit(erpGENSystemID)), "left")\
        .join (knw_LK_GD_BusinessDatatypeValueMapping.alias("bdt1"),(col("lfa1.KTOKK")==col("bdt1.sourceSystemValue"))\
               & (col("bdt1.businessDatatype")== lit('Account Group'))\
               & (col("bdt1.targetLanguageCode")== lit(reportingLanguage))\
               & (col("bdt1.sourceERPSystemID")== lit(erpSAPSystemID))\
               & (col("bdt1.targetERPSystemID")== lit(erpGENSystemID)), "left")\
        .join (knw_LK_GD_BusinessDatatypeValueMapping.alias("bdt2"),(col("lfa1.BRSCH")==col("bdt2.sourceSystemValue"))\
               & (col("bdt2.businessDatatype")== lit('Industry Sector'))\
               & (col("bdt2.targetLanguageCode")== lit(reportingLanguage))\
               & (col("bdt2.sourceERPSystemID")== lit(erpSAPSystemID))\
               & (col("bdt2.targetERPSystemID")== lit(erpGENSystemID)), "left")\
        .join (knw_LK_GD_BusinessDatatypeValueMapping.alias("bdt3"),(col("lfb1.VZSKZ")==col("bdt3.sourceSystemValue"))\
               & (col("bdt3.businessDatatype")== lit('Interest Calculation Indicator'))\
               & (col("bdt3.targetLanguageCode")== lit(reportingLanguage))\
               & (col("bdt3.sourceERPSystemID")== lit(erpSAPSystemID))\
               & (col("bdt3.targetERPSystemID")== lit(erpGENSystemID)), "left")\
        .join (knw_LK_GD_BusinessDatatypeValueMapping.alias("bdt4"),(col("lfb1.ZAHLS").eqNullSafe(col('bdt4.sourceSystemValue')))\
               & (col("bdt4.businessDatatype")== lit('Payment Blocking Reason'))\
               & (col("bdt4.targetLanguageCode")== lit(reportingLanguage))\
               & (col("bdt4.sourceERPSystemID")== lit(erpSAPSystemID))\
               & (col("bdt4.targetERPSystemID")== lit(erpGENSystemID)), "left")\
         .select(col("lfa1.MANDT").alias("vendorClientCode")\
         ,when(col("lfa1.LIFNR").isNull(),col("lfb1.LIFNR")).otherwise(col("lfa1.LIFNR")).alias("vendorNumber")\
         ,when(col("lfb1.ALTKN").isNull(),"").otherwise(col("lfb1.ALTKN")).alias("vendorPreviousNumber")\
         ,when(col("lfb1.ALTKN").isNull(),0).otherwise(length(col("lfb1.ALTKN"))).alias("vendorPreviousNumberLength")\
         ,when(col("lfb1.BUKRS").isNull(),"").otherwise(col("lfb1.BUKRS")).alias("vendorCompanyCode")\
         ,when(col("lfb1.ERDAT").isNull(),col("lfa1.ERDAT")).otherwise(col("lfb1.ERDAT")).alias("vendorCreationDate")\
         ,when(col("lfb1.ERNAM").isNull(),col("lfa1.ERNAM")).otherwise(col("lfb1.ERNAM")).alias("vendorCreationUser")\
         ,when((((col("lfa1.LOEVM")=="X") | (col("lfb1.LOEVM")=="X")) & (col("lfb1.NODEL")=='')) ,1)\
                 .otherwise(0).alias("isDeletedKPMG")\
         ,when(col("lfa1.LOEVM")=="X",1).otherwise(0).alias("isCentralDeleted")\
         ,when(col("lfb1.LOEVM")=="X",1).otherwise(0).alias("isDeleted")\
         ,when(col("lfb1.NODEL")=="X",1).otherwise(0).alias("isDeletionOfMasterRecordBlocked")\
         ,concat((when(col("lfa1.NAME1").isNull(),"").otherwise(col("lfa1.NAME1"))),lit(" "),\
                 (when(col("lfa1.NAME2").isNull(),"").otherwise(col("lfa1.NAME2")))).alias("vendorName")\
         ,when(col("bdt0.targetSystemValue").isNull(),col("lfa1.LAND1"))\
                 .otherwise(col("bdt0.targetSystemValue")).alias("vendorCountryCode")\
         ,when(col("bdt0.targetSystemValueDescription").isNull(),"")\
            .otherwise(col("bdt0.targetSystemValueDescription")).alias("vendorCountryName")\
         ,when(col("lfa1.ORT01").isNull(),"").otherwise(col("lfa1.ORT01")).alias("vendorCity")\
         ,when(col("lfa1.KONZS").isNull(),"").otherwise(col("lfa1.KONZS")).alias("vendorCompanyGroup")\
         ,when(length(col("lfa1.KONZS"))>0,concat(col("lfa1.KONZS"),lit(" ("),col("lfa1.MCOD1"),lit(")")))\
                 .otherwise(col("lfa1.KONZS")).alias("vendorCompanyGroupDescription")\
         ,when(col("bdt1.targetSystemValue").isNull(),col("lfa1.KTOKK")).\
                 otherwise(col("bdt1.targetSystemValue")).alias("vendorAccountGroup")\
         ,when(col("bdt1.targetSystemValueDescription").isNull(),"").otherwise(col("bdt1.targetSystemValueDescription"))\
                 .alias("vendorAccountGroupDescription")\
         ,when(col("bdt2.targetSystemValue").isNull(),col("lfa1.BRSCH")).otherwise(col("bdt2.targetSystemValue")).alias("vendorIndustryType")\
         ,when(col("bdt2.targetSystemValueDescription").isNull(),"").otherwise(col("bdt2.targetSystemValueDescription"))\
                 .alias("vendorIndustryTypeDescription")\
         ,when(col("lfa1.KUNNR").isNull(),"").otherwise(col("lfa1.KUNNR")).alias("vendorCustomerNumber")\
         ,when(col("lfb1.TOGRU").isNull(),"").otherwise(col("lfb1.TOGRU")).alias("paymentVerificationToleranceGroup")\
         ,when(col("lfb1.TOGRR").isNull(),"").otherwise(col("lfb1.TOGRR")).alias("vendorInvoiceVerificationToleranceGroup")\
         ,when(col("lfb1.LNRZB")!="",col("lfb1.LNRZB")).otherwise(col("lfa1.LNRZA")).alias("vendorAlternativePayeeNumberKPMG")\
         ,when(col("lfa1.LNRZA").isNull(),"").otherwise(col("lfa1.LNRZA")).alias("vendorCentralAlternativePayeeNumber")\
         ,when(col("lfb1.LNRZB").isNull(),"").otherwise(col("lfb1.LNRZB")).alias("vendorAlternativePayeeNumber")\
         ,when(col("lfa1.XZEMP")=="X",1).otherwise(0).alias("isAlternativePayeeAllowed")\
         ,when(col("lfb1.XLFZB")=="X",1).otherwise(0).alias("isAlernativePayeeAccountNumberActive")\
         ,when(col("lfa1.STCD1").isNull(),"").otherwise(col("lfa1.STCD1")).alias("vendorTaxNumber1")\
         ,when(col("lfa1.STCD2").isNull(),"").otherwise(col("lfa1.STCD2")).alias("vendorTaxNumber2")\
         ,when(col("lfa1.STCD3").isNull(),"").otherwise(col("lfa1.STCD3")).alias("vendorTaxNumber3")\
         ,when(col("lfa1.STCD4").isNull(),"").otherwise(col("lfa1.STCD4")).alias("vendorTaxNumber4")\
         ,when(col("lfa1.STKZU").isNull(),"").otherwise(col("lfa1.STKZU")).alias("vendorLiableForVAT")\
         ,when(col("lfa1.VBUND").isNull(),"").otherwise(col("lfa1.VBUND")).alias("vendorCompanyID")\
         ,when(col("lfa1.STCEG").isNull(),"").otherwise(col("lfa1.STCEG")).alias("vendorVATRegistrationNumber")\
         ,when(col("lfa1.SPERQ").isNull(),"").otherwise(col("lfa1.SPERQ")).alias("vendorFunctionThatWillBeBlocked")\
         ,when(col("lfa1.IPISP").isNull(),"").otherwise(col("lfa1.IPISP")).alias("vendorTaxSplit")\
         ,when(col("lfa1.TAXBS").isNull(),"").otherwise(col("lfa1.TAXBS")).alias("vendorTaxBaseInPercent")\
         ,when(col("bdt3.targetSystemValue").isNull(),when(col("lfb1.VZSKZ").isNull(),"").otherwise(col("lfb1.VZSKZ")))\
                 .otherwise(col("bdt3.targetSystemValue")).alias("vendorInterestCalculation")\
         ,when(col("bdt3.targetSystemValueDescription").isNull(),"").otherwise(col("bdt3.targetSystemValueDescription"))\
                 .alias("vendorInterestCalculationDescription")\
         ,when(col("lfb1.ZTERM").isNull(),"").otherwise(col("lfb1.ZTERM")).alias("vendorPaymentTerm")\
         ,when(((col("lfa1.SPERR")=="X") | (col("lfa1.SPERM")=="X") | (col("lfb1.SPERR")=="X")),1)\
                 .otherwise(0).alias("isPostingBlockedKPMG")\
         ,when(col("lfa1.SPERM")=="X",1).otherwise(0).alias("isCentralPurchasingBlockActive")\
         ,when(col("lfa1.SPERR")=="X",1).otherwise(0).alias("isCentralPostingBlockActive")\
         ,when(col("lfb1.SPERR")=="X",1).otherwise(0).alias("isPostingBlocked")\
         ,when(((col("lfa1.SPERZ")!="X") & (col("lfb1.ZAHLS")=="")),0).otherwise(1).alias("isBlockedForPaymentKPMG")\
         ,when(col("lfa1.SPERZ")=="X",1).otherwise(0).alias("isBlockedForPayment")\
         ,when(col("bdt4.targetSystemValue").isNull(),col("lfb1.ZAHLS")).otherwise(col("bdt4.targetSystemValue"))\
                 .alias("vendorPaymentBlockingCode")\
         ,when(col("t008.CHAR1")=="X",1).otherwise(0).alias("isChangeInPaymentProporsalPermitted")\
         ,when(col("t008.XOZSP")=="X",1).otherwise(0).alias("isBlockedForManualPayment")\
         ,when(col("t008.XNCHG")=="X",1).otherwise(0).alias("isPaymentBlockingChangeable")\
         ,when(col("bdt4.targetSystemValueDescription").isNull(),"").otherwise(col("bdt4.targetSystemValueDescription"))\
                 .alias("vendorPaymentBlockingReason")\
         ,when(col("lfa1.XCPDK")=="X",1).otherwise(0).alias("isOneTimeVendor")\
         ,when(col("lfb1.XVERR")=="X",1).otherwise(0).alias("isClearedBetweenCustomerAndVendor")\
         ,when(col("lfb1.AKONT").isNull(),"").otherwise(col("lfb1.AKONT")).alias("vendorAccountNumber"))
                 
    df_vendor_erp_LFA1_Final=df_vendor_erp_LFA1.select(col("vendorClientCode")\
          ,col("vendorNumber")\
          ,when(col("vendorPreviousNumberLength")>0,(concat(expr("repeat('0',10-vendorPreviousNumberLength)")\
               ,col("vendorPreviousNumber")))).otherwise("").alias("vendorPreviousNumber")\
          ,col("vendorCompanyCode")\
          ,col("vendorCreationDate")\
          ,col("vendorCreationUser")\
          ,col("isDeletedKPMG")\
          ,col("isCentralDeleted")\
          ,col("isDeleted")\
          ,col("isDeletionOfMasterRecordBlocked")\
          ,col("vendorName")\
          ,col("vendorCountryCode")\
          ,col("vendorCountryName")\
          ,col("vendorCity")\
          ,col("vendorCompanyGroup")\
          ,col("vendorAccountGroup")\
          ,col("vendorAccountGroupDescription")\
          ,col("vendorIndustryType")\
          ,col("vendorIndustryTypeDescription")\
          ,col("vendorCustomerNumber")\
          ,col("paymentVerificationToleranceGroup")\
          ,col("vendorInvoiceVerificationToleranceGroup")\
          ,col("vendorAlternativePayeeNumberKPMG")\
          ,col("vendorCentralAlternativePayeeNumber")\
          ,col("vendorAlternativePayeeNumber")\
          ,col("isAlternativePayeeAllowed")\
          ,col("isAlernativePayeeAccountNumberActive")\
          ,col("vendorTaxNumber1")\
          ,col("vendorTaxNumber2")\
          ,col("vendorTaxNumber3")\
          ,col("vendorTaxNumber4")\
          ,col("vendorLiableForVAT")\
          ,col("vendorCompanyID")\
          ,col("vendorVATRegistrationNumber")\
          ,col("vendorFunctionThatWillBeBlocked")\
          ,col("vendorTaxSplit")\
          ,col("vendorTaxBaseInPercent")\
          ,col("vendorInterestCalculation")\
          ,col("vendorInterestCalculationDescription")\
          ,col("vendorPaymentTerm")\
          ,col("isPostingBlockedKPMG")\
          ,col("isCentralPurchasingBlockActive")\
          ,col("isCentralPostingBlockActive")\
          ,col("isPostingBlocked")\
          ,col("isBlockedForPaymentKPMG")\
          ,col("isBlockedForPayment")\
          ,col("vendorPaymentBlockingCode")\
          ,col("isChangeInPaymentProporsalPermitted")\
          ,col("isBlockedForManualPayment")\
          ,col("isPaymentBlockingChangeable")\
          ,col("vendorPaymentBlockingReason")\
          ,col("isOneTimeVendor")\
          ,col("isClearedBetweenCustomerAndVendor")\
          ,lit(None).alias("vendorInterCompanyFromAccountMapping")\
          ,lit(None).alias("vendorInterCompanyFromMasterData")\
          ,lit(None).alias("vendorInterCompanyFromTransactionData")\
          ,lit(None).alias("vendorInterCompanyType")\
          ,lit(None).alias("phaseID")\
          ,lit(None).alias("vendorTaxNumber5")\
          ,col("vendorCompanyGroupDescription")\
          ,col("vendorAccountNumber")).distinct()
	
    df_vendor_erp_BSEG = erp_BSEG.alias("bseg")\
      .join(df_vendor_erp_LFA1_Final.alias("vendor"),(col("bseg.BUKRS")==col("vendor.vendorCompanyCode"))\
      & (col("bseg.LIFNR") == col("vendor.vendorNumber")),'leftanti')\
      .select(col("bseg.MANDT").alias("vendorClientCode")\
      ,col("bseg.LIFNR").alias("vendorNumber")).filter(col("bseg.LIFNR")!="").distinct()
    
    df_vendor_erp_BSEG_Final=df_vendor_erp_BSEG.select(col("vendorClientCode").alias("vendorClientCode")\
          ,col("vendorNumber").alias("vendorNumber")\
          ,lit('').alias("vendorPreviousNumber")\
          ,lit('').alias("vendorCompanyCode")\
          ,lit('19000101').alias("vendorCreationDate")\
          ,lit('').alias("vendorCreationUser")\
          ,lit(0).alias("isDeletedKPMG")\
          ,lit(0).alias("isCentralDeleted")\
          ,lit(0).alias("isDeleted")\
          ,lit(0).alias("isDeletionOfMasterRecordBlocked")\
          ,lit('').alias("vendorName")\
          ,lit('').alias("vendorCountryCode")\
          ,lit('').alias("vendorCountryName")\
          ,lit('').alias("vendorCity")\
          ,lit('').alias("vendorCompanyGroup")\
          ,lit('').alias("vendorAccountGroup")\
          ,lit('').alias("vendorAccountGroupDescription")\
          ,lit('').alias("vendorIndustryType")\
          ,lit('').alias("vendorIndustryTypeDescription")\
          ,lit('').alias("vendorCustomerNumber")\
          ,lit('').alias("paymentVerificationToleranceGroup")\
          ,lit('').alias("vendorInvoiceVerificationToleranceGroup")\
          ,lit('').alias("vendorAlternativePayeeNumberKPMG")\
          ,lit('').alias("vendorCentralAlternativePayeeNumber")\
          ,lit('').alias("vendorAlternativePayeeNumber")\
          ,lit(0).alias("isAlternativePayeeAllowed")\
          ,lit(0).alias("isAlernativePayeeAccountNumberActive")\
          ,lit('').alias("vendorTaxNumber1")\
          ,lit('').alias("vendorTaxNumber2")\
          ,lit('').alias("vendorTaxNumber3")\
          ,lit('').alias("vendorTaxNumber4")\
          ,lit(0).alias("vendorLiableForVAT")\
          ,lit('').alias("vendorCompanyID")\
          ,lit('').alias("vendorVATRegistrationNumber")\
          ,lit('').alias("vendorFunctionThatWillBeBlocked")\
          ,lit(0).alias("vendorTaxSplit")\
          ,lit(0).alias("vendorTaxBaseInPercent")\
          ,lit('').alias("vendorInterestCalculation")\
          ,lit('').alias("vendorInterestCalculationDescription")\
          ,lit('').alias("vendorPaymentTerm")\
          ,lit(0).alias("isPostingBlockedKPMG")\
          ,lit(0).alias("isCentralPurchasingBlockActive")\
          ,lit(0).alias("isCentralPostingBlockActive")\
          ,lit(0).alias("isPostingBlocked")\
          ,lit(0).alias("isBlockedForPaymentKPMG")\
          ,lit(0).alias("isBlockedForPayment")\
          ,lit(0).alias("vendorPaymentBlockingCode")\
          ,lit(0).alias("isChangeInPaymentProporsalPermitted")\
          ,lit(0).alias("isBlockedForManualPayment")\
          ,lit(0).alias("isPaymentBlockingChangeable")\
          ,lit('').alias("vendorPaymentBlockingReason")\
          ,lit(0).alias("isOneTimeVendor")\
          ,lit(0).alias("isClearedBetweenCustomerAndVendor")\
          ,lit(None).alias("vendorInterCompanyFromAccountMapping")\
          ,lit(None).alias("vendorInterCompanyFromMasterData")\
          ,lit(None).alias("vendorInterCompanyFromTransactionData")\
          ,lit(None).alias("vendorInterCompanyType")\
          ,lit(None).alias("phaseID")\
          ,lit(None).alias("vendorTaxNumber5")\
          ,lit('').alias("vendorCompanyGroupDescription")\
          ,lit('').alias("vendorAccountNumber"))
    
    ptp_L1_MD_Vendor=df_vendor_erp_LFA1_Final.union(df_vendor_erp_BSEG_Final)
    
    ptp_L1_MD_Vendor = objDataTransformation.gen_convertToCDMandCache \
      (ptp_L1_MD_Vendor,'ptp','L1_MD_Vendor',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_MD_Vendor populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
