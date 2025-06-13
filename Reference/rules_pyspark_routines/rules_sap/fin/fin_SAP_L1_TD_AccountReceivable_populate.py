# Databricks notebook source
from pyspark.sql.functions import coalesce,dayofmonth
import sys
import traceback

def fin_SAP_L1_TD_AccountReceivable_populate(): 
    try:
    
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
    
        logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    
        global fin_L1_TD_AccountReceivable

        erpSAPSystemID     = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))
        erpSystemIDGeneric = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC'))

        df_L1_TD_AccountReceivable_Closed_AR = erp_BSAD.alias("bsad_erp")\
            .join(knw_LK_CD_ReportingSetup.alias("rpt"),(col("bsad_erp.MANDT")==col("rpt.clientCode"))&\
            (col("bsad_erp.BUKRS")== col("rpt.companyCode")),how="left")\
            .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("usfl"),(col("usfl.businessDatatype")==lit("Debit Credit Indicator"))&\
            (col("usfl.sourceSystemValue")== col("bsad_erp.SHKZG"))&\
            (col("usfl.targetLanguageCode")==col("rpt.KPMGDataReportingLanguage"))&\
            (col("usfl.sourceERPSystemID")==lit(erpSAPSystemID)),how="left")\
            .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("usf3"),(col("usf3.businessDatatype")==lit("Document Status"))&\
            (col("usf3.sourceSystemValue")== col("bsad_erp.BSTAT"))&\
            (col("usf3.targetLanguageCode")==col("rpt.KPMGDataReportingLanguage"))&\
            (col("usf3.sourceERPSystemID")==lit(erpSAPSystemID))&\
            (col("usf3.targetERPSystemID")==lit(erpSystemIDGeneric)),how="left")\
            .join(fin_L1_MD_GLAccount.alias("glac1"),(col("bsad_erp.BUKRS")==col("glac1.companyCode"))&\
            (col("bsad_erp.HKONT")==col("glac1.accountNumber")),how="left")\
            .select(col("bsad_erp.MANDT").alias("accountReceivableClientCode")\
            ,col("bsad_erp.BUKRS").alias("companyCode")\
            ,col("bsad_erp.GJAHR").alias("financialYear")\
            ,col("bsad_erp.KUNNR").alias("customerNumber")\
            ,col("bsad_erp.BELNR").alias("GLJEDocumentNumber")\
            ,col("bsad_erp.BUZEI").alias("GLJELineNumber")\
            ,col("bsad_erp.BLDAT").alias("GLDocumentDate")\
            ,col("bsad_erp.HKONT").alias("accountNumber")\
            ,lit(None).alias("userName")\
            ,col("bsad_erp.XBLNR").alias("referenceDocumentNumber")\
            ,col("bsad_erp.CPUDT").alias("documentEntryDate")\
            ,when(col("bsad_erp.SHKZG") == lit('H'),col("bsad_erp.DMBTR") * -1).otherwise(col("bsad_erp.DMBTR")).alias("amountLC")\
            ,when(col("bsad_erp.SHKZG") == lit('H'),col("bsad_erp.WRBTR") * -1).otherwise(col("bsad_erp.WRBTR")).alias("amountDC")\
            ,col("bsad_erp.WAERS").alias("documentCurrency")\
            ,col("usfl.targetSystemValue").alias("debitCreditIndicator")\
            ,col("bsad_erp.AUGDT").alias("documentClearingDate")\
            ,col("bsad_erp.AUGBL").alias("clearingDocumentNumber")\
            ,col("bsad_erp.BUDAT").alias("GLJEPostingDate")\
            ,col("bsad_erp.SGTXT").alias("postingDescription")\
            ,col("usf3.targetSystemValue").alias("documentStatus")\
            ,when(col("bsad_erp.XSTOV") == lit('X') , 1).otherwise(0).alias("isCancelledFlag")
            ,col("bsad_erp.ZUONR").alias("allocationNumber")\
            ,col("bsad_erp.SKNTO").alias("cashDiscountLocalCurrency")\
            ,col("bsad_erp.MWSTS").alias("taxAmountLocalCurrency")\
            ,col("bsad_erp.UMSKZ").alias("GLSpecialIndicator")\
            ,col("bsad_erp.VBELN").alias("billingDocumentNumber")\
            ,col("bsad_erp.BLART").alias("documentType")\
            ,col("bsad_erp.BSCHL").alias("postingKey")\
            ,col("bsad_erp.ZLSCH").alias("paymentMethod")\
            ,col("bsad_erp.ZTERM").alias("accountReceivablePaymentTerm")\
            ,lit(0).alias("accountReceivablePaymentTermDayLimit")\
            ,col("bsad_erp.ZFBDT").alias("accountReceivableCalculationBaselineDate")\
            ,col("bsad_erp.ZBD1T").alias("accountReceivableCashDiscountDayForPayment1")\
            ,col("bsad_erp.ZBD2T").alias("accountReceivableCashDiscountDayForPayment2")\
            ,col("bsad_erp.ZBD3T").alias("accountReceivableDayForNetPayment")\
            ,col("bsad_erp.ZBD1P").alias("accountReceivableCashDiscountPercentageForPayment1")\
            ,col("bsad_erp.ZBD2P").alias("accountReceivableCashDiscountPercentageForPayment2")\
            ,col("bsad_erp.DMBTR").alias("amountLocalCurrency")\
            ,col("bsad_erp.WRBTR").alias("amountDocumentCurrency")\
            ,col("bsad_erp.SKFBT").alias("eligibleDiscountAmountDocumentCurrency")\
            ,lit('Closed AR').alias("dataOrigin")\
            ,lit('D').alias("accountType")\
            ,col("bsad_erp.MONAT").alias("financialPeriod")\
            ,col("bsad_erp.SGTXT").alias("accountReceivableLineitemDescription"))

        df_L1_TD_AccountReceivable_Open_AR = erp_BSID.alias("bsid_erp")\
            .join(knw_LK_CD_ReportingSetup.alias("rpt"),(col("bsid_erp.MANDT")== col("rpt.clientCode"))&\
            (col("bsid_erp.BUKRS")== col("rpt.companyCode")),how="left")\
            .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("usfl"),\
            (col("usfl.businessDatatype")==lit("Debit Credit Indicator"))&\
            (col("usfl.sourceSystemValue")==col("bsid_erp.SHKZG"))&\
            (col("usfl.targetLanguageCode")==col("rpt.KPMGDataReportingLanguage"))&\
            (col("usfl.sourceERPSystemID")== lit(erpSAPSystemID)),how="left")\
            .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("usf3"),\
            (col("usf3.businessDatatype")==lit("Document Status"))&\
            (col("usf3.sourceSystemValue")== col("bsid_erp.BSTAT"))&\
            (col("usf3.targetLanguageCode")== col("rpt.KPMGDataReportingLanguage"))&\
            (col("usf3.sourceERPSystemID")== lit(erpSAPSystemID))&\
            (col("usf3.targetERPSystemID")== lit(erpSystemIDGeneric)),how="left")\
            .join(fin_L1_MD_GLAccount.alias("glac1"),\
            (col("bsid_erp.BUKRS")== col("glac1.companyCode"))&\
            (col("bsid_erp.HKONT")== col("glac1.accountNumber")),how="left")\
            .join(df_L1_TD_AccountReceivable_Closed_AR.alias("acre"),\
            (col("bsid_erp.BUKRS")== col("acre.companyCode"))&\
            (col("bsid_erp.GJAHR")==col("acre.financialYear"))&\
            (col("bsid_erp.BELNR")== col("acre.GLJEDocumentNumber"))&\
            (col("bsid_erp.BUZEI")== col("acre.GLJELineNumber")),how="leftanti")\
            .select(col("bsid_erp.MANDT").alias("accountReceivableClientCode")\
            ,col("bsid_erp.BUKRS").alias("companyCode")\
            ,col("bsid_erp.GJAHR").alias("financialYear")\
            ,col("bsid_erp.KUNNR").alias("customerNumber")\
            ,col("bsid_erp.BELNR").alias("GLDocumentNumber")\
            ,col("bsid_erp.BUZEI").alias("GLLineNumber")\
            ,col("bsid_erp.BLDAT").alias("GLDocumentDate")\
            ,col("bsid_erp.HKONT").alias("accountNumber")\
            ,lit(None).alias("userName")\
            ,col("bsid_erp.XBLNR").alias("referenceDocumentNumber")\
            ,col("bsid_erp.CPUDT").alias("documentEntryDate")\
            ,when(col("bsid_erp.SHKZG") == lit("H"),col("bsid_erp.DMBTR") * -1).otherwise(col("bsid_erp.DMBTR")).alias("amountLC")\
            ,when(col("bsid_erp.SHKZG") == lit("H"),col("bsid_erp.WRBTR") * -1).otherwise(col("bsid_erp.WRBTR")).alias("amountDC")\
            ,col("bsid_erp.WAERS").alias("documentCurrency")\
            ,col("usfl.targetSystemValue").alias("debitCreditIndicator")\
            ,col("bsid_erp.AUGDT").alias("documentClearingDate")\
            ,col("bsid_erp.AUGBL").alias("clearingDocumentNumber")\
            ,col("bsid_erp.BUDAT").alias("GLJEPostingDate")\
            ,col("bsid_erp.SGTXT").alias("postingDescription")\
            ,col("usf3.targetSystemValue").alias("documentStatus")\
            ,when(col("bsid_erp.XSTOV")== lit("X"),1).otherwise(0).alias("isCancelledFlag")\
            ,col("bsid_erp.ZUONR").alias("allocationNumber")\
            ,col("bsid_erp.SKNTO").alias("cashDiscountLocalCurrency")\
            ,col("bsid_erp.MWSTS").alias("taxAmountLocalCurrency")\
            ,col("bsid_erp.UMSKZ").alias("GLSpecialIndicator")\
            ,col("bsid_erp.VBELN").alias("billingDocumentNumber")\
            ,col("bsid_erp.BLART").alias("documentType")\
            ,col("bsid_erp.BSCHL").alias("postingKey")\
            ,col("bsid_erp.ZLSCH").alias("paymentMethod")\
            ,col("bsid_erp.ZTERM").alias("accountReceivablePaymentTerm")\
            ,lit(0).alias("accountReceivablePaymentTermDayLimit")\
            ,col("bsid_erp.ZFBDT").alias("accountReceivableCalculationBaselineDate")\
            ,col("bsid_erp.ZBD1T").alias("accountReceivableCashDiscountDayForPayment1")\
            ,col("bsid_erp.ZBD2T").alias("accountReceivableCashDiscountDayForPayment2")\
            ,col("bsid_erp.ZBD3T").alias("accountReceivableDayForNetPayment")\
            ,col("bsid_erp.ZBD1P").alias("accountReceivableCashDiscountPercentageForPayment1")\
            ,col("bsid_erp.ZBD2P").alias("accountReceivableCashDiscountPercentageForPayment2")\
            ,col("bsid_erp.DMBTR").alias("amountLocalCurrency")\
            ,col("bsid_erp.WRBTR").alias("amountDocumentCurrency")\
            ,col("bsid_erp.SKFBT").alias("eligibleDiscountAmountDocumentCurrency")\
            ,lit("Open AR").alias("dataOrigin")\
            ,lit("D").alias("accountType")\
            ,col("bsid_erp.MONAT").alias("financialPeriod")\
            ,col("bsid_erp.SGTXT").alias("accountReceivableLineitemDescription"))

        df_L1_TD_AccountReceivable = df_L1_TD_AccountReceivable_Closed_AR.union(df_L1_TD_AccountReceivable_Open_AR)

        df_L1_TD_AccountReceivable = df_L1_TD_AccountReceivable.withColumn("accountReceivableCalculationBaselineDate_day"\
            ,dayofmonth("accountReceivableCalculationBaselineDate"))

        df_L1_TMP_AccountReceivablePaymentTermDayLimit = df_L1_TD_AccountReceivable.alias("arv1")\
            .join(fin_L1_STG_PaymentTerm.alias("spt1"),(col("spt1.paymentTermClientCode")== col("arv1.accountReceivableClientCode"))&\
            (col("spt1.paymentTerm")== col("arv1.accountReceivablePaymentTerm")),how="inner")\
            .filter(col("spt1.isPaymentTermDayLimitDependent") == 1)\
            .select(col("arv1.accountReceivableClientCode").alias("accountReceivableClientCode")\
            ,col("arv1.GLJEDocumentNumber").alias("accountReceivableDocumentNumber")\
            ,col("arv1.GLJELineNumber").alias("accountReceivableDocumentLineItem")\
            ,col("arv1.financialYear").alias("accountReceivableFiscalYear")\
            ,when((col("arv1.accountReceivableCalculationBaselineDate_day")<=col("spt1.paymentTermMinDayLimit"))&\
            (col("spt1.paymentTermMaxDayLimit") > col("arv1.accountReceivableCalculationBaselineDate_day")),col("spt1.paymentTermMinDayLimit"))\
            .when((col("arv1.accountReceivableCalculationBaselineDate_day") >=col("spt1.paymentTermMinDayLimit"))&\
            (col("arv1.accountReceivableCalculationBaselineDate_day") <=col("spt1.paymentTermMaxDayLimit")),col("spt1.paymentTermMaxDayLimit"))\
            .otherwise(0).alias("accountReceivablePaymentTermDayLimit")).distinct()

        fin_L1_TD_AccountReceivable = df_L1_TD_AccountReceivable.alias("arv1").join(df_L1_TMP_AccountReceivablePaymentTermDayLimit.alias("rdl1"),\
            (col("rdl1.accountReceivableClientCode")== col("arv1.accountReceivableClientCode"))&\
            (col("rdl1.accountReceivableDocumentNumber")== col("arv1.GLJEDocumentNumber"))&\
            (col("rdl1.accountReceivableDocumentLineItem") == col("arv1.GLJELineNumber"))&\
            (col("rdl1.accountReceivableFiscalYear")== col("arv1.financialYear")),how="left")\
            .select(col("arv1.accountReceivableClientCode").alias("accountReceivableClientCode")\
            ,col("arv1.companyCode").alias("companyCode")\
            ,col("arv1.financialYear").alias("financialYear")\
            ,col("arv1.customerNumber").alias("customerNumber")\
            ,col("arv1.GLJEDocumentNumber").alias("GLJEDocumentNumber")\
            ,col("arv1.GLJELineNumber").alias("GLJELineNumber")\
            ,col("arv1.GLDocumentDate").alias("GLDocumentDate")\
            ,col("arv1.accountNumber").alias("accountNumber")\
            ,col("arv1.userName").alias("userName")\
            ,col("arv1.referenceDocumentNumber").alias("referenceDocumentNumber")\
            ,col("arv1.documentEntryDate").alias("documentEntryDate")\
            ,col("arv1.amountLC").alias("amountLC")\
            ,col("arv1.amountDC").alias("amountDC")\
            ,col("arv1.documentCurrency").alias("documentCurrency")\
            ,col("arv1.debitCreditIndicator").alias("debitCreditIndicator")\
            ,col("arv1.documentClearingDate").alias("documentClearingDate")\
            ,col("arv1.clearingDocumentNumber").alias("clearingDocumentNumber")\
            ,col("arv1.GLJEPostingDate").alias("GLJEPostingDate")\
            ,col("arv1.postingDescription").alias("postingDescription")\
            ,col("arv1.documentStatus").alias("documentStatus")\
            ,col("arv1.isCancelledFlag").alias("isCancelledFlag")\
            ,col("arv1.allocationNumber").alias("allocationNumber")\
            ,col("arv1.cashDiscountLocalCurrency").alias("cashDiscountLocalCurrency")\
            ,col("arv1.taxAmountLocalCurrency").alias("taxAmountLocalCurrency")\
            ,col("arv1.GLSpecialIndicator").alias("GLSpecialIndicator")\
            ,col("arv1.billingDocumentNumber").alias("billingDocumentNumber")\
            ,col("arv1.documentType").alias("documentType")\
            ,col("arv1.postingKey").alias("postingKey")\
            ,col("arv1.paymentMethod").alias("paymentMethod")\
            ,col("arv1.accountReceivablePaymentTerm").alias("accountReceivablePaymentTerm")\
            ,coalesce(col("rdl1.accountReceivablePaymentTermDayLimit"),col("arv1.accountReceivablePaymentTermDayLimit")).alias("accountReceivablePaymentTermDayLimit")\
            ,col("arv1.accountReceivableCalculationBaselineDate").alias("accountReceivableCalculationBaselineDate")\
            ,col("arv1.accountReceivableCashDiscountDayForPayment1").alias("accountReceivableCashDiscountDayForPayment1")\
            ,col("arv1.accountReceivableCashDiscountDayForPayment2").alias("accountReceivableCashDiscountDayForPayment2")\
            ,col("arv1.accountReceivableDayForNetPayment").alias("accountReceivableDayForNetPayment")\
            ,col("arv1.accountReceivableCashDiscountPercentageForPayment1").alias("accountReceivableCashDiscountPercentageForPayment1")\
            ,col("arv1.accountReceivableCashDiscountPercentageForPayment2").alias("accountReceivableCashDiscountPercentageForPayment2")\
            ,col("arv1.amountLocalCurrency").alias("amountLocalCurrency")\
            ,col("arv1.amountDocumentCurrency").alias("amountDocumentCurrency")\
            ,col("arv1.eligibleDiscountAmountDocumentCurrency").alias("eligibleDiscountAmountDocumentCurrency")\
            ,col("arv1.dataOrigin").alias("dataOrigin")\
            ,col("arv1.accountType").alias("accountType")\
            ,col("arv1.financialPeriod").alias("financialPeriod")\
            ,col("arv1.accountReceivableLineitemDescription").alias("accountReceivableLineitemDescription"))
        
        fin_L1_TD_AccountReceivable =  objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_TD_AccountReceivable,'fin','L1_TD_AccountReceivable',targetPath=gl_CDMLayer1Path)
    
        executionStatus = "L1_TD_AccountReceivable populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
