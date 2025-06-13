# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,desc,expr,concat,length,regexp_replace

def fin_SAP_L1_TD_Journal_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global fin_L1_TD_Journal

    erpSAPSystemID     = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))
    erpSystemIDGeneric = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC'))
    reportingLanguage  = str(knw_KPMGDataReportingLanguage.select('languageCode').distinct().collect()[0][0])

    WI_CT = when (col("erp_swwwihead.WI_CT").isNull(),lit(0))\
                  .otherwise(col("erp_swwwihead.WI_CT")).desc()

    dr_Rank= Window.partitionBy("erp_wi2obj.INSTID")\
                .orderBy(col("erp_swwwihead.WI_CD").desc(),lit(WI_CT),col("erp_swwwihead.WI_ID").desc())

    fin_SAP_L0_TMP_ApprovalInformation = erp_SWWWIHEAD.alias('erp_swwwihead')\
      .join (erp_SWW_WI2OBJ.alias('erp_wi2obj'),((col("erp_swwwihead.CLIENT") == col("erp_wi2obj.CLIENT"))\
                            & (col("erp_swwwihead.TOP_WI_ID") == col("erp_wi2obj.TOP_WI_ID") )),"inner")\
      .filter(lower(col("WI_TYPE")) == 'w')\
      .select(row_number().over(dr_Rank).alias("approvalDateRank"),\
             col("erp_swwwihead.CLIENT").alias("clientCode"),col("erp_wi2obj.INSTID").alias("instanceIdentity"),\
             col("erp_swwwihead.WI_AAGENT").alias("approvedBy"),col("erp_swwwihead.WI_CD").alias("approvalDate"),\
             col("erp_swwwihead.WI_CT").alias("approvalTime")).distinct()



    referenceSubledger = when (col('BKPF.AWTYP') == lit('RMRP'), lit('Purchase S/L')) \
      .when (col('BKPF.AWTYP') == lit('MKPF'), lit('Inventory S/L')) \
      .when (col('BKPF.AWTYP') == lit('PRCHG'), lit('Inventory S/L')) \
      .when (col('BKPF.AWTYP').isin(lit('AMDP'),lit('AMBU')), lit('Fixed Assets S/L')) \
      .when (col('BKPF.AWTYP') == lit('VBRK'), lit('Sales S/L')) \
      .when ((col('BKPF.AWTYP')== lit('HRPAY')) & (col('BKPF.AWTYP')== lit('HRP1')),lit('HR S/L (Payroll)')) \
      .otherwise( when((col("BKPF.GLVOR")== lit('RMRP')) & (col('BKPF.TCODE')== lit('MRKO')), lit('Purchase S/L (Consignment/Pipeline)'))\
                                    .otherwise( when(col('BKPF.GLVOR') == lit('RFT1'), lit('HR S/L (Travelexpenses)'))
                                               .otherwise (lit('Other'))\
                                              )
                                  )
    
    userType = when (upper(col('USR02.BNAME')).isin('WF-BATCH','SAP_WFRT'),lit('A'))\
                .otherwise(col('USR02.USTYP'))

    manualPostingAttribute2 = when (lit(userType).isin('B','C'),lit(0) )\
                              .otherwise(lit(1))

    manualPostingAttribute3 = when(col('BKPF.TCODE').isin('F110','F150','FN5V','FNM1','FNM1S','FNM3','FNV5'),lit(0))\
                              .otherwise(lit(1))

    financialPeriod  = when (col("BKPF.MONAT") ==  lit(0),lit(-1))\
                        .otherwise(col("BKPF.MONAT"))

    isManualPosting = when((lit(referenceSubledger) != lit('Other')) | (lit(manualPostingAttribute2) ==lit(0)) | (lit(manualPostingAttribute3) == lit(0)), lit(0))\
                      .otherwise(lit(1))

    referenceSubledgerDocumentNumber = concat(col('BKPF.AWKEY'),when (when (col("BSEG.EBELN").isNull(), lit(''))\
            .otherwise(col("BSEG.EBELN")) != '', concat(lit('-'),col('BSEG.EBELN')))\
            .otherwise(''))

    documentStatusDetail = when(col('BKPF.BSTAT').isNull(),lit('NONE'))\
      .otherwise(concat(col('BKPF.BSTAT'),lit(' - '),when (col("usf3.sourceSystemValueDescription").isNull(),lit('NONE'))\
                                                    .otherwise(col("usf3.sourceSystemValueDescription")))	) 


    isClearing = when(when (col('BSEG.AUGBL').isNull(),lit(''))\
                  .otherwise(col("BSEG.AUGBL")) != lit(''),lit(1))\
                    .otherwise(lit(0))

    new_col_XREVERSAL = when(col('BKPF.XREVERSAL').isNull(), lit(0) ) \
                                .when(col('BKPF.XREVERSAL')=='', lit(0) ) \
                                .otherwise(col('BKPF.XREVERSAL'))

    creationTime = concat(substring(col('BKPF.CPUTM'), 1, 2),lit(':')\
      ,substring(substring(col('BKPF.CPUTM'), 3, 4),1,2),lit(':'),substring(col('BKPF.CPUTM'),-2,2))
    approvalTimeValue  = concat(substring(col('appr0.approvalTime'), 1, 2)\
      ,lit(':'),substring(substring(col('appr0.approvalTime'), 3, 4),1,2),lit(':'),substring(col('appr0.approvalTime'),-2,2))
    approvalTime = when(((col('appr0.approvalTime').isNull())|(col('appr0.approvalTime') == lit(''))),lit('00:00:00')).otherwise(lit(approvalTimeValue))
    
   

    df_fin_L1_TD_Journal = erp_BKPF.alias('BKPF')\
        .join (knw_LK_CD_ReportingSetup.alias("lars"),((col("BKPF.MANDT")    == col("lars.clientCode"))\
                                                     & (col("BKPF.BUKRS")   == col("lars.companyCode") )), "inner")\
        .join (erp_BSEG.alias('BSEG'),((col("BKPF.MANDT")    == col("BSEG.MANDT"))\
                                      & (col("BKPF.BUKRS")   == col("BSEG.BUKRS"))\
                                      & (col("BKPF.GJAHR")   == col("BSEG.GJAHR"))\
                                      & (col("BKPF.BELNR")   == col("BSEG.BELNR"))), "inner")\
        .join (erp_USR02.alias("USR02"),((col("BKPF.USNAM")    == col("USR02.BNAME"))\
                                      & (col("BKPF.MANDT")   == col("USR02.MANDT") )), "left")\
        .join (fin_SAP_L0_STG_reversalDocument.alias('lsr'),((col("BKPF.MANDT")    == col("lsr.MANDT"))\
                                      & (col("BKPF.BUKRS")   == col("lsr.BUKRS"))\
                                      & (col("BKPF.GJAHR")   == col("lsr.GJAHR"))\
                                      & (col("BKPF.BELNR")   == col("lsr.BELNR"))\
                                      & (col("BKPF.STBLG")   == col("lsr.STBLG"))\
                                      & (col("BKPF.STJAH")   == col("lsr.STJAH"))\
                                      & (col("BKPF.BUDAT")   == col("lsr.postingDate"))\
                                      & (col("lsr.creationDate")   == col("BKPF.CPUDT"))), "left")\
        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('usfl'),((col("usfl.businessDatatype") == lit('Debit Credit Indicator')) \
                                      & (col("usfl.sourceSystemValue")   == col("BSEG.SHKZG"))\
                                      & (col("usfl.targetLanguageCode")   == col("lars.KPMGDataReportingLanguage"))\
                                      & (col("usfl.sourceERPSystemID") == lit(erpSAPSystemID))), "left")\
        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('usf3'),((col("usf3.businessDatatype") == lit('Document Status')) \
                                      & (col("usf3.sourceSystemValue")   == col("BKPF.BSTAT"))\
                                      & (col("usf3.targetLanguageCode")   == col("lars.KPMGDataReportingLanguage"))\
                                      & (col("usf3.sourceERPSystemID") == lit(erpSAPSystemID))\
                                      & (col("usf3.targetERPSystemID") == lit(erpSystemIDGeneric))), "left")\
        .join(fin_SAP_L0_TMP_ApprovalInformation.alias("appr0"),((col("appr0.clientCode") == col("BKPF.MANDT"))\
                                      & (col("appr0.instanceIdentity") == lit(concat(when(erp_BKPF.BUKRS.isNull(), lit('')).otherwise(erp_BKPF.BUKRS),\
                                                                             when(erp_BKPF.BELNR.isNull(), lit('')).otherwise(erp_BKPF.BELNR),\
                                                                             when(erp_BKPF.GJAHR.isNull(), lit('')).otherwise(erp_BKPF.GJAHR))))\
                                      & (col("appr0.approvalDateRank") == lit(1))), "left")\
        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt0'),((col("bdt0.businessDatatype") == lit('Posting Key')) \
                                      & (col("bdt0.sourceSystemValue")   == col("BSEG.BSCHL"))\
                                      & (col("bdt0.targetLanguageCode")   == lit(reportingLanguage))\
                                      & (col("bdt0.sourceERPSystemID") == lit(erpSAPSystemID))\
                                      & (col("bdt0.targetERPSystemID") == lit(erpSystemIDGeneric))), "left")\
        .select(col('BKPF.BUKRS').alias('companyCode'),col('BKPF.GJAHR').alias('fiscalYear'),lit(financialPeriod).alias("financialPeriod")\
          ,col("BKPF.BELNR").alias("documentNumber"),col("BSEG.BUZEI").alias("lineItem")\
          ,col("usfl.targetSystemValue").alias("debitCreditIndicator")\
          ,col("BSEG.HKONT").alias("accountNumber"),col("BSEG.DMBTR").alias("amountLC"),col("BSEG.WRBTR").alias("amountDC")\
          ,expr("cast(BSEG.MENGE as numeric(15,2)) as quantity"),col("lars.localCurrencyCode").alias("localCurrency")\
          ,col("BKPF.WAERS").alias("documentCurrency"),col("BKPF.BLART").alias("documentType"),col("BKPF.BUDAT").alias("postingDate")\
          ,col("BKPF.BLDAT").alias("documentDate"),col("BKPF.USNAM").alias("createdBy"),col("appr0.approvalDate").alias("approvalDate")\
          ,lit(creationTime).alias('creationTime'),when (lit(approvalTime).isNull(),lit('00:00:00'))\
                                                                            .otherwise(lit(approvalTime)).alias('approvalTime')\
          ,col("appr0.approvedBy").alias("approvedBy"),col("BKPF.CPUDT").alias("creationDate"),col("BKPF.BKTXT").alias("headerDescription")\
          ,col("BSEG.SGTXT").alias("lineDescription"),col("BKPF.TCODE").alias("transactionCode")\
          ,col("BKPF.STBLG").alias("reversalDocumentNumber")\
          ,regexp_replace(col('BKPF.XBLNR'), '\t', '').alias('referenceDocumentNumber')\
          ,lit(referenceSubledgerDocumentNumber).alias('referenceSubledgerDocumentNumber'),col('BKPF.AWSYS').alias('referenceSystem')\
          ,lit(referenceSubledger).alias('referenceSubledger'),lit(isManualPosting).alias('isManualPosting')\
          ,lit(referenceSubledger).alias('manualPostingAttribute1'),when (col("USR02.USTYP").isNull(),lit(''))\
                   .otherwise(col("USR02.USTYP")).alias('manualPostingAttribute2')\
          ,when (col("BKPF.TCODE").isNull(),lit(''))\
                 .otherwise(col("BKPF.TCODE")).alias('manualPostingAttribute3')\
          ,when (col("usf3.targetSystemValue").isNull(),col('BKPF.BSTAT'))\
                 .otherwise(col("usf3.targetSystemValue")).alias('documentStatus')\
          ,lit(documentStatusDetail).alias('documentStatusDetail'),col('BSEG.LIFNR').alias('vendorNumber')\
          ,col('BSEG.KUNNR').alias('customerNumber')\
          ,col('BSEG.MATNR').alias('productNumber'),col('BSEG.MATNR').alias('productArtificialID'),col('BSEG.AUGDT').alias('clearingDate')\
          ,col('BSEG.AUGBL').alias('clearingDocumentNumber'),lit(isClearing).alias('isClearing')\
          ,when(length(col('BSEG.VBUND')) > 0,lit(1))\
                    .otherwise(lit(0)).alias('isInterCoFlag')\
          ,col('BKPF.GLVOR').alias('businessTransactionCode'),col('BSEG.BSCHL').alias('postingKey')\
                ,col('BKPF.STJAH').alias('reversalFiscalYear')\
          ,when((col('BKPF.GRPID') == lit('')) | (col('BKPF.GRPID').isNull()),lit(0))\
                 .otherwise(lit(1)).alias('isBatchInput')\
         ,col('BKPF.AWTYP').alias('referenceTransaction'),when(col('lsr.reversalID').isNull(),lit(0))\
                 .otherwise(col('lsr.reversalID')).alias('reversalID')\
          ,when(col('lsr.reversalType').isNull(),lit(new_col_XREVERSAL))\
                 .otherwise(col('lsr.reversalType')).cast('int').alias('reversalType')\
          ,col('BSEG.KOART').alias('accountType'),col('BSEG.VBELN').alias('billingDocumentNumber')\
          ,col('BSEG.ZUONR').alias('assignmentNumber')\
          ,when (col('BKPF.XSTOV') == lit('x'),lit(1))\
                 .otherwise(lit(0)).alias('isReversal')\
          ,col('BSEG.UMSKZ').alias('GLSpecialIndicator'),col('BSEG.BUZID').alias('lineItemID'),col('BSEG.SKNTO').alias('cashDiscountAmount')\
          ,col('BSEG.KTOSL').alias('transactionKey')
          ,when (col('BSEG.XNEGP') == lit('X'),lit(1))\
             .otherwise(lit(0)).alias('isNegativePostingIndicator')\
          ,col('BKPF.WWERT').alias('exchangeDate'),col('BKPF.STGRD').alias('reversalReason')\
          ,when(col('bdt0.targetSystemValue').isNull(),'').otherwise(col('bdt0.targetSystemValue'))\
                                                                .alias('transactionType')\
          ,when(col('bdt0.targetSystemValueDescription').isNull(),'').otherwise(col('bdt0.targetSystemValueDescription'))\
                        .alias('transactionTypeDescription')\
          ,col("BSEG.MANDT").alias("clientCode")\
          ,col("BSEG.MWSTS").alias("taxAmountLocalCurrency")\
          ,col("BSEG.ZLSCH").alias("paymentMethod")\
          ,col("BSEG.ZTERM").alias("paymentTerm")\
          ,col("BSEG.ZFBDT").alias("calculationBaselineDate")\
          ,col("BSEG.ZBD1T").alias("cashDiscountDayForPayment1")\
          ,col("BSEG.ZBD2T").alias("cashDiscountDayForPayment2")\
          ,col("BSEG.ZBD3T").alias("dayForNetPayment")\
          ,col("BSEG.ZBD1P").alias("cashDiscountPercentageForPayment1")\
          ,col("BSEG.ZBD2P").alias("cashDiscountPercentageForPayment2")\
          ,col("BSEG.SKFBT").alias("eligibleDiscountAmountDocumentCurrency"))
    
    fin_L1_TD_Journal = df_fin_L1_TD_Journal
    fin_L1_TD_Journal = gen_journalExtendedKeys_prepare() ## under kpmg_rules_generic_methods.gen
    fin_L1_TD_Journal =  objDataTransformation.gen_convertToCDMandCache \
              (fin_L1_TD_Journal,'fin','L1_TD_Journal',targetPath=gl_CDMLayer1Path)
    
 
    executionStatus = "L1_TD_Journal populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
   
