# Databricks notebook source
from pyspark.sql.functions import concat,coalesce,year,substring,dense_rank
from pyspark.sql.window import Window
import sys
import traceback

def fin_ORA_L1_TD_Journal_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    global fin_L1_TD_Journal
    global fin_L1_TD_Journal_referenceID
  
    windowSpec  = Window.orderBy(concat(col("LEDGER_ID"),lit("-"),col("balancingSegmentValue")),col("JE_HEADER_ID"))

    df_L1_TD_Journal_ReferenceId = erp_RA_CUSTOMER_TRX_ALL.alias("trx")\
        .join(erp_XLA_TRANSACTION_ENTITIES.alias("ent"),(col("trx.customer_trx_id")==col("ent.SOURCE_ID_INT_1")),how="inner")\
        .join(erp_XLA_AE_HEADERS.alias("head"),(col("ent.ENTITY_ID") == col("head.ENTITY_ID"))\
              & (col("trx.SET_OF_BOOKS_ID")==col("head.LEDGER_ID")),how="inner")\
        .join(erp_XLA_AE_LINES.alias("lines"),(col("head.AE_HEADER_ID") == col("lines.AE_HEADER_ID"))\
              & (col("lines.ACCOUNTING_CLASS_CODE")==lit("REVENUE")),how="inner")\
        .join(erp_GL_IMPORT_REFERENCES.alias("ref"),col("lines.GL_SL_LINK_ID") == col("ref.GL_SL_LINK_ID"),how="inner")\
        .join(erp_GL_JE_LINES.alias("glline"),(col("ref.JE_HEADER_ID") == col("glline.JE_HEADER_ID"))\
              & (col("ref.JE_LINE_NUM") == col("glline.JE_LINE_NUM")),how="inner")\
        .join(gen_ORA_L0_STG_ClientCCIDAccount.alias("ccid"),(col("lines.CODE_COMBINATION_ID")==col("ccid.codeCombinationId"))\
              & (col("ccid.accounttype")==lit("R")),how="inner")\
        .join(erp_RA_CUST_TRX_LINE_GL_DIST_ALL.alias("dist"),(col("dist.CUSTOMER_TRX_ID")==col("trx.CUSTOMER_TRX_ID"))\
              &(col("dist.EVENT_ID")==col("head.EVENT_ID"))\
              &(col("lines.CODE_COMBINATION_ID")==col("dist.CODE_COMBINATION_ID"))\
              &(col("dist.ACCOUNT_CLASS")==lit("REV")),how="inner")\
        .join(erp_RA_CUSTOMER_TRX_LINES_ALL.alias("lines_a"),(col("lines_a.CUSTOMER_TRX_ID")==col("trx.CUSTOMER_TRX_ID"))\
              &(col("lines_a.CUSTOMER_TRX_LINE_ID")==col("dist.CUSTOMER_TRX_LINE_ID"))\
              &(col("lines_a.CUSTOMER_TRX_ID")==col("dist.CUSTOMER_TRX_ID"))\
              &(col("lines_a.LINE_TYPE").isin(["LINE", "CB"])),how="inner")\
        .select(col("glline.JE_HEADER_ID").alias("documentNumber")\
              ,col("glline.JE_LINE_NUM").alias("lineItem")\
              ,concat(col("glline.LEDGER_ID"),lit("-"),col("ccid.balancingSegmentValue")).alias("CompanyCode")\
              ,col("trx.CUSTOMER_TRX_ID").alias("referenceSubledgerDocumentNumber")\
              ,col("dist.CUSTOMER_TRX_LINE_ID").alias("referenceSubledgerDocumentLineNumber")\
              ,col("glline.LEDGER_ID").alias("LEDGER_ID")\
              ,col("ccid.balancingSegmentValue").alias("balancingSegmentValue")\
              ,col("glline.JE_HEADER_ID").alias("JE_HEADER_ID")\
              ).withColumn("referenceID",dense_rank().over(windowSpec))

    df_L1_TD_Journal_ReferenceId = df_L1_TD_Journal_ReferenceId\
              .select(col("referenceID").alias("referenceID")\
              ,col("documentNumber").alias("documentNumber")\
              ,col("lineItem").alias("lineItem")\
              ,col("CompanyCode").alias("CompanyCode")\
              ,col("referenceSubledgerDocumentNumber").alias("referenceSubledgerDocumentNumber")\
              ,col("referenceSubledgerDocumentLineNumber").alias("referenceSubledgerDocumentLineNumber"))


    df_L1_TD_Journal_c_d = erp_GL_JE_LINES.alias("gjl")\
                      .join(erp_GL_JE_HEADERS.alias("gjh"),(col("gjh.JE_HEADER_ID") == col("gjl.JE_HEADER_ID"))\
                            & (col("gjh.ACTUAL_FLAG") == lit("A"))\
                            & (col("gjh.CURRENCY_CODE") != lit("STAT")),how="inner")\
                      .join(erp_GL_CODE_COMBINATIONS.alias("gcc"),col("gjl.CODE_COMBINATION_ID") == col("gcc.CODE_COMBINATION_ID"),how="inner")\
                      .join(gen_ORA_L0_STG_ClientCCIDAccount.alias("lsc"),col("gcc.CODE_COMBINATION_ID") == col("lsc.codeCombinationId"),how="inner")\
                      .join(fin_L0_STG_ClientBusinessStructure.alias("ocbs")\
                            ,col("ocbs.kaap_Company_Code") == concat(col("gjl.LEDGER_ID"),lit("-"),col("lsc.balancingSegmentValue")),how="inner")\
                      .join(erp_GL_LEDGERS.alias("gl"),col("gjl.LEDGER_ID")==col("gl.LEDGER_ID"),how="inner")\
                      .join(erp_GL_PERIODS.alias("gp"),(col("gp.PERIOD_NAME")==col("gjl.PERIOD_NAME"))\
                           &(col("gp.PERIOD_SET_NAME")==col("gl.PERIOD_SET_NAME"))\
                           &(col("gp.PERIOD_TYPE") == col("gl.ACCOUNTED_PERIOD_TYPE")),how="inner")\
                      .join(knw_LK_CD_ReportingSetup.alias("rep"),(col("rep.companyCode") == col("ocbs.kaap_Company_Code"))\
                           &(col("rep.balancingSegment")==col("lsc.balancingSegmentValue"))\
                           &(col("rep.ledgerID") == col("gl.LEDGER_ID")),how="inner")\
                      .join(fin_L1_MD_GLAccount.alias("glacc"),(col("lsc.naturalAccountNumber")==col("glacc.accountNumber"))\
                           &(col("glacc.companyCode") == concat(col("gjl.LEDGER_ID"),lit("-"),col("lsc.balancingSegmentValue"))),how="inner")\
                      .join(erp_GL_JE_HEADERS.alias("gjhrev"),col("gjh.REVERSED_JE_HEADER_ID") == col("gjhrev.JE_HEADER_ID"),how="left")\
                      .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("usf3"),(col("usf3.businessDatatype")==lit("Document Status"))\
                          &(col("usf3.sourceSystemValue") == col("gjh.STATUS"))\
                          &(col("usf3.targetLanguageCode")== col("rep.KPMGDataReportingLanguage"))\
                          &(col("usf3.sourceERPSystemID")==gl_ERPSystemID),how="left")\
                    .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("usf4"),(col("usf4.businessDatatype")==lit("Reference SubLedger"))\
                          &(col("usf4.sourceSystemValue") == col("gjh.JE_SOURCE"))\
                          &(col("usf4.targetLanguageCode")== col("rep.KPMGDataReportingLanguage"))\
                          &(col("usf4.sourceERPSystemID")==gl_ERPSystemID),how="left")\
                    .join(df_L1_TD_Journal_ReferenceId.alias("joulr"),(col("rep.companyCode")==col("joulr.companyCode"))\
                          & (col("gjh.JE_HEADER_ID")==col("joulr.documentNumber"))\
                          & (col("gjl.JE_LINE_NUM")==col("joulr.lineItem"))\
                          & ((when(col("usf4.targetSystemValue")=="Receivables S/L",lit("Sales S/L"))\
                        .when(col("usf4.targetSystemValue").isNotNull(),col("usf4.targetSystemValue"))\
                        .otherwise(lit("Other")))==lit("Sales S/L")),how="left")\
                    .select(col("rep.companyCode").alias("companyCode")\
                    ,col("gp.PERIOD_YEAR").alias("fiscalYear")\
                    ,col("gp.PERIOD_NUM").alias("financialPeriod")\
                    ,col("gjh.JE_HEADER_ID").alias("documentNumber")\
                    ,col("gjl.JE_LINE_NUM").alias("lineItem")\
                    ,col("lsc.naturalAccountNumber").alias("accountNumber")\
                    ,coalesce(col("gjl.ACCOUNTED_DR"),lit(0)).alias("amountLC_DR")\
                    ,coalesce(col("gjl.ENTERED_DR"),lit(0)).alias("amountDC_DR")\
                    ,coalesce(col("gjl.ACCOUNTED_CR"),lit(0)).alias("amountLC_CR")\
                    ,coalesce(col("gjl.ENTERED_CR"),lit(0)).alias("amountDC_CR")\
                    ,col("ocbs.ledger_Currency").alias("localCurrency")\
                    ,col("gjh.CURRENCY_CODE").alias("documentCurrency")\
                    ,col("gjh.JE_CATEGORY").alias("documentType")\
                    ,coalesce(col("gjl.EFFECTIVE_DATE"),col("gjh.DEFAULT_EFFECTIVE_DATE")).alias("postingDate")\
                    ,col("gjh.POSTED_DATE").alias("documentDate")\
                    ,coalesce(col("gjh.LAST_UPDATED_BY"),col("gjh.CREATED_BY")).alias("createdBy")\
                    ,col("gjh.CREATION_DATE").cast("date").alias("creationDate")\
                    ,col("gjh.DATE_CREATED").alias("creationTime")\
                    ,col("gjh.NAME").alias("headerDescription")\
                    ,col("gjl.DESCRIPTION").alias("lineDescription")\
                    ,col("gjh.JE_SOURCE").alias("transactionCode")\
                    ,substring(col("gjh.REVERSED_JE_HEADER_ID").cast("string"),1,20).alias("reversalDocumentNumber")\
                    ,substring(col("gjl.GL_SL_LINK_TABLE"),1,30).alias("referenceSystem")\
                    ,when(col("usf4.targetSystemValue")=="Receivables S/L",lit("Sales S/L"))\
                        .when(col("usf4.targetSystemValue").isNotNull(),col("usf4.targetSystemValue"))\
                        .otherwise(lit("Other")).alias("referenceSubledger")\
                    ,when(col("gjh.JE_SOURCE")== lit("Manual"),lit(1))\
                      .when(col("gjh.JE_SOURCE")== lit("AutoCopy"),lit(1))\
                      .when(col("gjh.JE_SOURCE")== lit("Other"),lit(1))\
                      .when(col("gjh.JE_SOURCE")== lit("Spreadsheet"),lit(1))\
                      .otherwise(lit(0)).alias("isManualPosting")\
                    ,col("gjh.JE_SOURCE").alias("manualPostingAttribute1")\
                    ,when(col("gjh.STATUS")=="P",lit("N")).otherwise(lit("P")).alias("documentStatus")\
                    ,when(col("gjh.STATUS").isNotNull(),concat(col("gjh.STATUS")\
                                                      ,lit(" - "),coalesce(col("usf3.sourceSystemValueDescription"),lit("NONE"))))\
                                                      .otherwise(lit("P")).alias("documentStatusDetail")\
                    ,year(col("gjhrev.POSTED_DATE")).alias("reversalFiscalYear")\
                    ,coalesce(col("gjh.ACCRUAL_REV_JE_HEADER_ID"),col("gjh.REVERSED_JE_HEADER_ID")).alias("reversalID")\
                    ,when(((col("gjh.ACCRUAL_REV_JE_HEADER_ID").isNotNull())&(col("gjh.REVERSED_JE_HEADER_ID")== 0)),lit(1))\
                      .when(((col("gjh.ACCRUAL_REV_JE_HEADER_ID").isNotNull())&(col("gjh.REVERSED_JE_HEADER_ID")!= 0)),lit(2))\
                      .otherwise(lit(0)).alias("reversalType")
                    ,col("glacc.accountType").alias("accountType")\
                    ,when(col("gjh.REVERSED_JE_HEADER_ID").isNotNull(),0).otherwise(lit(1)).alias("isReversal")\
                    ,col("joulr.referenceSubledgerDocumentNumber").alias("referenceSubledgerDocumentNumber"))

    df_L1_TD_Journal_d = df_L1_TD_Journal_c_d.alias("jou_d")\
                    .filter((col("jou_d.amountLC_DR")!=0)|((col("jou_d.amountLC_CR")==0)&(col("jou_d.amountLC_DR")==0)))\
                    .select(col("jou_d.companyCode").alias("companyCode")\
                    ,col("jou_d.fiscalYear").alias("fiscalYear")\
                    ,col("jou_d.financialPeriod").alias("financialPeriod")\
                    ,col("jou_d.documentNumber").alias("documentNumber")\
                    ,col("jou_d.lineItem").alias("lineItem")\
                    ,col("jou_d.accountNumber").alias("accountNumber")\
                    ,lit("D").alias("debitCreditIndicator")\
                    ,col("jou_d.amountLC_DR").alias("amountLC")\
                    ,col("jou_d.amountDC_DR").alias("amountDC")\
                    ,lit(None).alias("quantity")\
                    ,col("jou_d.localCurrency").alias("localCurrency")\
                    ,col("jou_d.documentCurrency").alias("documentCurrency")
                    ,col("jou_d.documentType").alias("documentType")\
                    ,col("jou_d.postingDate").alias("postingDate")\
                    ,col("jou_d.documentDate").alias("documentDate")\
                    ,col("jou_d.createdBy").alias("createdBy")\
                    ,lit(None).alias("approvalDate")\
                    ,lit(None).alias("approvedBy")\
                    ,col("jou_d.creationDate").alias("creationDate")\
                    ,col("jou_d.creationTime").alias("creationTime")\
                    ,col("jou_d.headerDescription").alias("headerDescription")\
                    ,col("jou_d.lineDescription").alias("lineDescription")\
                    ,col("jou_d.transactionCode").alias("transactionCode")\
                    ,col("jou_d.reversalDocumentNumber").alias("reversalDocumentNumber")\
                    ,lit(None).alias("referenceDocumentNumber")\
                    ,col("jou_d.referenceSubledgerDocumentNumber").alias("referenceSubledgerDocumentNumber")\
                    ,col("jou_d.referenceSystem").alias("referenceSystem")\
                    ,col("jou_d.referenceSubledger").alias("referenceSubledger")\
                    ,col("jou_d.isManualPosting").alias("isManualPosting")\
                    ,col("jou_d.manualPostingAttribute1").alias("manualPostingAttribute1")\
                    ,lit(None).alias("manualPostingAttribute2")\
                    ,lit(None).alias("manualPostingAttribute3")\
                    ,col("jou_d.documentStatus").alias("documentStatus")\
                    ,col("jou_d.documentStatusDetail").alias("documentStatusDetail")\
                    ,lit("#NA#").alias("vendorNumber")\
                    ,lit("#NA#").alias("customerNumber")\
                    ,lit("#NA#").alias("productNumber")\
                    ,lit("#NA#").alias("productArtificialId")\
                    ,lit(None).alias("clearingDate")\
                    ,lit(None).alias("clearingDocumentNumber")\
                    ,lit(None).alias("isClearing")\
                    ,lit(None).alias("isInterCoFlag")\
                    ,lit(None).alias("businessTransactionCode")\
                    ,lit(None).alias("postingKey")\
                    ,col("jou_d.reversalFiscalYear").alias("reversalFiscalYear")\
                    ,lit(None).alias("isBatchInput")\
                    ,lit(None).alias("referenceTransaction")\
                    ,col("jou_d.reversalID").alias("reversalID")\
                    ,col("jou_d.reversalType").alias("reversalType")\
                    ,col("jou_d.accountType").alias("accountType")\
                    ,lit(None).alias("billingDocumentNumber")\
                    ,lit(None).alias("assignmentNumber")\
                    ,col("jou_d.isReversal").alias("isReversal")\
                    ,lit(None).alias("GLSpecialIndicator")\
                    ,lit(None).alias("lineItemID")\
                    ,lit(None).alias("cashDiscountAmount")\
                    ,lit(None).alias("transactionKey")\
                    ,lit(None).alias("isNegativePostingIndicator")\
                    ,lit(None).alias("exchangeDate")\
                    ,lit(None).alias("reversalReason"))

    df_L1_TD_Journal_c = df_L1_TD_Journal_c_d.alias("jou_c")\
                    .filter((col("jou_c.amountLC_CR")!=0)|((col("jou_c.amountLC_CR")==0)&(col("jou_c.amountLC_DR")==0)))\
                    .select(col("jou_c.companyCode").alias("companyCode")\
                    ,col("jou_c.fiscalYear").alias("fiscalYear")\
                    ,col("jou_c.financialPeriod").alias("financialPeriod")\
                    ,col("jou_c.documentNumber").alias("documentNumber")\
                    ,col("jou_c.lineItem").alias("lineItem")\
                    ,col("jou_c.accountNumber").alias("accountNumber")\
                    ,lit("C").alias("debitCreditIndicator")\
                    ,col("jou_c.amountLC_CR").alias("amountLC")\
                    ,col("jou_c.amountDC_CR").alias("amountDC")\
                    ,lit(None).alias("quantity")\
                    ,col("jou_c.localCurrency").alias("localCurrency")\
                    ,col("jou_c.documentCurrency").alias("documentCurrency")
                    ,col("jou_c.documentType").alias("documentType")\
                    ,col("jou_c.postingDate").alias("postingDate")\
                    ,col("jou_c.documentDate").alias("documentDate")\
                    ,col("jou_c.createdBy").alias("createdBy")\
                    ,lit(None).alias("approvalDate")\
                    ,lit(None).alias("approvedBy")\
                    ,col("jou_c.creationDate").alias("creationDate")\
                    ,col("jou_c.creationTime").alias("creationTime")\
                    ,col("jou_c.headerDescription").alias("headerDescription")\
                    ,col("jou_c.lineDescription").alias("lineDescription")\
                    ,col("jou_c.transactionCode").alias("transactionCode")\
                    ,col("jou_c.reversalDocumentNumber").alias("reversalDocumentNumber")\
                    ,lit(None).alias("referenceDocumentNumber")\
                    ,col("jou_c.referenceSubledgerDocumentNumber").alias("referenceSubledgerDocumentNumber")\
                    ,col("jou_c.referenceSystem").alias("referenceSystem")\
                    ,col("jou_c.referenceSubledger").alias("referenceSubledger")\
                    ,col("jou_c.isManualPosting").alias("isManualPosting")\
                    ,col("jou_c.manualPostingAttribute1").alias("manualPostingAttribute1")\
                    ,lit(None).alias("manualPostingAttribute2")\
                    ,lit(None).alias("manualPostingAttribute3")\
                    ,col("jou_c.documentStatus").alias("documentStatus")\
                    ,col("jou_c.documentStatusDetail").alias("documentStatusDetail")\
                    ,lit("#NA#").alias("vendorNumber")\
                    ,lit("#NA#").alias("customerNumber")\
                    ,lit("#NA#").alias("productNumber")\
                    ,lit("#NA#").alias("productArtificialId")\
                    ,lit(None).alias("clearingDate")\
                    ,lit(None).alias("clearingDocumentNumber")\
                    ,lit(None).alias("isClearing")\
                    ,lit(None).alias("isInterCoFlag")\
                    ,lit(None).alias("businessTransactionCode")\
                    ,lit(None).alias("postingKey")\
                    ,col("jou_c.reversalFiscalYear").alias("reversalFiscalYear")\
                    ,lit(None).alias("isBatchInput")\
                    ,lit(None).alias("referenceTransaction")\
                    ,col("jou_c.reversalID").alias("reversalID")\
                    ,col("jou_c.reversalType").alias("reversalType")\
                    ,col("jou_c.accountType").alias("accountType")\
                    ,lit(None).alias("billingDocumentNumber")\
                    ,lit(None).alias("assignmentNumber")\
                    ,col("jou_c.isReversal").alias("isReversal")\
                    ,lit(None).alias("GLSpecialIndicator")\
                    ,lit(None).alias("lineItemID")\
                    ,lit(None).alias("cashDiscountAmount")\
                    ,lit(None).alias("transactionKey")\
                    ,lit(None).alias("isNegativePostingIndicator")\
                    ,lit(None).alias("exchangeDate")\
                    ,lit(None).alias("reversalReason"))

    fin_L1_TD_Journal = df_L1_TD_Journal_d.union(df_L1_TD_Journal_c)

    fin_L1_TD_Journal = gen_journalExtendedKeys_prepare() ## under kpmg_rules_generic_methods.gen
    fin_L1_TD_Journal =  objDataTransformation.gen_convertToCDMandCache \
          (fin_L1_TD_Journal,'fin','L1_TD_Journal',targetPath=gl_CDMLayer1Path) 
     
    
    fin_L1_TD_Journal_ReferenceId = objDataTransformation.gen_convertToCDMandCache \
        (df_L1_TD_Journal_ReferenceId,'fin','L1_TD_Journal_ReferenceId',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_TD_Journal populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


