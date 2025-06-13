# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,length,trim

def fin_SAP_L1_TD_ElectronicBankStatement_populate(): 
    try:
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

      global fin_L1_TD_ElectronicBankStatement

      trim_GJAHR = length(trim(col('febep_erp.GJAHR')))
      trim_fiscalYear = length(trim(col('stfin.fiscalYear')))

      length_value = length(col('febep_erp.BELNR'))

      fin_L1_TD_ElectronicBankStatement = erp_FEBKO.alias('febko_erp')\
          .join(knw_LK_CD_ReportingSetup.alias('lars'),((col('febko_erp.MANDT')==(col('lars.clientCode')))\
                                        &(col('febko_erp.BUKRS')==(col('lars.companyCode')))),how='inner')\
          .join(erp_FEBEP.alias('febep_erp'),((col('febko_erp.KUKEY')==(col('febep_erp.KUKEY')))\
                                        &(col('febko_erp.MANDT')==(col('febep_erp.MANDT')))),how='inner')\
          .join(gen_L1_MD_Period.alias('per'),((col('febep_erp.BUDAT')==(col('per.calendarDate')))
                                        &(col('febko_erp.BUKRS')==(col('per.companyCode')))),how='leftouter')\
          .join(gen_L1_MD_Period.alias('stfin'),((col('febko_erp.AZDAT')==(col('stfin.calendarDate')))\
                                        &(col('febko_erp.BUKRS')==(col('stfin.companyCode')))),how='leftouter')\
          .select(col('febko_erp.MANDT').alias('client')\
                 ,col('febko_erp.BUKRS').alias('companyCode')\
                 ,when(lit(trim_GJAHR)==4,trim(col('febep_erp.GJAHR')))\
                                         .otherwise(when(lit(trim_fiscalYear)==4,trim(col('stfin.fiscalYear')))\
                                                    .otherwise(col('per.fiscalYear'))).alias('fiscalYear')\
                 ,col('febko_erp.HKONT').alias('GLaccountNumber'),col('febko_erp.KUKEY').alias('bankFeedDocumentNumber')\
                 ,when(col('febep_erp.EPERL')=='X',lit(1)).otherwise(lit(0))\
                                            .alias('isBankFeedRecordCompleted')\
                 ,when((lit(length_value) < 10),when(col('febep_erp.ZUONR')!= lit(''),lit(substring(col('febep_erp.ZUONR'),1,40)))\
                     .otherwise(when(col('febep_erp.SGTXT')!= lit(''),lit(substring(col('febep_erp.SGTXT'),1,20)))\
                           .otherwise(col('febep_erp.BELNR'))))\
                           .otherwise(col('febep_erp.BELNR')).alias('GLDocumentNumber')\
                 ,col('febko_erp.AZDAT').alias('bankFeedDate'),col('stfin.fiscalYear').alias('bankFeedDateFiscalYear')\
                 ,col('febep_erp.BUDAT').alias('bankFeedPostingDate'),col('febep_erp.KWBTR').alias('amountDC')\
                 ,lit(0).alias('amountLC'),col('febep_erp.KWAER').alias('bankFeedDocumentCurrency')\
                 ,col('febep_erp.AVKOA').alias('paymentAdviceAccountType'),col('febep_erp.AVKON').alias('paymentAdviceBankAccountNumber')\
                 ,col('febep_erp.EPVOZ').alias('debitCreditIndicator'),col('febep_erp.ZUONR').alias('bankFeedAssignmentNumber')\
                 ,col('febep_erp.SGTXT').alias('bankFeedItemDescription'),col('febep_erp.BUTXT').alias('bankFeedPostingText')\
                 ,col('febep_erp.ESNUM').alias('bankFeedLineItem'),col('febep_erp.NBBLN').alias('bankFeedSubledgerDocNumber')\
                 ,col('febep_erp.XBLNR').alias('bankFeedReferenceDocNumber'),col('febep_erp.CHECT').alias('bankFeedCheckNumber')\
                 ,col('febep_erp.VGINT').alias('bankFeedPostingRule'),col('febep_erp.VORGC').alias('bankFeedBusinessTransactionCode')\
                 ,col('febko_erp.KTONR').alias('bankFeedLocalBankAccountNumber'),col('febko_erp.ABSND').alias('bankFeedSendingBankName')\
                 ,col('febep_erp.PARTN').alias('bankFeedBusinessPartner'),col('febep_erp.PAKTO').alias('bankFeedPartnerBankAccountNumber')\
                 ,col('febep_erp.PABLZ').alias('bankFeedPartnerBankCode'),col('febep_erp.PASWI').alias('bankFeedPartnerbankSwiftCode')\
                 ,col('febep_erp.PABKS').alias('bankFeedPartnerBankCountryKey'),col('febko_erp.ASTAT').alias('bankFeedStatementStatus')\
                 ,col('febko_erp.DSTAT').alias('bankFeedPrintStatus')\
                 ,when(col('febko_erp.VB1OK')=='X',lit(1)).otherwise(lit(0))\
                                               .alias('isBankFeedPostedToFIArea1')\
                 ,when(col('febep_erp.VB2OK')=='X',lit(1)).otherwise(lit(0))\
                                               .alias('isBankFeedPostedToFIArea2')\
                 ,when(col('febko_erp.KIPRE')=='X',lit(1)).otherwise(lit(0))\
                                               .alias('isBankFeedRecordInterpreted')\
                 ,col('febko_erp.SUMSO').alias('bankFeedHeaderDebitAmount'),col('febko_erp.SUMHA').alias('bankFeedHeaderCreditAmount')\
                 ,col('febko_erp.WAERS').alias('bankFeedHeaderCurrency')) \
          .filter(((col('febko_erp.AZDAT')==col('febep_erp.BUDAT'))\
                        |(col('febep_erp.GJAHR')==col('stfin.fiscalYear'))))\
     
     
      fin_L1_TD_ElectronicBankStatement =  objDataTransformation.gen_convertToCDMandCache \
         (fin_L1_TD_ElectronicBankStatement,'fin','L1_TD_ElectronicBankStatement',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "L1_TD_ElectronicBankStatement populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

#fin_SAP_L1_TD_ElectronicBankStatement_populate()       
        

