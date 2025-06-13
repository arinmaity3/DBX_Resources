# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when


def fin_SAP_L1_MD_PaymentTerm_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    
    global fin_L1_MD_PaymentTerm
    fin_SAP_L0_TMP_PaymentTermDescription = erp_T052U.alias('t052u')\
                                           .join(knw_LK_ISOLanguageKey.alias('lngkey'),\
                                                        ((col('lngkey.sourceSystemValue'))==(col('t052u.SPRAS'))),how="inner")\
                                           .join(knw_clientDataReportingLanguage.alias('rlg0')\
                                                   ,((col('rlg0.clientCode')==(col('t052u.MANDT')))\
                                                    &(col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
                                           .select(col('t052u.MANDT').alias('paymentTermClientCode'),col('rlg0.languageCode').alias('paymentTermLanguageCode')\
                                                   ,col('t052u.ZTERM').alias('paymentTerm'),col('t052u.ZTAGG').cast('int').alias('paymentTermDayLimit')\
                                                   ,col('t052u.TEXT1').alias('paymentTermDescription'))\
                                           .union(\
                                                 erp_T052U.alias('t052u')\
                                           .join(knw_LK_ISOLanguageKey.alias('lngkey'),\
                                                        ((col('lngkey.sourceSystemValue'))==(col('t052u.SPRAS'))),how="inner")\
                                           .join(knw_clientDataSystemReportingLanguage.alias('slg0')\
                                                   ,((col('slg0.clientCode')==(col('t052u.MANDT')))\
                                                    &(col('slg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
                                           .select(col('t052u.MANDT').alias('paymentTermClientCode'),coalesce(col('slg0.languageCode'),lit('NA')).alias('paymentTermLanguageCode')\
                                                   ,col('t052u.ZTERM').alias('paymentTerm'),col('t052u.ZTAGG').cast('int').alias('paymentTermDayLimit')\
                                                   ,col('t052u.TEXT1').alias('paymentTermDescription'))\
                                                 ).distinct()

    fin_SAP_L0_TMP_PaymentTermText = erp_TVZBT.alias('tvzbt')\
                                           .join(knw_LK_ISOLanguageKey.alias('lngkey'),\
                                                        ((col('lngkey.sourceSystemValue'))==(col('tvzbt.SPRAS'))),how="inner")\
                                           .join(knw_clientDataReportingLanguage.alias('rlg0')\
                                                   ,((col('rlg0.clientCode')==(col('tvzbt.MANDT')))\
                                                    &(col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
                                           .select(col('tvzbt.MANDT').alias('paymentTermClientCode'),col('rlg0.languageCode').alias('paymentTermLanguageCode')\
                                                   ,col('tvzbt.ZTERM').alias('paymentTerm'),col('tvzbt.VTEXT').alias('paymentTermText'))\
                                           .union(\
                                                 erp_TVZBT.alias('tvzbt')\
                                           .join(knw_LK_ISOLanguageKey.alias('lngkey'),\
                                                        ((col('lngkey.sourceSystemValue'))==(col('tvzbt.SPRAS'))),how="inner")\
                                           .join(knw_clientDataSystemReportingLanguage.alias('slg0')\
                                                   ,((col('slg0.clientCode')==(col('tvzbt.MANDT')))\
                                                    &(col('slg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
                                           .select(col('tvzbt.MANDT').alias('paymentTermClientCode'),coalesce(col('slg0.languageCode'),lit('NA')).alias('paymentTermLanguageCode')\
                                                   ,col('tvzbt.ZTERM').alias('paymentTerm'),col('tvzbt.VTEXT').alias('paymentTermText'))\
                                                 ).distinct()

    fin_SAP_L0_TMP_PaymentTerm1 = erp_T052.alias('t052')\
                                  .filter(col('t052.ZTAGG') != 0)\
                                  .select(col('t052.MANDT').alias('paymentTermClientCode'),col('t052.ZTERM').alias('paymentTerm'),col('t052.ZTAGG').alias('paymentTermDayLimit')\
                                         ,lit(0).alias('paymentTermMinDayLimit'),lit(0).alias('paymentTermMaxDayLimit'))

    fin_SAP_L0_TMP_PaymentTerm2 = erp_T052.alias('t052')\
                                  .join(fin_SAP_L0_TMP_PaymentTerm1.alias('pmt0')\
                                               ,((col('pmt0.paymentTermClientCode') == col('t052.MANDT'))\
                                               &(col('pmt0.paymentTerm')==(col('t052.ZTERM')))) ,how='leftanti')\
                                  .filter(col('t052.ZTAGG') == 0)\
                                  .select(col('t052.MANDT').alias('paymentTermClientCode'),col('t052.ZTERM').alias('paymentTerm'),col('t052.ZTAGG').alias('paymentTermDayLimit')\
                                         ,lit(0).alias('paymentTermMinDayLimit'),lit(0).alias('paymentTermMaxDayLimit'))
    
    clientLanguageCode_get = knw_LK_CD_ReportingSetup\
                        .select(col('clientCode'),col('clientDataReportingLanguage').alias('languageCode'))\
                        .union(\
                         knw_LK_CD_ReportingSetup.select(col('clientCode'),col('clientDataSystemReportingLanguage').alias('languageCode'))\
                              ).distinct()
 

    fin_SAP_L0_TMP_PaymentTerm = fin_SAP_L0_TMP_PaymentTerm1.union(fin_SAP_L0_TMP_PaymentTerm2)

    paymentTermDayLimitCTE = fin_SAP_L0_TMP_PaymentTerm\
                          .groupBy("paymentTermClientCode","paymentTerm")\
                                               .agg(min("paymentTermDayLimit").alias("MinDayLimit"),\
                                                    max("paymentTermDayLimit").alias("MaxDayLimit"))


    paymentTermMinDayLimit = when((col('ptl0.paymentTermClientCode').isNotNull()) & (col('ptl0.paymentTerm').isNotNull()),col('ptl0.MinDayLimit'))\
                    .otherwise(col('pmt0.paymentTermMinDayLimit'))

    paymentTermMaxDayLimit = when((col('ptl0.paymentTermClientCode').isNotNull()) & (col('ptl0.paymentTerm').isNotNull()),col('ptl0.MaxDayLimit'))\
                    .otherwise(col('pmt0.paymentTermMaxDayLimit'))


    fin_SAP_L0_TMP_PaymentTerm = fin_SAP_L0_TMP_PaymentTerm.alias('pmt0')\
                                        .join(paymentTermDayLimitCTE.alias('ptl0'),\
                                                  (col('pmt0.paymentTermClientCode').eqNullSafe(col('ptl0.paymentTermClientCode'))\
                                                  &(col('pmt0.paymentTerm')==(col('ptl0.paymentTerm')))),how='left')\
                                      .select(col('pmt0.paymentTermClientCode'),col('pmt0.paymentTerm'),col('pmt0.paymentTermDayLimit'),\
                                              lit(paymentTermMinDayLimit).alias('paymentTermMinDayLimit'),lit(paymentTermMaxDayLimit).alias('paymentTermMaxDayLimit'))

    fin_L1_MD_PaymentTerm = fin_SAP_L0_TMP_PaymentTerm.alias('ptm0')\
                            .join(erp_T052.alias('t052'),\
                                  ((col('t052.MANDT') == col('ptm0.paymentTermClientCode'))\
                                 &(col('t052.ZTERM') == col('ptm0.paymentTerm'))\
                                 &(col('t052.ZTAGG') == col('ptm0.paymentTermDayLimit'))),how='inner')\
                            .join(clientLanguageCode_get.alias('rlg0'),\
                                    ((col('t052.MANDT') == col('rlg0.clientCode'))),how='inner')\
                            .join(fin_SAP_L0_TMP_PaymentTermDescription.alias('ptd0'),\
                                  ((col('ptd0.paymentTermClientCode') == col('ptm0.paymentTermClientCode'))\
                                 &(col('ptd0.paymentTermLanguageCode') == col('rlg0.languageCode'))\
                                 &(col('ptd0.paymentTerm') == col('ptm0.paymentTerm'))\
                                 &(col('ptd0.paymentTermDayLimit') == col('ptm0.paymentTermDayLimit'))),how='left')\
                            .join(fin_SAP_L0_TMP_PaymentTermText.alias('ptt0'),\
                                  ((col('ptt0.paymentTermClientCode') == col('ptm0.paymentTermClientCode'))\
                                 &(col('ptt0.paymentTermLanguageCode') == col('rlg0.languageCode'))\
                                 &(col('ptt0.paymentTerm') == col('ptm0.paymentTerm'))),how='left')\
                            .select(col('ptm0.paymentTermClientCode'),col('ptm0.paymentTerm'),col('ptm0.paymentTermDayLimit'),col('ptm0.paymentTermMinDayLimit')\
                                   ,col('ptm0.paymentTermMaxDayLimit'),coalesce(col('rlg0.languageCode'),lit('')).alias('paymentTermLanguageCode')\
                                   ,when(col('ptd0.paymentTermDescription').isNull(),lit(''))\
                                        .otherwise(col('ptd0.paymentTermDescription')).alias('paymentTermDescription')\
                                   ,when(col('ptt0.paymentTermText').isNull(),lit(''))\
                                        .otherwise(col('ptt0.paymentTermText')).alias('paymentTermText')\
                                   ,col('t052.ZFAEL').alias('paymentTermDayForPaymentBaselineDate'),col('t052.ZMONA').alias('paymentTermMonthNumberForPaymentBaselineDate')\
                                   ,col('t052.ZPRZ1').alias('paymentTermCashDiscountPercentageForPayment1'),col('t052.ZTAG1').alias('paymentTermCashDiscountDayForPayment1')\
                                   ,col('t052.ZSTG1').alias('paymentTermCashDiscountFixedCalendarDayForPayment1')\
                                   ,col('t052.ZSMN1').alias('paymentTermCashDiscountFixedMonthNumberForPayment1')\
                                   ,col('t052.ZPRZ2').alias('paymentTermCashDiscountPercentageForPayment2'),col('t052.ZTAG2').alias('paymentTermCashDiscountDayForPayment2')\
                                   ,col('t052.ZSTG2').alias('paymentTermCashDiscountFixedCalendarDayForPayment2')\
                                   ,col('t052.ZSMN2').alias('paymentTermCashDiscountFixedMonthNumberForPayment2'),col('t052.ZTAG3').alias('paymentTermDayForNetPayment')\
                                   ,col('t052.ZSTG3').alias('paymentTermFixedCalendarDayForNetPayment'),col('t052.ZSMN3').alias('paymentTermCashDiscountFixedMonthNumberForNetPayment')\
                                   ,when((col('ptm0.paymentTermMinDayLimit') == 0) & (col('ptm0.paymentTermMaxDayLimit') == 0),lit(0))\
                                    .otherwise(lit(1)).alias('isPaymentTermDayLimitDependent')\
                                   ,when((col('t052.ZSTG1') != 0) | (col('t052.ZSTG2') != 0) | (col('t052.ZSTG3') != 0),lit(1))\
                                    .otherwise(lit(0)).alias('isFixedDateForPaymentDueDate')\
                                   ,when(col('t052.KOART') != lit('K'),lit(1))\
                                    .otherwise(lit('0')).alias('isApplicableForCustomerGLAccount')\
                                   ,when(col('t052.KOART') != lit('D'),lit(1))\
                                    .otherwise(lit('0')).alias('isApplicableForVendorGLAccount'))

    fin_L1_MD_PaymentTerm =  objDataTransformation.gen_convertToCDMandCache \
         (fin_L1_MD_PaymentTerm,'fin','L1_MD_PaymentTerm',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "fin_L1_MD_PaymentTerm populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


