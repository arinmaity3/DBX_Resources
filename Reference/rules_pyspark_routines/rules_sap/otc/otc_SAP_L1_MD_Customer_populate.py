# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when

def otc_SAP_L1_MD_Customer_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
        global otc_L1_MD_Customer
        erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
        
        otc_L1_TMP_CustomerReportingLanguage = erp_T005T.alias('T005T') \
            .join (knw_LK_CD_ReportingSetup.alias('lkrs'),(col('lkrs.clientCode')==col('T005T.MANDT')),'inner') \
            .join (knw_LK_ISOLanguageKey.alias('lan'),(col('lan.sourceSystemValue') == col('T005T.SPRAS')),'inner') \
            .join (knw_clientDataReportingLanguage.alias('rlg1'),(col('rlg1.clientCode') == col('lkrs.clientCode')) \
                                                          & (col('rlg1.companyCode') == col('lkrs.companyCode')) \
                                                          & (col('rlg1.languageCode') == col('lan.targetSystemValue')),'inner') \
            .select(col('lkrs.clientCode').alias('clientCode') \
                    ,col('lkrs.companyCode').alias('companyCode') \
                    ,col('T005T.LAND1').alias('customerCountryCode') \
                    ,col('T005T.LANDX').alias('customerCountryName') \
                    ,col('rlg1.languageCode').alias('languageCode') \
                    )

        otc_L1_TMP_CustomerReportingLanguage_02 = knw_LK_CD_ReportingSetup.alias('lkrs') \
            .join (knw_clientDataReportingLanguage.alias('rlg1'),(col('rlg1.clientCode') == col('lkrs.clientCode')) \
                                                          & (col('rlg1.companyCode') == col('lkrs.companyCode')),'inner') \
            .join (otc_L1_TMP_CustomerReportingLanguage.alias('rpt'),(col('rpt.clientCode') == col('lkrs.clientCode')) \
                                                          & (col('rpt.companyCode') == col('lkrs.companyCode')) \
                                                          & (col('rpt.customerCountryCode') == lit('')),'leftanti') \
            .select(col('lkrs.clientCode').alias('clientCode') \
                    ,col('lkrs.companyCode').alias('companyCode') \
                    ,lit('').alias('customerCountryCode') \
                    ,lit('').alias('customerCountryName') \
                    ,col('rlg1.languageCode').alias('languageCode') \
                    )

        otc_L1_TMP_CustomerReportingLanguage = otc_L1_TMP_CustomerReportingLanguage.union(otc_L1_TMP_CustomerReportingLanguage_02)   
        
        otc_L1_TMP_CustomerReportingLanguage_03 = erp_T005T.alias('T005T') \
            .join (knw_LK_CD_ReportingSetup.alias('lkrs'),(col('lkrs.clientCode')==col('T005T.MANDT')),'inner') \
            .join (knw_LK_ISOLanguageKey.alias('lan'),(col('lan.sourceSystemValue') == col('T005T.SPRAS')),'inner') \
            .join (knw_clientDataSystemReportingLanguage.alias('rlg1'),(col('rlg1.clientCode') == col('lkrs.clientCode')) \
                                                          & (col('rlg1.companyCode') == col('lkrs.companyCode')) \
                                                          & (col('rlg1.languageCode') == col('lan.targetSystemValue')),'inner') \
            .join (otc_L1_TMP_CustomerReportingLanguage.alias('rpt'),(col('rpt.clientCode') == col('lkrs.clientCode')) \
                                                          & (col('rpt.companyCode') == col('lkrs.companyCode')) \
                                                          & (col('rpt.customerCountryCode') == col('T005T.LAND1')),'leftanti') \
            .select(col('lkrs.clientCode').alias('clientCode') \
                    ,col('lkrs.companyCode').alias('companyCode') \
                    ,col('T005T.LAND1').alias('customerCountryCode') \
                    ,col('T005T.LANDX').alias('customerCountryName') \
                    ,col('rlg1.languageCode').alias('languageCode') \
                    )

        otc_L1_TMP_CustomerReportingLanguage = otc_L1_TMP_CustomerReportingLanguage.union(otc_L1_TMP_CustomerReportingLanguage_03)   
        
        otc_SAP_L0_TMP_Customer = erp_KNB1.alias('KB') \
            .join (erp_KNA1.alias('KA'),(col('KB.MANDT').eqNullSafe(col('KA.MANDT'))) \
                                      & (col('KB.KUNNR').eqNullSafe(col('KA.KUNNR'))),'left') \
            .join (otc_L1_TMP_CustomerReportingLanguage.alias('CS'),(col('KB.MANDT').eqNullSafe(col('CS.clientCode'))) \
                                      & (col('KB.BUKRS').eqNullSafe(col('CS.companyCode'))) \
                                      & (col('KA.LAND1').eqNullSafe(col('CS.customerCountryCode'))),'left') \
            .select(col('KB.MANDT').alias('clientCode') \
                 ,col('KB.BUKRS').alias('companyCode') \
                 ,col('KB.KUNNR').alias('customerNumber') \
                 ,concat((when((col('KA.NAME1').isNull()|col('KA.NAME1').isin('','null')),lit('')) \
                 .otherwise(col('KA.NAME1'))),lit(' '),(when((col('KA.NAME2').isNull()|col('KA.NAME2') \
                 .isin('','null')),lit('')).otherwise(col('KA.NAME2')))).alias('customerName') \
                 ,when((col('KA.LIFSD').isNull()|col('KA.LIFSD').isin('','null')),lit(''))\
                 .otherwise(lit('DB')).alias('deliveryBlock') \
                 ,when((col('KA.AUFSD').isNull()|col('KA.AUFSD').isin('','null')),lit(''))\
                 .otherwise(lit('OB')).alias('orderBlock') \
                 ,when((col('KA.FAKSD').isNull()|col('KA.FAKSD').isin('','null')),lit(''))\
                 .otherwise(lit('IB')).alias('invoiceBlock') \
                 ,when((col('KA.SPERR').isNull()|col('KA.SPERR').isin('','null')),lit(''))\
                 .otherwise(lit('PB')).alias('postingBlock') \
                 ,col('KA.ORT01').alias('customerCity') \
                 ,col('KA.LAND1').alias('customerCountryCode') \
                 ,when(col('CS.customerCountryName').isNull(),lit('#NA#')) \
                 .otherwise(col('CS.customerCountryName')).alias('customerCountryName') \
                 ,when(col('KB.ERNAM').isNull(),'').otherwise(col('KB.ERNAM')).alias('createdBy') \
                 ,when(col('KB.ERDAT').isNull(),'1900-01-01').otherwise(col('KB.ERDAT')).alias('createdOn')\
                 ,lit(0).alias('isInterCoFlag') \
                 ,col('KA.VBUND').alias('tradingCompID') \
                 ,col('KB.ZTERM').alias('paymentTerm') \
                 ,when(col('KA.XCPDK') == lit('X'),lit(1)) \
                 .otherwise(lit(0)).alias('isOneTimeAccount') \
                 ,col('KB.AKONT').alias('customerAccountNumber') \
                 ,col('KA.VBUND').alias('customerCompanyID') \
                 ).distinct()

        otc_SAP_L0_TMP_Customer_02 = erp_KNA1.alias('KA') \
            .crossJoin (knw_LK_CD_ReportingSetup.alias('lkrs')) \
            .join (otc_L1_TMP_CustomerReportingLanguage.alias('CS'),(col('KA.MANDT').eqNullSafe(col('CS.clientCode'))) \
                                      & (col('lkrs.companyCode').eqNullSafe(col('CS.companyCode'))) \
                                      & (col('KA.LAND1').eqNullSafe(col('CS.customerCountryCode'))),'left') \
            .join (erp_KNB1.alias('KB'),(col('KB.MANDT') == col('KA.MANDT')) \
                                      & (col('KB.KUNNR') == col('KA.KUNNR')) \
                                      & (col('KB.BUKRS') == col('lkrs.companyCode')),'leftanti') \
                 .select(col('KA.MANDT').alias('clientCode') \
                 ,when(col('CS.companyCode').isNull(),col('lkrs.companyCode')) \
                 .otherwise(col('CS.companyCode')).alias('companyCode') \
                 ,col('KA.KUNNR').alias('customerNumber') \
                 ,concat((when((col('KA.NAME1').isNull()|col('KA.NAME1').isin('','null')),lit('')) \
                 .otherwise(col('KA.NAME1'))),lit(' '),(when((col('KA.NAME2').isNull()|col('KA.NAME2') \
                 .isin('','null')),lit('')).otherwise(col('KA.NAME2')))).alias('customerName') \
                 ,when((col('KA.LIFSD').isNull()|col('KA.LIFSD').isin('','null')),lit(''))\
                 .otherwise(lit('DB')).alias('deliveryBlock') \
                 ,when((col('KA.AUFSD').isNull()|col('KA.AUFSD').isin('','null')),lit(''))\
                 .otherwise(lit('OB')).alias('orderBlock') \
                 ,when((col('KA.FAKSD').isNull()|col('KA.FAKSD').isin('','null')),lit(''))\
                 .otherwise(lit('IB')).alias('invoiceBlock') \
                 ,when((col('KA.SPERR').isNull()|col('KA.SPERR').isin('','null')),lit(''))\
                 .otherwise(lit('PB')).alias('postingBlock') \
                 ,col('KA.ORT01').alias('customerCity') \
                 ,col('KA.LAND1').alias('customerCountryCode') \
                 ,when(col('CS.customerCountryName').isNull(),lit('#NA#')) \
                 .otherwise(col('CS.customerCountryName')).alias('customerCountryName') \
                 ,lit('').alias('createdBy') \
                 ,lit('1900-01-01').alias('createdOn') \
                 ,lit(0).alias('isInterCoFlag') \
                 ,col('KA.VBUND').alias('tradingCompID') \
                 ,lit('').alias('paymentTerm') \
                 ,when(col('KA.XCPDK') == lit('X'),lit(1)) \
                 .otherwise(lit(0)).alias('isOneTimeAccount') \
                 ,when(col('KA.KNRZA').isNull(),'').otherwise(col('KA.KNRZA')).alias('customerAccountNumber')\
                 ,col('KA.VBUND').alias('customerCompanyID') \
                 ).distinct()

        otc_SAP_L0_TMP_Customer = otc_SAP_L0_TMP_Customer.union(otc_SAP_L0_TMP_Customer_02)  

        KKKLIMK = when(col('KK.KLIMK').isNull(),lit(0)) \
                  .otherwise(col('KK.KLIMK'))
        KKCTLPC = when(col('KK.CTLPC').isNull(),lit('')) \
                  .otherwise(col('KK.CTLPC'))

        otc_SAP_L0_TMP_CustomerCreditLimit = erp_KNB1.alias('KB') \
            .join (erp_KNKK.alias('KK'), (col('KB.MANDT').eqNullSafe(col('KK.MANDT'))) \
                                      & (col('KB.KUNNR').eqNullSafe(col('KK.KUNNR'))),'left') \
            .join (erp_KNKK.alias('KN'), (col('KK.MANDT').eqNullSafe(col('KN.MANDT'))) \
                                      & (col('KK.KNKLI').eqNullSafe(col('KN.KUNNR'))) \
                                      & (col('KK.KKBER').eqNullSafe(col('KN.KKBER'))),'left') \
            .join (erp_T001.alias('T1'), (col('KB.MANDT') == col('T1.MANDT')) \
                                      & (col('KB.BUKRS') == col('T1.BUKRS')) \
                                      & (col('T1.KKBER') == col('KK.KKBER')),'inner')\
            .select(col('T1.BUKRS').alias('companyCode') \
            ,col('KB.KUNNR').alias('customerNumber') \
            ,col('KK.KKBER').alias('creditControlArea') \
            ,when(col('KN.KLIMK').isNull(),lit(KKKLIMK)) \
            .otherwise(col('KN.KLIMK')).alias('creditLimit') \
            ,when(col('KN.CTLPC').isNull(),lit(KKCTLPC)) \
            .otherwise('KN.CTLPC').alias('riskCategory') \
            ,col('KK.MANDT').alias('hasCreditLimit') \
            ).filter(col('KK.KKBER').isNotNull())

        delsrc = when(col('del.sourceSystemValue').isNull(), lit('') ) \
                              .otherwise(concat(lit(' & '),col('del.sourceSystemValueDescription')))
        ordsrc = when(col('ord.sourceSystemValue').isNull(), lit('') ) \
                              .otherwise(concat(lit(' & '),col('ord.sourceSystemValueDescription')))
        invsrc = when(col('inv.sourceSystemValue').isNull(), lit('') ) \
                              .otherwise(concat(lit(' & '),col('inv.sourceSystemValueDescription')))
        possrc = when(col('pos.sourceSystemValue').isNull(), lit('') ) \
                              .otherwise(concat(lit(' & '),col('pos.sourceSystemValueDescription')))
        stuffstring = concat(lit(delsrc),lit(ordsrc),lit(invsrc),lit(possrc))       
        
        otc_L1_MD_Customer = otc_SAP_L0_TMP_Customer.alias('cus') \
            .join (knw_LK_CD_ReportingSetup.alias('rpt'),(col('rpt.companyCode') == (when(col('cus.companyCode') \
                                      .isNull(),col('rpt.companyCode')).otherwise(col('cus.companyCode')))) \
                                      & (col('rpt.clientCode').eqNullSafe(col('cus.clientCode'))),'left') \
            .join (knw_LK_GD_BusinessDatatypeValueMapping.alias('del') \
                                      ,(col('del.businessDatatype').eqNullSafe(lit('CustomerBlockType'))) \
                                      & (col('cus.deliveryBlock').eqNullSafe(col('del.sourceSystemValue'))) \
                                      & (col('del.sourceERPSystemID').eqNullSafe(lit(erpGENSystemID))) \
                                      & (col('del.targetLanguageCode').eqNullSafe(col('rpt.KPMGDataReportingLanguage'))),'left') \
            .join (knw_LK_GD_BusinessDatatypeValueMapping.alias('inv') \
                                      ,(col('inv.businessDatatype').eqNullSafe(lit('CustomerBlockType'))) \
                                      & (col('cus.invoiceBlock').eqNullSafe(col('inv.sourceSystemValue'))) \
                                      & (col('inv.sourceERPSystemID').eqNullSafe(lit(erpGENSystemID))) \
                                      & (col('inv.targetLanguageCode').eqNullSafe(col('rpt.KPMGDataReportingLanguage'))),'left') \
            .join (knw_LK_GD_BusinessDatatypeValueMapping.alias('ord') \
                                      ,(col('ord.businessDatatype').eqNullSafe(lit('CustomerBlockType'))) \
                                      & (col('cus.orderBlock').eqNullSafe(col('ord.sourceSystemValue'))) \
                                      & (col('ord.sourceERPSystemID').eqNullSafe(lit(erpGENSystemID))) \
                                      & (col('ord.targetLanguageCode').eqNullSafe(col('rpt.KPMGDataReportingLanguage'))),'left') \
            .join (knw_LK_GD_BusinessDatatypeValueMapping.alias('pos') \
                                      ,(col('pos.businessDatatype').eqNullSafe(lit('CustomerBlockType'))) \
                                      & (col('cus.postingBlock').eqNullSafe(col('pos.sourceSystemValue'))) \
                                      & (col('pos.sourceERPSystemID').eqNullSafe(lit(erpGENSystemID))) \
                                      & (col('pos.targetLanguageCode').eqNullSafe(col('rpt.KPMGDataReportingLanguage'))),'left') \
            .join (knw_LK_GD_BusinessDatatypeValueMapping.alias('nob') \
                                      ,(col('nob.businessDatatype').eqNullSafe(lit('CustomerBlockType'))) \
                                      & (col('nob.sourceSystemValue').eqNullSafe(lit('NB'))) \
                                      & (col('nob.sourceERPSystemID').eqNullSafe(lit(erpGENSystemID))) \
                                      & (col('nob.targetLanguageCode').eqNullSafe(col('rpt.KPMGDataReportingLanguage'))),'left') \
            .join (otc_SAP_L0_TMP_CustomerCreditLimit.alias('ctcl0') \
                                      , (col('ctcl0.customerNumber').eqNullSafe(col('cus.customerNumber'))) \
                                      & (col('ctcl0.companyCode').eqNullSafe(col('cus.companyCode'))),'left') \
                 .select(col('cus.clientCode').alias('clientCode') \
                 ,col('cus.companyCode').alias('companyCode') \
                 ,col('cus.customerNumber').alias('customerNumber') \
                 ,col('cus.customerName').alias('customerName')\
                 ,lit(stuffstring).alias('stuffstring')\
                 ,col('nob.sourceSystemValueDescription').alias('sourceSystemValueDescription')\
                 ,col('cus.customerCity').alias('customerCity') \
                 ,col('cus.customerCountryCode').alias('customerCountryCode') \
                 ,col('cus.customerCountryName').alias('customerCountryName') \
                 ,col('cus.createdBy').alias('createdBy') \
                 ,col('cus.createdOn').alias('createdOn') \
                 ,col('cus.paymentTerm').alias('paymentTerm') \
                 ,col('cus.isOneTimeAccount').alias('isOneTimeAccount') \
                 ,col('cus.customerAccountNumber').alias('customerAccountNumber') \
                 ,col('cus.customerCompanyID').alias('customerCompanyID') \
                 ,col('ctcl0.creditLimit').alias('creditLimit'))\
                 .distinct()
        otc_L1_MD_Customer=otc_L1_MD_Customer.withColumn("length",length(col("stuffstring")))
        otc_L1_MD_Customer=otc_L1_MD_Customer.withColumn("blockType",col("stuffstring").substr(lit(3).cast(IntegerType()),length(col("stuffstring"))))
      
        otc_L1_MD_Customer = otc_L1_MD_Customer.select('clientCode','companyCode','customerNumber','customerName'\
                         ,when(((col('blockType').isNull()) | (col('blockType')=='')),col('sourceSystemValueDescription'))\
                                    .otherwise(col('blockType')).alias('blockType')\
                          ,'customerCity','customerCountryCode','customerCountryName','createdBy','createdOn','paymentTerm','isOneTimeAccount'\
                          ,'customerAccountNumber','customerCompanyID','creditLimit')
        
        otc_L1_MD_Customer = objDataTransformation.gen_convertToCDMandCache\
            (otc_L1_MD_Customer,'otc','L1_MD_Customer',targetPath=gl_CDMLayer1Path)
        
        executionStatus = "L1_MD_Customer populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
