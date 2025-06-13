# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when,concat,round

def ptp_SAP_L1_TD_PurchaseOrder_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global ptp_L1_TD_PurchaseOrder

    erpSAPSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')
    erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    reportingLanguage = knw_KPMGDataReportingLanguage.first()["languageCode"]
    extractionDate = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'EXTRACTION_DATE')
    
    clientProcessID_PTP = 2

    PurchaseOrderSubType = ptp_L1_MD_PurchaseOrderSubType.select('purchaseOrderSubTypeClientCode',\
                       'purchaseOrderSubTypeCompanyCode','purchaseOrderSubType','purchaseOrderType')\
                      .distinct()

    knw_LK_GD_BusinessDatatypeValueMapping = gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].\
            select("businessDatatype","businessDatatypeDescription","sourceERPSystemID"\
            ,"sourceSystemName","sourceSystemValue","sourceSystemValueDescription","targetERPSystemID","targetSystemName"\
            ,"targetSystemValue","targetSystemValueDescription","targetLanguageCode")
    
    knw_vw_LK_GD_ClientKPMGDocumentType = gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType'].\
                                 select('documentTypeClient','documentTypeClientProcessID','isReversed','documentTypeKPMG'\
                                       ,'documentTypeKPMGShort','documentTypeClientDescription','documentTypeClientID')
    
    v_BPRME_MEINS_IFF = when(col('ekp0.BPRME')==lit(''),col('ekp0.MEINS')).otherwise(col('ekp0.BPRME'))
    v_ekp0_UMREN_IFF = when(col('ekp0.UMREN')==lit(0.00),lit(1)).otherwise(col('ekp0.UMREN'))
    v_ekp0_UMREZ_IFF = when(col('ekp0.UMREZ')==lit(0.00),lit(1)).otherwise(col('ekp0.UMREZ'))
    maro_MEINS_nullif = when(col('mar0.MEINS')==lit(''),lit('')).otherwise(col('mar0.MEINS'))
    
    v_PEINH_IFF = when((col('ekp0.PEINH')==lit(0.00)),lit(1)).otherwise(col('ekp0.PEINH'))
    v_NETPR_IFF = when((col('ekp0.NETPR')==lit(0.00)),lit(1)).otherwise(col('ekp0.NETPR'))
    v_BPUMZ_IFF = when((col('ekp0.BPUMZ')==lit(0.00)),lit(1)).otherwise(col('ekp0.BPUMZ'))
    v_BPUMN_IFF = when((col('ekp0.BPUMN')==lit(0.00)),lit(1)).otherwise(col('ekp0.BPUMN'))
    
    purchaseOrderNumeratorOrderUnitToBaseUnit =	when(lit(maro_MEINS_nullif).isNull(),lit(0))\
                                                .when(col('ekp0.MEINS')!=(col('mar0.MEINS')),lit(v_ekp0_UMREZ_IFF))\
                                                .otherwise(lit(1))
    purchaseOrderDenominatorOrderUnitToBaseUnit = when(lit(maro_MEINS_nullif).isNull(),lit(0))\
                                                .when(col('ekp0.MEINS')!=(col('mar0.MEINS')),lit(v_ekp0_UMREN_IFF))\
                                                .otherwise(lit(1))    
    v_KTMNG_isnull = when(col('ekp0.KTMNG').isNull(),lit(0.00)).otherwise(col('ekp0.KTMNG'))
    v_MEINS = when(col('mar0.MEINS').isNull(),lit('')).otherwise(col('mar0.MEINS'))
    v_MENGE_Rund3 = round(col('ekp0.MENGE')*lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF),3)
    v_MENGE_Rund4 = round(col('ekp0.MENGE')*lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF),4)
    v_MENGE_UM_Rund3 = round(col('ekp0.MENGE')*(lit(v_ekp0_UMREZ_IFF)/lit(v_ekp0_UMREN_IFF)),3)
    v_MENGE_UM_Rund4 = round(col('ekp0.MENGE')*(lit(v_ekp0_UMREZ_IFF)/lit(v_ekp0_UMREN_IFF)),4)
    v_MENGE_QuantitySKU_Rund = when(lit(v_MENGE_UM_Rund3)==lit(0),lit(v_MENGE_UM_Rund4)).otherwise(lit(v_MENGE_UM_Rund3))
    v_KTMNG_Rund3 = round(col('ekp0.KTMNG')*(lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)),3)
    v_KTMNG_Rund4 = round(col('ekp0.KTMNG')*(lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)),4)
    v_BRTWR_Rund3 = round((col('ekp0.BRTWR'))*lit(v_PEINH_IFF)/lit(v_NETPR_IFF),3)
    v_BRTWR_Rund4 = round((col('ekp0.BRTWR'))*lit(v_PEINH_IFF)/lit(v_NETPR_IFF),4)
    v_KTMNG_TQutInOrderPriceUnit_Rund = when(lit(v_KTMNG_Rund3)==lit(0),lit(v_KTMNG_Rund4)).otherwise(lit(v_KTMNG_Rund3))
    v_BRTWR_TQutInOrderPriceUnit_Rund = when(lit(v_BRTWR_Rund3)==lit(0),lit(v_BRTWR_Rund4)).otherwise(lit(v_BRTWR_Rund3))
    
    v_BRTWR_PuTrgQuntyInOrderUnit_Rund3 = round(((col('ekp0.BRTWR')*lit(v_PEINH_IFF))/(lit(v_NETPR_IFF)*\
                                                 (lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)))),3)
    v_BRTWR_PuTrgQuntyInOrderUnit_Rund4 = round(((col('ekp0.BRTWR')*lit(v_PEINH_IFF))/(lit(v_NETPR_IFF)*\
                                                 (lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)))),4)
    v_BRTWR_PuTrgQuntyInOrderUnit_Rund = when(lit(v_BRTWR_PuTrgQuntyInOrderUnit_Rund3)==0.00,\
         lit(v_BRTWR_PuTrgQuntyInOrderUnit_Rund4)).otherwise(lit(v_BRTWR_PuTrgQuntyInOrderUnit_Rund3))
    v_InStockKeepingUnit_BRTWR_Rund3 = round(((col('ekp0.BRTWR')*lit(v_PEINH_IFF))/(lit(v_NETPR_IFF)*\
                                                (lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)))),3)
    v_InStockKeepingUnit_BRTWR_Rund4 = round(((col('ekp0.BRTWR')*lit(v_PEINH_IFF))/(lit(v_NETPR_IFF)*\
                                                (lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)))),4)
    v_InStockKeepingUnit_BRTWR_Rund = when(lit(v_InStockKeepingUnit_BRTWR_Rund3)==0.00,lit(v_InStockKeepingUnit_BRTWR_Rund4))\
                            .otherwise(lit(v_InStockKeepingUnit_BRTWR_Rund3))
    
    v_InStockKeepingUnit_KTMNG_Rund3 = round((col('ekp0.KTMNG')*(lit(v_ekp0_UMREZ_IFF)/lit(v_ekp0_UMREN_IFF))),3)
    v_InStockKeepingUnit_KTMNG_Rund4 = round((col('ekp0.KTMNG')*(lit(v_ekp0_UMREZ_IFF)/lit(v_ekp0_UMREN_IFF))),4)
    v_InStockKeepingUnit_KTMNG_Rund = when(lit(v_InStockKeepingUnit_KTMNG_Rund3)==0.00,lit(v_InStockKeepingUnit_KTMNG_Rund4))\
                        .otherwise(lit(v_InStockKeepingUnit_KTMNG_Rund3))
    #
    v_InStockKeepingUnit_Rund3 = round(((col('ekp0.BRTWR')*lit(v_PEINH_IFF))/(lit(v_NETPR_IFF)*\
                                      (lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)))*(lit(v_ekp0_UMREZ_IFF)/lit(v_ekp0_UMREN_IFF))),3)
    v_InStockKeepingUnit_Rund4 = round(((col('ekp0.BRTWR')*lit(v_PEINH_IFF))/(lit(v_NETPR_IFF)*\
                                      (lit(v_BPUMZ_IFF)/lit(v_BPUMN_IFF)))*(lit(v_ekp0_UMREZ_IFF)/lit(v_ekp0_UMREN_IFF))),4)
    v_InStockKeepingUnit_Rund = when((lit(v_InStockKeepingUnit_Rund3)==0.00),lit(v_InStockKeepingUnit_Rund4))\
                       .otherwise(lit(v_InStockKeepingUnit_Rund3))
    
    v_InStockKeepingUnit_case1 =when((col('ekp0.MEINS')==lit(v_MEINS)),col('ekp0.KTMNG'))\
                   .otherwise(lit(v_InStockKeepingUnit_KTMNG_Rund))
    v_InStockKeepingUnit_case2 =when((col('ekp0.MEINS')==lit(v_MEINS)),lit(v_InStockKeepingUnit_BRTWR_Rund))\
                   .otherwise(lit(v_InStockKeepingUnit_Rund))

    ptp_L1_TD_PurchaseOrder = erp_EKKO.alias('ekk0')\
                          .join(erp_EKPO.alias('ekp0'),((col('ekk0.MANDT').eqNullSafe(col('ekp0.MANDT')))&\
                                                       (col('ekk0.EBELN').eqNullSafe(col('ekp0.EBELN')))),how='left')\
                          .join(erp_MARA.alias('mar0'),((col('ekp0.MANDT').eqNullSafe(col('mar0.MANDT')))&\
                                                       (col('ekp0.MATNR').eqNullSafe(col('mar0.MATNR')))),how='left')\
                          .join(erp_T001.alias('t001'),((col('ekk0.MANDT').eqNullSafe(col('t001.MANDT')))&\
                                                       (col('ekk0.BUKRS').eqNullSafe(col('t001.BUKRS')))),how='left')\
                          .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt0')\
                                ,((col('bdt0.sourceSystemValue').eqNullSafe(col('ekk0.BSTYP')))&\
                                (col('bdt0.businessDatatype').eqNullSafe(lit('Purchasing Document Category')))&\
                                (col('bdt0.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
                                (col('bdt0.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
                                (col('bdt0.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
                          .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt1')\
                                ,((col('bdt1.sourceSystemValue').eqNullSafe(col('ekp0.PSTYP')))&\
                                (col('bdt1.businessDatatype').eqNullSafe(lit('Purchasing Document Item Category')))&\
                                (col('bdt1.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
                                (col('bdt1.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
                                (col('bdt1.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
                         .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt3')\
                                ,((col('bdt3.sourceSystemValue').eqNullSafe(col('ekk0.BSAKZ')))&\
                                (col('bdt3.businessDatatype').eqNullSafe(lit('Purchasing Document Control Type')))&\
                                (col('bdt3.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
                                (col('bdt3.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
                                (col('bdt3.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
                        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt4')\
                                ,((col('bdt4.sourceSystemValue').eqNullSafe(col('ekk0.STATU')))&\
                                (col('bdt4.businessDatatype').eqNullSafe(lit('Purchasing Document Status')))&\
                                (col('bdt4.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
                                (col('bdt4.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
                                (col('bdt4.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
                        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt5')\
                                ,((col('bdt5.sourceSystemValue').eqNullSafe(col('ekk0.PROCSTAT')))&\
                                (col('bdt5.businessDatatype').eqNullSafe(lit('Purchasing Document Processing State')))&\
                                (col('bdt5.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
                                (col('bdt5.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
                                (col('bdt5.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
                       .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt10')\
                                ,((col('bdt10.sourceSystemValue').eqNullSafe(col('ekp0.KZVBR')))&\
                                (col('bdt10.businessDatatype').eqNullSafe(lit('Consumption Posting Type')))&\
                                (col('bdt10.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
                                (col('bdt10.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
                                (col('bdt10.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
                      .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('dct1'),\
                           ((col('dct1.documentTypeClient').eqNullSafe(col('ekk0.BSTYP')))&\
                            (col('dct1.documentTypeClientProcessID').eqNullSafe(lit(clientProcessID_PTP)))&\
                            (col('dct1.isReversed').eqNullSafe(lit(0)))),how='left')\
                     .join(PurchaseOrderSubType.alias('pcos1'),\
                           ((col('pcos1.purchaseOrderSubTypeClientCode').eqNullSafe(col('ekk0.MANDT')))&\
                            (col('pcos1.purchaseOrderSubTypeCompanyCode').eqNullSafe(col('ekk0.BUKRS')))&\
                            (col('pcos1.purchaseOrderSubType').eqNullSafe(col('ekk0.BSART')))&\
                            (col('pcos1.purchaseOrderType').eqNullSafe(col('ekk0.BSTYP')))),how='left')\
                     .select(col('ekk0.MANDT').alias('purchaseOrderClientCode'),col('ekk0.EBELN').alias('purchaseOrderNumber')\
                            ,when(col('ekp0.EBELP').isNull(),lit('')).otherwise(col('ekp0.EBELP')).alias('purchaseOrderLineItem')\
             ,concat(lit(0),when(col('ekp0.EBELP').isNull(),lit('')).otherwise(col('ekp0.EBELP'))).alias('purchaseOrdeLineItem2')\
                            ,col('dct1.documentTypeKPMG').alias('purchaseOrderTypeKPMG')\
                            ,col('dct1.documentTypeKPMGShort').alias('purchaseOrderTypeKPMGShort')\
                            ,col('dct1.documentTypeClientDescription').alias('purchaseOrderTypeClientDescription')\
                            ,col('ekk0.BEDAT').alias('purchaseOrderDate')\
                            ,when(col('ekk0.BEDAT')>lit(extractionDate),lit(extractionDate)).otherwise(col('ekk0.BEDAT'))\
                              .alias('purchaseOrderConversionDate')\
                            ,col('ekk0.ERNAM').alias('purchaseOrderCreationUser')\
                            ,col('ekk0.AEDAT').alias('purchaseOrderCreationDate')\
                            ,lit('1900-01-01 00:00:000').alias('purchaseOrderCreationTime'),lit('').alias('purchaseOrderSegment01')\
                            ,lit('').alias('purchaseOrderSegment02'),lit('').alias('purchaseOrderSegment03')\
                            ,lit('').alias('purchaseOrderSegment04'),lit('').alias('purchaseOrderSegment05')\
                            ,col('ekk0.BUKRS').alias('purchaseOrderCompanyCode')\
                            ,col('ekk0.EKORG').alias('purchaseOrderPurchasingOrganisation')\
                            ,col('ekk0.EKGRP').alias('purchaseOrderPurchasingGroup'),col('ekp0.WERKS').alias('purchaseOrderPlant')\
                            ,col('ekp0.MATNR').alias('purchaseOrderMaterialNumber')\
                            ,col('ekp0.MATNR').alias('purchaseOrderProductArtificialID')\
                            ,col('ekk0.KUNNR').alias('purchaseOrderCustomerNumber'),col('ekk0.LIFNR').alias('purchaseOrderVendorNumber')\
           ,col('ekk0.LLIEF').alias('purchaseOrderSupplingVendorNumber'),col('ekk0.AUSNR').alias('purchaseOrderReferenceHeader')\
           ,lit('').alias('purchaseOrderReferenceLineItem'),lit('').alias('purchaseOrderReferenceFinYear')\
           ,lit('').alias('purchaseOrderReferenceAdditionalFilter'),lit('').alias('purchaseOrderReferenceAdditionalFilterPlaceHolder')\
           ,lit('').alias('purchaseOrderReferenceFinYear2'),lit('').alias('purchaseOrderReferenceAdditionalFilter2')\
           ,lit('').alias('purchaseOrderReferenceAdditionalFilterPlaceHolder2'),col('ekk0.ANGNR').alias('purchaseOrderReferenceHeader3')\
           ,lit('').alias('purchaseOrderReferenceLineItem3'),lit('').alias('purchaseOrderReferenceFinYear3')\
           ,lit('').alias('purchaseOrderReferenceAdditionalFilter3'),lit('').alias('purchaseOrderReferenceAdditionalFilterPlaceHolder3')\
           ,col('ekp0.KONNR').alias('purchaseOrderReferenceHeader4'),col('ekp0.KTPNR').alias('purchaseOrderReferenceLineItem4')\
           ,lit('').alias('purchaseOrderReferenceFinYear4'),lit('').alias('purchaseOrderReferenceAdditionalFilter4')\
           ,lit('').alias('purchaseOrderReferenceAdditionalFilterPlaceHolder4')\
           ,col('ekp0.BANFN').alias('purchaseOrderReferenceHeader5'),col('ekp0.BNFPO').alias('purchaseOrderReferenceLineItem5')\
           ,lit('').alias('purchaseOrderReferenceFinYear5'),lit('').alias('purchaseOrderReferenceAdditionalFilter5')\
           ,lit('').alias('purchaseOrderReferenceAdditionalFilterPlaceHolder5'),lit('').alias('purchaseOrderReferenceLineItem6')\
           ,lit('').alias('purchaseOrderReferenceFinYear6'),lit('').alias('purchaseOrderReferenceAdditionalFilter6')\
           ,lit('').alias('purchaseOrderReferenceAdditionalFilterPlaceHolder6')\
           ,when(col('bdt0.targetSystemValue').isNull(),col('ekk0.BSTYP'))\
                 .otherwise(col('bdt0.targetSystemValue')).alias('purchaseOrderType')\
           ,when(col('bdt0.targetSystemValueDescription').isNull(),lit(''))\
                           .otherwise(col('bdt0.targetSystemValueDescription')).alias('purchaseOrderTypeDescription')\
           ,col('pcos1.purchaseOrderSubType').alias('purchaseOrderSubType')\
           ,when(col('bdt1.targetSystemValue').isNull(),col('ekp0.PSTYP'))\
                             .otherwise(col('bdt1.targetSystemValue')).alias('purchaseOrderCategory')\
          ,when(col('bdt1.targetSystemValueDescription').isNull(),lit(''))\
                       .otherwise(col('bdt1.targetSystemValueDescription')).alias('purchaseOrderCategoryDescription')\
          ,round(col('ekp0.MENGE'),4).alias('purchaseOrderQuantity')\
          ,col('ekp0.MEINS').alias('purchaseOrderMeasureUnit')\
          ,lit(v_MEINS).alias('purchaseOrderSKU')\
          ,lit(v_BPRME_MEINS_IFF).alias('purchaseOrderPurchasingPriceUnit')\
          ,col('ekp0.NETWR').alias('purchaseOrderAmountDC'),lit(0.00).alias('purchaseOrderAmountLC')\
          ,lit(0.00).alias('purchaseOrderUnitPriceSULC'),lit(0.00).alias('purchaseOrderUnitPriceSKULC')\
          ,col('ekk0.WAERS').alias('purchaseOrderDocumentCurrency')\
          ,col('T001.WAERS').alias('purchaseOrderLocalCurrency')\
          ,col('ekk0.WKURS').alias('purchaseOrderExchangeRate')\
          ,when(col('ekk0.KUFIX')=='X',lit(1)).otherwise(lit(0)).alias('isExchangeRateFixed')\
         ,when(col('ekp0.MEINS')!=lit(v_BPRME_MEINS_IFF),lit(v_BPUMZ_IFF)).otherwise(lit(1))\
               .alias('purchaseOrderNumeratorOrderPriceUnitToOrderUnit')\
         ,when(col('ekp0.MEINS')!=lit(v_BPRME_MEINS_IFF),lit(v_BPUMN_IFF)).otherwise(lit(1))\
               .alias('purchaseOrderDenominatorOrderPriceUnitToOrderUnit')\
		  ,col('ekp0.NETPR').alias('purchaseOrderNetPriceValueDC'),lit(0.00).alias('purchaseOrderNetPriceValueLC')\
         ,lit(v_PEINH_IFF).alias('purchaseOrderPriceUnit')\
         ,col('ekp0.MEINS').alias('purchaseOrderNetPriceOrderUnit')\
         ,lit(v_BPRME_MEINS_IFF).alias('purchaseOrderNetPriceOrderPriceUnit')\
         ,lit(v_MEINS).alias('purchaseOrderNetPriceStockKeepingUnit')\
         ,when(when(col('ekp0.ZWERT').isNull(),lit(0)).otherwise(col('ekp0.ZWERT'))!=0,col('ekp0.ZWERT'))\
                                  .otherwise(col('ekp0.BRTWR')).alias('purchaseOrderTargetAmountDC')\
         ,lit(0.00).alias('purchaseOrderTargetAmountLC'),col('ekp0.BRTWR').alias('purchaseOrderGrossAmountDC')\
         ,lit(0.00).alias('purchaseOrderGrossAmountLC'),lit(0.00).alias('purchaseOrderAmountReducedDC')\
		 ,lit(0.00).alias('purchaseOrderAmountReducedLC')\
         ,when(col('ekp0.SCHPR')==lit('X'),lit(1)).otherwise(lit(0))\
                             .alias('isPriceEstimated')\
         ,col('ekp0.EFFWR').alias('purchaseOrderEffectiveValue'),col('ekp0.UEBTO').alias('purchaseOrderOverdeliveryToleranceLimit')\
         ,col('ekp0.UNTTO').alias('purchaseOrderUnderdeliveryToleranceLimit')\
         ,when(col('ekp0.UEBTK')==lit('X'),lit(1)).otherwise(lit(0)).alias('isUnlimitedOverdeliveryAllowed')\
         ,when(col('bdt3.targetSystemValue').isNull(),col('ekk0.BSAKZ'))\
                              .otherwise(col('bdt3.targetSystemValue')).alias('purchaseOrderControlType')\
         ,when(col('bdt3.targetSystemValueDescription').isNull(),lit(''))\
                             .otherwise(col('bdt3.targetSystemValueDescription')).alias('purchaseOrderControlTypeDescription')\
         ,when(col('bdt4.targetSystemValue').isNull(),col('ekk0.STATU'))\
                             .otherwise(col('bdt4.targetSystemValue')).alias('purchaseOrderProcessingStatus')\
         ,when(col('bdt4.targetSystemValueDescription').isNull(),lit(''))\
                             .otherwise(col('bdt4.targetSystemValueDescription')).alias('purchaseOrderProcessingStatusDescription')\
         ,col('ekk0.ZTERM').alias('purchaseOrderPaymentTerm'),col('ekk0.ZBD1T').alias('purchaseOrderCashDiscountDayForPayment1')\
         ,col('ekk0.ZBD2T').alias('purchaseOrderCashDiscountDayForPayment2'),col('ekk0.ZBD3T').alias('purchaseOrderDayForNetPayment')\
         ,col('ekk0.ZBD1P').alias('purchaseOrderCashDiscountPercentageForPayment1')\
         ,col('ekk0.ZBD2P').alias('purchaseOrderCashDiscountPercentageForPayment2')\
         ,col('ekk0.INCO1').alias('purchaseOrderIncotermsPart1'),col('ekk0.INCO2').alias('purchaseOrderIncotermsPart2')\
         ,col('ekk0.KNUMV').alias('purchaseOrderConditionDocumentNumber')\
         ,col('ekk0.KDATB').alias('purchaseOrderStartOfValidityPeriod'),col('ekk0.KDATE').alias('purchaseOrderEndOfValidityPeriod')\
         ,col('ekk0.ANGDT').alias('purchaseOrderBidQuotationSubmissionDeadline')\
         ,col('ekk0.BNDDT').alias('purchaseOrderQuotationBindingPeriod')\
         ,col('ekk0.GWLDT').alias('purchaseOrderWarrantyDate'),col('ekk0.IHRAN').alias('purchaseOrderSubmissionDate')\
         ,col('ekp0.UEBPO').alias('purchaseOrderHigherLevelItem')\
         ,when(col('ekp0.LOEKZ')==lit('L'),lit(1)).otherwise(lit(0)).alias('isDeleted')\
         ,when(col('ekk0.MEMORY')==lit('X'),lit(0)).otherwise(lit(1)).alias('isCompleted')\
         ,col('ekk0.FRGGR').alias('purchaseOrderReleaseGroup'),col('ekk0.FRGSX').alias('purchaseOrderReleaseStrategy')\
         ,col('ekk0.FRGZU').alias('purchaseOrderReleaseStatus')\
         ,when(col('ekk0.FRGRL')==lit('X'),lit(0)).otherwise(lit(1)).alias('isReleaseComplettyEffected')\
         ,when(col('bdt5.targetSystemValue').isNull(),col('ekk0.PROCSTAT'))\
                             .otherwise(col('bdt5.targetSystemValue')).alias('purchaseOrderProcessingState')\
         ,when(col('bdt5.targetSystemValueDescription').isNull(),lit(''))\
                             .otherwise(col('bdt5.targetSystemValueDescription')).alias('purchaseOrderProcessingStateDescription')\
         ,col('ekk0.REVNO').alias('purchaseOrderVersionNumber'),col('ekp0.AEDAT').alias('purchaseOrderChangeDate')\
         ,when(col('ekp0.ABSKZ')==lit('X'),lit(1)).otherwise(lit(0)).alias('isRejected')\
         ,when(col('ekp0.ELIKZ')==lit('X'),lit(1)).otherwise(lit(0)).alias('isDeliveryCompleted')\
         ,col('ekp0.PLIFZ').alias('purchaseOrderPlannedDeliveryTime')\
         ,when(col('ekp0.WEPOS')==lit('X'),lit(1)).otherwise(lit(0)).alias('isGoodsReceiptExpected')\
         ,when(col('ekp0.WEUNB')==lit('X'),lit(1)).otherwise(lit(0)).alias('isGoodsReceiptNonValuated')\
         ,when(col('ekp0.WEBRE')==lit('X'),lit(1)).otherwise(lit(0))\
                                     .alias('isGoodsReceiptBasedInvoiceVerificationActive')\
         ,when(col('ekp0.REPOS')==lit('X'),lit(1)).otherwise(lit(0)).alias('isInvoiceReceiptExpected')\
         ,when(col('ekp0.EREKZ')==lit('X'),lit(1)).otherwise(lit(0)).alias('isFinalInvoicePosted')\
         ,when(col('ekp0.XERSY')==lit('X'),lit(1)).otherwise(lit(0)).alias('isAutomaticSettlement')\
         ,col('ekp0.KNTTP').alias('purchaseOrderAccountAssignmentCategory')\
         ,when(col('bdt10.targetSystemValue').isNull(),col('ekp0.KZVBR'))\
                             .otherwise(col('bdt10.targetSystemValue')).alias('purchaseOrderConsumptionPosting')\
         ,when(col('bdt10.targetSystemValueDescription').isNull(),lit(''))\
                             .otherwise(col('bdt10.targetSystemValueDescription')).alias('purchaseOrderConsumptionPostingDescription')\
         ,lit(erpSAPSystemID).alias('purchaseOrderSourceERPSystemID'),lit('EKPO').alias('purchaseOrderSourceERPTable')\
         ,when(col('dct1.documentTypeClientID').isNull(),lit(95))\
                             .otherwise(col('dct1.documentTypeClientID')).alias('purchaseOrderDocumentTypeClientID')\
         ,lit(purchaseOrderDenominatorOrderUnitToBaseUnit).alias('purchaseOrderDenominatorOrderUnitToBaseUnit')\
         ,lit(purchaseOrderNumeratorOrderUnitToBaseUnit).alias('purchaseOrderNumeratorOrderUnitToBaseUnit')\
         ,when((col('ekp0.MENGE')==lit(0.00)) | (col('ekp0.MEINS') == lit(v_BPRME_MEINS_IFF)),col('ekp0.MENGE'))\
	                      .otherwise(when(lit(v_MENGE_Rund3)==lit(0.00),lit(v_MENGE_Rund4)).otherwise(lit(v_MENGE_Rund3)))\
                                 .alias('purchaseOrderQuantityPriceUnit')\
         ,round(when(col('ekp0.MATNR')=='',lit(0)).\
                      otherwise(when(((col('ekp0.MENGE')==0) | (col('ekp0.MEINS')==lit(v_MEINS))),col('ekp0.MENGE'))\
                    .otherwise(lit(v_MENGE_QuantitySKU_Rund))),4).alias('purchaseOrderQuantitySKU')\
         ,round(when(col('ekp0.BSTYP')!='F',when(lit(v_KTMNG_isnull)!=lit(0),\
               when(col('ekp0.MEINS')==lit(v_BPRME_MEINS_IFF),col('ekp0.KTMNG'))\
               .otherwise(v_KTMNG_TQutInOrderPriceUnit_Rund))\
                .otherwise(when(col('ekp0.BRTWR')==lit(0),lit(0)).otherwise(v_BRTWR_TQutInOrderPriceUnit_Rund)))\
		        .otherwise(lit(0)),4).alias('purchaseOrderTargetQuantityInOrderPriceUnit')\
         ,round(when(col('ekp0.BSTYP')!='F',when(lit(v_KTMNG_isnull)!=lit(0.00),col('ekp0.KTMNG'))\
             .otherwise(when(col('ekp0.BRTWR')==lit(0.00),lit(0)).otherwise(lit(v_BRTWR_PuTrgQuntyInOrderUnit_Rund))))\
             .otherwise(lit(0)),4).alias('purchaseOrderTargetQuantityInOrderUnit')\
         ,when(col('ekp0.BSTYP')!='F',when(col('ekp0.MATNR')=='',lit(0.00))\
                            .otherwise(when((lit(v_KTMNG_isnull)!=0.00),lit(v_InStockKeepingUnit_case1))\
                                       .otherwise(when((col('ekp0.BRTWR')==0.00),lit(0)).otherwise(lit(v_InStockKeepingUnit_case2)))))\
             .otherwise(lit(0)).alias('purchaseOrderTargetQuantityInStockKeepingUnit')
           )

#update1-Column1
    purchaseOrderNetPriceInOrderUnit = round(when(col('purchaseOrderTypeKPMG').isin('Purchase Order','Scheduling Agreement'),\
    	         when(((col('purchaseOrderAmountDC')==0.00) | (col('purchaseOrderQuantity')==0.00)),col('purchaseOrderNetPriceValueDC'))\
              .otherwise((col('purchaseOrderAmountDC')/col('purchaseOrderQuantity'))))\
    	      .otherwise(when(col('purchaseOrderTargetAmountDC')!=0,when(col('purchaseOrderTargetQuantityInOrderUnit')==0.00,\
    	                                           col('purchaseOrderNetPriceValueDC'))\
    	      .otherwise(col('purchaseOrderTargetAmountDC')/(col('purchaseOrderTargetQuantityInOrderUnit'))))\
    		  .otherwise(col('purchaseOrderNetPriceValueDC'))),6)

#update1-column2
    v_purchaseOrderNetPriceInOrderPriceUnit_case1 = when(((col('purchaseOrderAmountDC')==0.00)|\
                               (col('purchaseOrderQuantityPriceUnit')==0.00)),\
                           (col('purchaseOrderNetPriceValueDC')/col('purchaseOrderPriceUnit'))*\
       (col('purchaseOrderNumeratorOrderPriceUnitToOrderUnit')/(col('purchaseOrderDenominatorOrderPriceUnitToOrderUnit'))))\
       .otherwise(col('purchaseOrderAmountDC')/col('purchaseOrderQuantityPriceUnit'))
    
    v_purchaseOrderNetPriceInOrderPriceUnit_case2 = \
        when(col('purchaseOrderNetPriceOrderUnit')!=(col('purchaseOrderNetPriceOrderPriceUnit')),\
         ((col('purchaseOrderNetPriceValueDC')/(col('purchaseOrderPriceUnit')))*\
          (col('purchaseOrderNumeratorOrderPriceUnitToOrderUnit')/(col('purchaseOrderDenominatorOrderPriceUnitToOrderUnit')))))\
        .otherwise(col('purchaseOrderNetPriceValueDC')/col('purchaseOrderPriceUnit'))

    purchaseOrderNetPriceInOrderPriceUnit = \
        round(when(col('purchaseOrderTypeKPMG').isin('Purchase Order','Scheduling Agreement'),lit(v_purchaseOrderNetPriceInOrderPriceUnit_case1)) \
        .otherwise(when(col('purchaseOrderTargetAmountDC')!=0,when(col('purchaseOrderTargetQuantityInOrderPriceUnit')==0.00,\
                                               (col('purchaseOrderNetPriceValueDC')/col('purchaseOrderPriceUnit')))\
                              .otherwise(col('purchaseOrderTargetAmountDC')/col('purchaseOrderTargetQuantityInOrderPriceUnit')))\
                       .otherwise(lit(v_purchaseOrderNetPriceInOrderPriceUnit_case2))),6)
    
#Update1=Column3
    v_purchaseOrderNetPriceInStockKeepingUnit_case1 = when(((col('purchaseOrderAmountDC')==0.00)|(col('purchaseOrderQuantitySKU')==0.00)),\
        ((col('purchaseOrderNetPriceValueDC')/(col('purchaseOrderPriceUnit')))*\
        (col('purchaseOrderNumeratorOrderUnitToBaseUnit')/col('purchaseOrderDenominatorOrderUnitToBaseUnit'))))\
        .otherwise(col('purchaseOrderAmountDC')/col('purchaseOrderQuantitySKU'))
    
    v_purchaseOrderNetPriceInStockKeepingUnit_case2 = when(col('purchaseOrderNetPriceOrderUnit')!=(\
                                col('purchaseOrderNetPriceStockKeepingUnit')),\
                       ((col('purchaseOrderNetPriceValueDC')/col('purchaseOrderPriceUnit'))*\
                       (col('purchaseOrderNumeratorOrderUnitToBaseUnit')/col('purchaseOrderDenominatorOrderUnitToBaseUnit'))))\
                 .otherwise(col('purchaseOrderNetPriceValueDC')/col('purchaseOrderPriceUnit'))
    
    purchaseOrderNetPriceInStockKeepingUnit	= when(col('purchaseOrderTypeKPMG').isin('Purchase Order','Scheduling Agreement'),\
           when(col('purchaseOrderNetPriceStockKeepingUnit')!='',lit(v_purchaseOrderNetPriceInStockKeepingUnit_case1))\
           .otherwise(lit(0))).otherwise(when(col('purchaseOrderNetPriceStockKeepingUnit')!='',\
                                             when(col('purchaseOrderTargetAmountDC')!=0.00,\
                                                 when(col('purchaseOrderTargetQuantityInStockKeepingUnit')==0.00,\
                                                    (col('purchaseOrderNetPriceValueDC')/col('purchaseOrderPriceUnit')))\
                             .otherwise(col('purchaseOrderTargetAmountDC')/col('purchaseOrderTargetQuantityInStockKeepingUnit')))\
                             .otherwise(lit(v_purchaseOrderNetPriceInStockKeepingUnit_case2)))\
                              .otherwise(lit(0)))
    			
    
    ptp_L1_TD_PurchaseOrder = ptp_L1_TD_PurchaseOrder.withColumn("purchaseOrderNetPriceInOrderUnit",lit(purchaseOrderNetPriceInOrderUnit))\
                          .withColumn("purchaseOrderNetPriceInOrderPriceUnit",lit(purchaseOrderNetPriceInOrderPriceUnit))\
                          .withColumn("purchaseOrderNetPriceInStockKeepingUnit",lit(purchaseOrderNetPriceInStockKeepingUnit))

    ptp_L1_TD_PurchaseOrder = ptp_L1_TD_PurchaseOrder.alias('pro1')\
                     .join(gen_L1_STG_MessageStatus.alias('mst1')\
					       ,((col('mst1.objectKey')==col('pro1.purchaseOrderNumber'))&\
						   (col('mst1.messageRank')==lit(1))),how='inner')\
					 .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt15')\
					     ,((col('bdt15.sourceSystemValue').eqNullSafe(col('mst1.processingStatus')))&\
						 (col('bdt15.businessDatatype').eqNullSafe(lit('Message Processing Status')))&\
						 (col('bdt15.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
						 (col('bdt15.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
						 (col('bdt15.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
					  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt16')\
					     ,((col('bdt16.sourceSystemValue').eqNullSafe(col('mst1.transmissionMedium')))&\
						 (col('bdt16.businessDatatype').eqNullSafe(lit('Message Transmission Medium')))&\
						 (col('bdt16.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))&\
						 (col('bdt16.targetERPSystemID').eqNullSafe(lit(erpGENSystemID)))&\
						 (col('bdt16.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))),how='left')\
					.select(col('pro1.purchaseOrderClientCode'),col('pro1.purchaseOrderNumber')\
                    ,col('pro1.purchaseOrderLineItem')\
			,col('pro1.purchaseOrdeLineItem2')\
			,col('pro1.purchaseOrderTypeKPMG')\
			,col('pro1.purchaseOrderTypeKPMGShort')\
			,col('pro1.purchaseOrderTypeClientDescription')\
			,col('pro1.purchaseOrderDate')\
			,col('pro1.purchaseOrderConversionDate')\
			,col('pro1.purchaseOrderCreationUser')\
			,col('pro1.purchaseOrderCreationDate'),col('pro1.purchaseOrderCreationTime')\
            ,col('pro1.purchaseOrderSegment01')\
			,col('pro1.purchaseOrderSegment02'),col('pro1.purchaseOrderSegment03')\
            ,col('pro1.purchaseOrderSegment04')\
			,col('pro1.purchaseOrderSegment05'),col('pro1.purchaseOrderCompanyCode')\
            ,col('pro1.purchaseOrderPurchasingOrganisation')\
			,col('pro1.purchaseOrderPurchasingGroup'),col('pro1.purchaseOrderPlant')\
            ,col('pro1.purchaseOrderMaterialNumber')\
			,col('pro1.purchaseOrderProductArtificialID')\
			,col('pro1.purchaseOrderCustomerNumber')\
			,col('pro1.purchaseOrderVendorNumber')\
			,col('pro1.purchaseOrderSupplingVendorNumber')\
			,col('pro1.purchaseOrderReferenceHeader'),col('pro1.purchaseOrderReferenceLineItem')\
			,col('pro1.purchaseOrderReferenceFinYear')\
			,col('pro1.purchaseOrderReferenceAdditionalFilter')\
			,col('pro1.purchaseOrderReferenceAdditionalFilterPlaceHolder')\
			,col('pro1.purchaseOrderReferenceFinYear2')\
			,col('pro1.purchaseOrderReferenceAdditionalFilter2')\
			,col('pro1.purchaseOrderReferenceAdditionalFilterPlaceHolder2')\
			,col('pro1.purchaseOrderReferenceHeader3')\
			,col('pro1.purchaseOrderReferenceLineItem3')\
			,col('pro1.purchaseOrderReferenceFinYear3')\
			,col('pro1.purchaseOrderReferenceAdditionalFilter3')\
			,col('pro1.purchaseOrderReferenceAdditionalFilterPlaceHolder3')\
			,col('pro1.purchaseOrderReferenceHeader4')\
			,col('pro1.purchaseOrderReferenceLineItem4')\
			,col('pro1.purchaseOrderReferenceFinYear4')\
			,col('pro1.purchaseOrderReferenceAdditionalFilter4')\
			,col('pro1.purchaseOrderReferenceAdditionalFilterPlaceHolder4')\
			,col('pro1.purchaseOrderReferenceHeader5')\
			,col('pro1.purchaseOrderReferenceLineItem5')\
			,col('pro1.purchaseOrderReferenceFinYear5')\
			,col('pro1.purchaseOrderReferenceAdditionalFilter5')\
			,col('pro1.purchaseOrderReferenceAdditionalFilterPlaceHolder5')\
			,col('pro1.purchaseOrderReferenceLineItem6')\
			,col('pro1.purchaseOrderReferenceFinYear6')\
			,col('pro1.purchaseOrderReferenceAdditionalFilter6')\
			,col('pro1.purchaseOrderReferenceAdditionalFilterPlaceHolder6')\
			,col('pro1.purchaseOrderType')\
			,col('pro1.purchaseOrderTypeDescription')\
			,col('pro1.purchaseOrderSubType')\
			,col('pro1.purchaseOrderCategory')\
			,col('pro1.purchaseOrderCategoryDescription')\
			,col('pro1.purchaseOrderTargetQuantityInOrderUnit')\
			,col('pro1.purchaseOrderQuantity')\
			,col('pro1.purchaseOrderMeasureUnit')\
			,col('pro1.purchaseOrderTargetQuantityInStockKeepingUnit')\
			,col('pro1.purchaseOrderQuantitySKU')\
			,col('pro1.purchaseOrderSKU')\
			,col('pro1.purchaseOrderTargetQuantityInOrderPriceUnit')\
			,col('pro1.purchaseOrderQuantityPriceUnit')\
			,col('pro1.purchaseOrderPurchasingPriceUnit')\
			,col('pro1.purchaseOrderAmountDC')\
			,col('pro1.purchaseOrderAmountLC')\
			,col('pro1.purchaseOrderUnitPriceSULC')\
			,col('pro1.purchaseOrderUnitPriceSKULC')\
			,col('pro1.purchaseOrderDocumentCurrency')\
			,col('pro1.purchaseOrderLocalCurrency')\
			,col('pro1.purchaseOrderExchangeRate')\
			,col('pro1.isExchangeRateFixed')\
			,col('pro1.purchaseOrderNumeratorOrderUnitToBaseUnit')\
			,col('pro1.purchaseOrderDenominatorOrderUnitToBaseUnit')\
			,col('pro1.purchaseOrderNumeratorOrderPriceUnitToOrderUnit')\
			,col('pro1.purchaseOrderDenominatorOrderPriceUnitToOrderUnit')\
			,col('pro1.purchaseOrderNetPriceValueDC')\
			,col('pro1.purchaseOrderNetPriceValueLC')\
			,col('pro1.purchaseOrderPriceUnit')\
			,col('pro1.purchaseOrderNetPriceInOrderUnit')\
			,col('pro1.purchaseOrderNetPriceInOrderPriceUnit')\
			,col('pro1.purchaseOrderNetPriceInStockKeepingUnit')\
			,col('pro1.purchaseOrderNetPriceOrderUnit')\
			,col('pro1.purchaseOrderNetPriceOrderPriceUnit')\
			,col('pro1.purchaseOrderNetPriceStockKeepingUnit')\
			,col('pro1.purchaseOrderTargetAmountDC')\
			,col('pro1.purchaseOrderTargetAmountLC')\
			,col('pro1.purchaseOrderGrossAmountDC')\
			,col('pro1.purchaseOrderGrossAmountLC')\
            ,col('pro1.purchaseOrderAmountReducedDC')\
			,col('pro1.purchaseOrderAmountReducedLC')\
			,col('pro1.isPriceEstimated')\
			,col('pro1.purchaseOrderEffectiveValue')\
			,col('pro1.purchaseOrderOverdeliveryToleranceLimit')\
			,col('pro1.purchaseOrderUnderdeliveryToleranceLimit')\
			,col('pro1.isUnlimitedOverdeliveryAllowed')\
			,col('pro1.purchaseOrderControlType')\
			,col('pro1.purchaseOrderControlTypeDescription')\
			,col('pro1.purchaseOrderProcessingStatus')\
			,col('pro1.purchaseOrderProcessingStatusDescription')\
			,col('pro1.purchaseOrderPaymentTerm')\
			,col('pro1.purchaseOrderCashDiscountDayForPayment1')\
			,col('pro1.purchaseOrderCashDiscountDayForPayment2')\
			,col('pro1.purchaseOrderDayForNetPayment')\
			,col('pro1.purchaseOrderCashDiscountPercentageForPayment1')\
			,col('pro1.purchaseOrderCashDiscountPercentageForPayment2')\
			,col('pro1.purchaseOrderIncotermsPart1')\
			,col('pro1.purchaseOrderIncotermsPart2')\
			,col('pro1.purchaseOrderConditionDocumentNumber')\
			,col('pro1.purchaseOrderStartOfValidityPeriod')\
			,col('pro1.purchaseOrderEndOfValidityPeriod')\
			,col('pro1.purchaseOrderBidQuotationSubmissionDeadline')\
			,col('pro1.purchaseOrderQuotationBindingPeriod')\
			,col('pro1.purchaseOrderWarrantyDate')\
			,col('pro1.purchaseOrderSubmissionDate')\
			,col('pro1.purchaseOrderHigherLevelItem')\
			,col('pro1.isDeleted'),col('pro1.isCompleted')\
            ,col('pro1.purchaseOrderReleaseGroup'),col('pro1.purchaseOrderReleaseStrategy')\
			,col('pro1.purchaseOrderReleaseStatus'),col('pro1.isReleaseComplettyEffected')\
            ,col('pro1.purchaseOrderProcessingState')\
			,col('pro1.purchaseOrderProcessingStateDescription')\
			,col('pro1.purchaseOrderVersionNumber')\
			,col('pro1.purchaseOrderChangeDate')\
			,col('pro1.isRejected'),col('isDeliveryCompleted'),col('pro1.purchaseOrderPlannedDeliveryTime')\
			,col('pro1.isGoodsReceiptExpected'),col('pro1.isGoodsReceiptNonValuated')\
            ,col('pro1.isGoodsReceiptBasedInvoiceVerificationActive')\
			,col('pro1.isInvoiceReceiptExpected')\
			,col('pro1.isFinalInvoicePosted')\
			,col('pro1.isAutomaticSettlement')\
			,col('pro1.purchaseOrderAccountAssignmentCategory')\
			,col('pro1.purchaseOrderConsumptionPosting')\
			,col('pro1.purchaseOrderConsumptionPostingDescription')\
			,col('pro1.purchaseOrderSourceERPSystemID')\
			,col('pro1.purchaseOrderSourceERPTable')\
			,col('pro1.purchaseOrderDocumentTypeClientID')\
            ,when(col('bdt15.targetSystemValue').isNull(),col('mst1.processingStatus'))\
                            .otherwise(col('bdt15.targetSystemValue')).alias('purchaseOrderMessageStatus')\
            ,when(col('bdt15.targetSystemValueDescription').isNull(),lit('Message Processing Status'))\
                            .otherwise(col('bdt15.targetSystemValueDescription')).alias('purchaseOrderMessageStatusDescription')\
            ,when(col('bdt16.targetSystemValue').isNull(),col('mst1.transmissionMedium'))\
                            .otherwise(col('bdt16.targetSystemValue')).alias('purchaseOrderMessgeType')\
            ,when(col('bdt16.targetSystemValueDescription').isNull(),lit('Message Transmission Medium'))\
                            .otherwise(col('bdt16.targetSystemValueDescription')).alias('purchaseOrderMessgeTypeDescription'))

#update 3
    v_purchaseOrderUnitPriceSUDC = when(col('pro1.purchaseOrderQuantity')>0,\
    				        (col('pro1.purchaseOrderAmountDC')/col('pro1.purchaseOrderQuantity'))).otherwise(lit(0))
    v_purchaseOrderUnitPriceSKUDC = when(col('pro1.purchaseOrderQuantitySKU')>0,\
    						(col('pro1.purchaseOrderAmountDC')/(col('pro1.purchaseOrderQuantitySKU')))).otherwise(lit(0))
    
    ptp_L1_TD_PurchaseOrder = ptp_L1_TD_PurchaseOrder.withColumn("purchaseOrderUnitPriceSUDC",lit(v_purchaseOrderUnitPriceSUDC))\
                              .withColumn("purchaseOrderUnitPriceSKUDC",lit(v_purchaseOrderUnitPriceSKUDC))

    ##Call the currency Conversion Function
    ls_curcyResult=currencyConversion_L1_TD_PurchaseOrder(ptp_L1_TD_PurchaseOrder)
    if(ls_curcyResult[0] == LOG_EXECUTION_STATUS.FAILED): 
      raise Exception(ls_curcyResult[1])
    ptp_L1_TD_PurchaseOrder=ls_curcyResult[2] 
    
    ptp_L1_TD_PurchaseOrder =  objDataTransformation.gen_convertToCDMandCache \
              (ptp_L1_TD_PurchaseOrder,'ptp','L1_TD_PurchaseOrder',targetPath=gl_CDMLayer1Path)
    
       
    executionStatus = "L1_TD_PurchaseOrder populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

  
  
#L1_TD_PurchaseOrder
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce,abs

def currencyConversion_L1_TD_PurchaseOrder(df_fromSource):
 
  objGenHelper = gen_genericHelper()
  sourceschema='ptp'
  sourcetable='L1_TD_PurchaseOrder'
  sourceCompanyCode='purchaseOrderCompanyCode'
  sourceCurrencyField='purchaseOrderDocumentCurrency'
  df_out=None
  executionStatus="Currency Conversion Validation is passed"
  try: 
    #ls_SourceDate=currencyConversionSourceDate_get(sourceschemaName="otc",sourcetableName="L1_TD_Billing",overrideWithDate='2020-01-01')
    lstValidationStatus=currencyConversionSourceDate_get(sourceschemaName=sourceschema,sourcetableName=sourcetable,overrideWithDate=None)

    if(lstValidationStatus[0] == LOG_EXECUTION_STATUS.FAILED): 
       raise Exception(lstValidationStatus[1])

    v_sourceDate=lstValidationStatus[2]
    col_RowID= F.row_number().over(Window.orderBy(lit('')))

    df_source=df_fromSource.alias('src')\
       .drop(*('purchaseOrderAmountLC', 'purchaseOrderNetPriceValueLC'\
                            ,'purchaseOrderTargetAmountLC','purchaseOrderGrossAmountLC'\
                            ,'purchaseOrderAmountReducedLC' ,'purchaseOrderUnitPriceSULC'\
                             ,'purchaseOrderUnitPriceSKULC'\
                            ))\
      .select(col('src.*'))\
      .withColumn("sourceDate", expr(v_sourceDate))

    targetTmpPath = gl_layer0Temp_temp + "GCC_SRC_" + sourcetable +".delta"

    objGenHelper.gen_writeToFile_perfom(df_source,targetTmpPath,mode='overwrite',isMergeSchema = True) 
    df_source=objGenHelper.gen_readFromFile_perform(targetTmpPath)   

    #1
    #purchaseOrderAmountDC
    sourceAmountField='purchaseOrderAmountDC'
    targetAmountField='purchaseOrderAmountLC'
    lst_23CurrencyValueField=currencyConversionSecondThirdCurrencyValue_get(sourceschemaName=sourceschema\
                                                                          ,sourcetableName=sourcetable\
                                                                          ,targetAmountField=targetAmountField
                                                                         )
    secondCurrencyValue=lst_23CurrencyValueField[0]
    thirdCurrencyValue=lst_23CurrencyValueField[1]

    df_GenericCurrencyConversion_col1=df_source\
        .select(col(sourceCompanyCode).alias('sourceCompanyCode')\
             ,col('sourceDate').alias('sourceDate')\
             ,col(sourceAmountField).alias('sourceAmount')\
             ,lit(secondCurrencyValue).alias('secondCurrencyValue')\
             ,lit(thirdCurrencyValue).alias('thirdCurrencyValue')\
             ,col(sourceCurrencyField).alias('sourceCurrency')\
             ,lit(sourceAmountField).alias('sourceColName')\
             ).distinct()

    #df_GenericCurrencyConversion_col1.display()

    #2
    #purchaseOrderNetPriceValueDC
    sourceAmountField='purchaseOrderNetPriceValueDC'
    targetAmountField='purchaseOrderNetPriceValueLC'
    lst_23CurrencyValueField=currencyConversionSecondThirdCurrencyValue_get(sourceschemaName=sourceschema\
                                                                          ,sourcetableName=sourcetable\
                                                                          ,targetAmountField=targetAmountField
                                                                         )
    secondCurrencyValue=lst_23CurrencyValueField[0]
    thirdCurrencyValue=lst_23CurrencyValueField[1]

    df_GenericCurrencyConversion_col2=df_source\
        .select(col(sourceCompanyCode).alias('sourceCompanyCode')\
             ,col('sourceDate').alias('sourceDate')\
             ,col(sourceAmountField).alias('sourceAmount')\
             ,lit(secondCurrencyValue).alias('secondCurrencyValue')\
             ,lit(thirdCurrencyValue).alias('thirdCurrencyValue')\
             ,col(sourceCurrencyField).alias('sourceCurrency')\
             ,lit(sourceAmountField).alias('sourceColName')\
             ).distinct()

    #df_GenericCurrencyConversion_col2.display()

    #3
    #purchaseOrderTargetAmountDC
    sourceAmountField='purchaseOrderTargetAmountDC'
    targetAmountField='purchaseOrderTargetAmountLC'
    lst_23CurrencyValueField=currencyConversionSecondThirdCurrencyValue_get(sourceschemaName=sourceschema\
                                                                          ,sourcetableName=sourcetable\
                                                                          ,targetAmountField=targetAmountField
                                                                         )
    secondCurrencyValue=lst_23CurrencyValueField[0]
    thirdCurrencyValue=lst_23CurrencyValueField[1]

    df_GenericCurrencyConversion_col3=df_source\
        .select(col(sourceCompanyCode).alias('sourceCompanyCode')\
             ,col('sourceDate').alias('sourceDate')\
             ,col(sourceAmountField).alias('sourceAmount')\
             ,lit(secondCurrencyValue).alias('secondCurrencyValue')\
             ,lit(thirdCurrencyValue).alias('thirdCurrencyValue')\
             ,col(sourceCurrencyField).alias('sourceCurrency')\
             ,lit(sourceAmountField).alias('sourceColName')\
             ).distinct()

    #df_GenericCurrencyConversion_col3.display()

    #4
    #purchaseOrderGrossAmountDC
    sourceAmountField='purchaseOrderGrossAmountDC'
    targetAmountField='purchaseOrderGrossAmountLC'
    lst_23CurrencyValueField=currencyConversionSecondThirdCurrencyValue_get(sourceschemaName=sourceschema\
                                                                          ,sourcetableName=sourcetable\
                                                                          ,targetAmountField=targetAmountField
                                                                         )
    secondCurrencyValue=lst_23CurrencyValueField[0]
    thirdCurrencyValue=lst_23CurrencyValueField[1]

    df_GenericCurrencyConversion_col4=df_source\
      .select(col(sourceCompanyCode).alias('sourceCompanyCode')\
             ,col('sourceDate').alias('sourceDate')\
             ,col(sourceAmountField).alias('sourceAmount')\
             ,lit(secondCurrencyValue).alias('secondCurrencyValue')\
             ,lit(thirdCurrencyValue).alias('thirdCurrencyValue')\
             ,col(sourceCurrencyField).alias('sourceCurrency')\
             ,lit(sourceAmountField).alias('sourceColName')\
             ).distinct()

    #df_GenericCurrencyConversion_col4.display()
    #5
    #purchaseOrderAmountReducedDC
    sourceAmountField='purchaseOrderAmountReducedDC'
    targetAmountField='purchaseOrderAmountReducedLC'
    lst_23CurrencyValueField=currencyConversionSecondThirdCurrencyValue_get(sourceschemaName=sourceschema\
                                                                          ,sourcetableName=sourcetable\
                                                                          ,targetAmountField=targetAmountField
                                                                         )
    secondCurrencyValue=lst_23CurrencyValueField[0]
    thirdCurrencyValue=lst_23CurrencyValueField[1]

    df_GenericCurrencyConversion_col5=df_source\
        .select(col(sourceCompanyCode).alias('sourceCompanyCode')\
             ,col('sourceDate').alias('sourceDate')\
             ,col(sourceAmountField).alias('sourceAmount')\
             ,lit(secondCurrencyValue).alias('secondCurrencyValue')\
             ,lit(thirdCurrencyValue).alias('thirdCurrencyValue')\
             ,col(sourceCurrencyField).alias('sourceCurrency')\
             ,lit(sourceAmountField).alias('sourceColName')\
             ).distinct()

    #df_GenericCurrencyConversion_col5.display()

    #6
    #purchaseOrderUnitPriceSUDC
    sourceAmountField='purchaseOrderUnitPriceSUDC'
    targetAmountField='purchaseOrderUnitPriceSULC'
    lst_23CurrencyValueField=currencyConversionSecondThirdCurrencyValue_get(sourceschemaName=sourceschema\
                                                                          ,sourcetableName=sourcetable\
                                                                          ,targetAmountField=targetAmountField
                                                                         )
    secondCurrencyValue=lst_23CurrencyValueField[0]
    thirdCurrencyValue=lst_23CurrencyValueField[1]

    df_GenericCurrencyConversion_col6=df_source\
        .select(col(sourceCompanyCode).alias('sourceCompanyCode')\
             ,col('sourceDate').alias('sourceDate')\
             ,col(sourceAmountField).alias('sourceAmount')\
             ,lit(secondCurrencyValue).alias('secondCurrencyValue')\
             ,lit(thirdCurrencyValue).alias('thirdCurrencyValue')\
             ,col(sourceCurrencyField).alias('sourceCurrency')\
             ,lit(sourceAmountField).alias('sourceColName')\
             ).distinct()

    #df_GenericCurrencyConversion_col6.display()
    
    #7
    #purchaseOrderUnitPriceSKUDC
    sourceAmountField='purchaseOrderUnitPriceSKUDC'
    targetAmountField='purchaseOrderUnitPriceSKULC'
    lst_23CurrencyValueField=currencyConversionSecondThirdCurrencyValue_get(sourceschemaName=sourceschema\
                                                                          ,sourcetableName=sourcetable\
                                                                          ,targetAmountField=targetAmountField
                                                                         )
    secondCurrencyValue=lst_23CurrencyValueField[0]
    thirdCurrencyValue=lst_23CurrencyValueField[1]

    df_GenericCurrencyConversion_col7=df_source\
        .select(col(sourceCompanyCode).alias('sourceCompanyCode')\
             ,col('sourceDate').alias('sourceDate')\
             ,col(sourceAmountField).alias('sourceAmount')\
             ,lit(secondCurrencyValue).alias('secondCurrencyValue')\
             ,lit(thirdCurrencyValue).alias('thirdCurrencyValue')\
             ,col(sourceCurrencyField).alias('sourceCurrency')\
             ,lit(sourceAmountField).alias('sourceColName')\
             ).distinct()

    #df_GenericCurrencyConversion_col7.display()
    
    df_GenericCurrencyConversion=df_GenericCurrencyConversion_col1\
                 .unionAll(df_GenericCurrencyConversion_col2)\
                 .unionAll(df_GenericCurrencyConversion_col3)\
                 .unionAll(df_GenericCurrencyConversion_col4)\
                 .unionAll(df_GenericCurrencyConversion_col5)\
                 .unionAll(df_GenericCurrencyConversion_col6)\
                 .unionAll(df_GenericCurrencyConversion_col7)

    targetPath = gl_layer0Temp_temp + "GCC_TMP_" + sourcetable +".delta"
    objGenHelper.gen_writeToFile_perfom(df_GenericCurrencyConversion,targetPath,mode='overwrite',isMergeSchema = True) 
    df_tmp_GenericCurrencyConversion=objGenHelper.gen_readFromFile_perform(targetPath) 
    # add secondCurrencyValue/thirdCurrencyValue column by checking its existence
    if 'secondCurrencyValue' not in df_tmp_GenericCurrencyConversion.columns:
      df_tmp_GenericCurrencyConversion=df_tmp_GenericCurrencyConversion.withColumn("secondCurrencyValue", lit(None))
    if 'thirdCurrencyValue' not in df_tmp_GenericCurrencyConversion.columns:
      df_tmp_GenericCurrencyConversion=df_tmp_GenericCurrencyConversion.withColumn("thirdCurrencyValue", lit(None))

    ##Call the currency Conversion Function

    ls_gen_currencyConversion_result =gen_currencyConversion_perform(df_tmp_GenericCurrencyConversion,sourcetable = sourcetable \
                                    ,isToBeConvertedToRC=0,overrideWithDate=None)
    
    df_result=ls_gen_currencyConversion_result[2]  

    df_out=df_source.alias('a')\
      .join(df_result.alias('b').filter(col('sourceColName')=='purchaseOrderAmountDC')\
           ,((col("a.purchaseOrderCompanyCode").eqNullSafe(col("b.sourceCompanyCode")))\
             & (col("a.purchaseOrderDocumentCurrency").eqNullSafe(col("b.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("b.sourceDate")))\
             & (col("a.purchaseOrderAmountDC").eqNullSafe(col("b.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('c').filter(col('sourceColName')=='purchaseOrderNetPriceValueDC')\
           ,((col("a.purchaseOrderCompanyCode").eqNullSafe(col("c.sourceCompanyCode")))\
             & (col("a.purchaseOrderDocumentCurrency").eqNullSafe(col("c.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("c.sourceDate")))\
             & (col("a.purchaseOrderNetPriceValueDC").eqNullSafe(col("c.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('d').filter(col('sourceColName')=='purchaseOrderTargetAmountDC')\
           ,((col("a.purchaseOrderCompanyCode").eqNullSafe(col("d.sourceCompanyCode")))\
             & (col("a.purchaseOrderDocumentCurrency").eqNullSafe(col("d.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("d.sourceDate")))\
             & (col("a.purchaseOrderTargetAmountDC").eqNullSafe(col("d.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('e').filter(col('sourceColName')=='purchaseOrderGrossAmountDC')\
           ,((col("a.purchaseOrderCompanyCode").eqNullSafe(col("e.sourceCompanyCode")))\
             & (col("a.purchaseOrderDocumentCurrency").eqNullSafe(col("e.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("e.sourceDate")))\
             & (col("a.purchaseOrderGrossAmountDC").eqNullSafe(col("e.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('f').filter(col('sourceColName')=='purchaseOrderAmountReducedDC')\
           ,((col("a.purchaseOrderCompanyCode").eqNullSafe(col("f.sourceCompanyCode")))\
             & (col("a.purchaseOrderDocumentCurrency").eqNullSafe(col("f.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("f.sourceDate")))\
             & (col("a.purchaseOrderAmountReducedDC").eqNullSafe(col("f.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('g').filter(col('sourceColName')=='purchaseOrderUnitPriceSUDC')\
           ,((col("a.purchaseOrderCompanyCode").eqNullSafe(col("g.sourceCompanyCode")))\
             & (col("a.purchaseOrderDocumentCurrency").eqNullSafe(col("g.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("g.sourceDate")))\
             & (col("a.purchaseOrderUnitPriceSUDC").eqNullSafe(col("g.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('h').filter(col('sourceColName')=='purchaseOrderUnitPriceSKUDC')\
           ,((col("a.purchaseOrderCompanyCode").eqNullSafe(col("h.sourceCompanyCode")))\
             & (col("a.purchaseOrderDocumentCurrency").eqNullSafe(col("h.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("h.sourceDate")))\
             & (col("a.purchaseOrderUnitPriceSKUDC").eqNullSafe(col("h.sourceAmount")))\
            ),"left")\
      .select(col('a.*')\
             ,col('b.targetAmount').alias('purchaseOrderAmountLC')\
             ,col('c.targetAmount').alias('purchaseOrderNetPriceValueLC')\
             ,col('d.targetAmount').alias('purchaseOrderTargetAmountLC')\
             ,col('e.targetAmount').alias('purchaseOrderGrossAmountLC')\
             ,col('f.targetAmount').alias('purchaseOrderAmountReducedLC')\
             ,col('g.targetAmount').alias('purchaseOrderUnitPriceSULC')\
             ,col('h.targetAmount').alias('purchaseOrderUnitPriceSKULC')\
          ,col('a.sourceDate'))
  
   
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus,df_out]     

  except Exception as err:        
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus,df_out]   
