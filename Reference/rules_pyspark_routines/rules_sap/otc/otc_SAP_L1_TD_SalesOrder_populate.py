# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce,abs

def otc_SAP_L1_TD_SalesOrder_populate(): 
  try:    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global otc_L1_TD_SalesOrder
    
    clientProcessID_OTC=1
    erpSAPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))
    
    knw_vw_LK_GD_ClientKPMGDocumentType = gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType']
    
    otc_L1_TD_SalesOrder_tmp = erp_VBAP.alias('vbap_erp')\
      .join (erp_VBAK.alias('vbak_erp')\
             ,((col("vbap_erp.MANDT") == col("vbak_erp.MANDT"))\
               & (col("vbap_erp.VBELN") == col("vbak_erp.VBELN"))\
              ),"inner")\
      .join (erp_VBPA.alias('vbpa_pos')\
             ,((col("vbpa_pos.MANDT").eqNullSafe(col("vbak_erp.MANDT")))\
               & (col("vbpa_pos.VBELN").eqNullSafe(col("vbak_erp.VBELN")))\
               & (col("vbpa_pos.POSNR").eqNullSafe(col("vbap_erp.POSNR")))\
               & (col("vbpa_pos.PARVW").eqNullSafe(lit('RG')))\
              ),"left")\
      .join (erp_VBPA.alias('vbpa_kopf')\
             ,((col("vbpa_kopf.MANDT").eqNullSafe(col("vbak_erp.MANDT")))\
               & (col("vbpa_kopf.VBELN").eqNullSafe(col("vbak_erp.VBELN")))\
               & (col("vbpa_kopf.POSNR").eqNullSafe(lit('000000')))\
               & (col("vbpa_kopf.PARVW").eqNullSafe(lit('RG') ))\
              ),"left")\
      .join (knw_vw_LK_GD_ClientKPMGDocumentType.alias('oDocType')\
             ,((col("vbak_erp.VBTYP").eqNullSafe(col("oDocType.documentTypeClient")))\
               & (col("oDocType.documentTypeClientProcessID").eqNullSafe(lit(clientProcessID_OTC)))\
               & (F.when(F.col("vbap_erp.SHKZG")==lit('X'),lit('1')).otherwise(lit('0'))\
                  .eqNullSafe(col("oDocType.isReversed")))\
               & (col("oDocType.ERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
              ),"left")\
       .join (knw_vw_LK_GD_ClientKPMGDocumentType.alias('oDocType2')\
             ,((col("vbak_erp.VBTYP").eqNullSafe(col("oDocType2.documentTypeClient")))\
               & (col("oDocType2.documentTypeClientProcessID").eqNullSafe(lit(clientProcessID_OTC)))\
               & (col("oDocType2.isReversed").eqNullSafe(lit(0)))\
               & (col("oDocType.ERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
              ),"left")\
       .join (knw_vw_LK_GD_ClientKPMGDocumentType.alias('oPredDocType')\
             ,((col("vbap_erp.VGTYP").eqNullSafe(col("oPredDocType.documentTypeClient")))\
               & (col("oPredDocType.documentTypeClientProcessID").eqNullSafe(lit(clientProcessID_OTC)))\
               & (col("oPredDocType.isReversed").eqNullSafe(lit(0)))\
               & (col("oPredDocType.ERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
              ),"left")\
        .join (erp_TVKO.alias('tvko_erp')\
             ,((col("vbak_erp.MANDT").eqNullSafe(col("tvko_erp.MANDT")))\
               & (col("vbak_erp.VKORG").eqNullSafe(col("tvko_erp.VKORG")))\
              ),"left")\
        .join (erp_T001.alias('t001_erp')\
             ,((col("vbak_erp.MANDT").eqNullSafe(col("t001_erp.MANDT")))\
               & (col("tvko_erp.BUKRS").eqNullSafe(col("t001_erp.BUKRS")))\
              ),"left")\
        .select(col('vbap_erp.VBELN').alias('orderingDocumentNumber')\
            ,col('vbap_erp.POSNR').alias('orderingDocumentLineItem')\
            ,col('vbap_erp.UEPOS').alias('orderingDocumentLineItemHigherBOM')\
            ,col('vbak_erp.VBTYP').alias('orderingDocumentTypeClient')\
            ,coalesce(col('oDocType.documentTypeClientID'),col('oDocType2.documentTypeClientID'))\
                .alias('orderingDocumentTypeClientID')\
            ,coalesce(col('oDocType.documentTypeClientDescription'),col('oDocType2.documentTypeClientDescription'))\
                .alias('orderingDocumentTypeClientDescription')\
            ,coalesce(col('oDocType.documentTypeKPMG'),col('oDocType2.documentTypeKPMG'))\
                .alias('orderingDocumentTypeKPMG')\
            ,coalesce(col('oDocType.documentTypeKPMGInterpreted'),col('oDocType2.documentTypeKPMGInterpreted'))\
                .alias('documentTypeKPMGInterpreted')\
            ,coalesce(col('oDocType.interpretedDocumentTypeClientID'),col('oDocType2.interpretedDocumentTypeClientID'))\
                .alias('interpretedDocumentTypeClientID')\
            ,coalesce(col('oDocType.documentTypeKPMGShort'),col('oDocType2.documentTypeKPMGShort'))\
                .alias('orderingDocumentTypeKPMGShort')\
            ,coalesce(col('oDocType.KPMGInterpretedCalculationRule'),col('oDocType2.KPMGInterpretedCalculationRule'))\
                .alias('_KPMGInterpretedCalculationRule')\
            ,when(col('VBTYP').isin([":","C","I","E","F","H","A","B"])\
                  ,lit('1'))\
               .when(col('VBTYP').isin(["K","L"]),lit('2'))\
               .when(col('VBTYP').isin(["G"]),lit('3')).alias('VBTYP_1')\
            ,when(((col('vbap_erp.KWMENG') > 0 ) & (col('vbap_erp.UMVKN') != 0) )\
                  ,(col('vbap_erp.KWMENG')*col('vbap_erp.UMVKZ')/col('vbap_erp.UMVKN'))\
                 ).otherwise(0).alias('KWMENG_UMVKZ_UMVKN')\
            ,when((col('vbap_erp.UMZIN') != 0)\
                  ,(col('vbap_erp.ZMENG')*col('vbap_erp.UMZIZ')/col('vbap_erp.UMZIN'))\
                 ).otherwise(0).alias('ZMENG_UMZIZ_UMZIN')\
            ,col('vbap_erp.VBELV').alias('orderingDocumentOriginatingDocumentNumber')\
            ,col('vbap_erp.POSNV').alias('orderingDocumentOriginatingDocumentLineItem')\
            ,col('vbap_erp.VGBEL').alias('orderingDocumentPredecessorDocumentNumber')\
            ,col('vbap_erp.VGPOS').alias('orderingDocumentPredecessorDocumentLineItem')\
            ,col('vbap_erp.VGTYP').alias('orderingDocumentPredecessorDocumentTypeClient')\
            ,col('oPredDocType.documentTypeClientDescription')\
                  .alias('orderingDocumentPredecessordocumentTypeClientDescription')\
            ,col('oPredDocType.documentTypeKPMG').alias('orderingDocumentPredecessorDocumentTypeKPMG')\
            ,col('oPredDocType.documentTypeKPMGShort')\
                  .alias('orderingDocumentPredecessorDocumentTypeKPMGShort')\
            ,col('vbak_erp.VGBEL').alias('orderingDocumentPredecessorDocumentNumberHeader')\
            ,col('vbak_erp.VGTYP').alias('orderingDocumentPredecessorDocumentTypeHeader')\
            ,col('vbap_erp.MATNR').alias('orderingDocumentProduct')\
            ,col('vbap_erp.MATNR').alias('orderingDocumentProductArtificialID')\
            ,col('vbak_erp.KUNNR').alias('orderingDocumentCustomerSoldTo')\
            ,col('vbap_erp.MEINS').alias('orderingDocumentSKU')\
            ,col('vbap_erp.KWMENG').alias('orderingDocumentQuantitySUSource')\
            ,col('vbap_erp.UMVKN')\
            ,col('vbap_erp.UMVKZ')\
            ,col('vbap_erp.VRKME').alias('orderingDocumentSU')\
            ,lit(0).alias('orderingDocumentUnitPriceSULC')\
            ,lit(0).alias('orderingDocumentUnitPriceSKULC')\
            ,col('vbap_erp.CMPRE').alias('orderingDocumentItemCreditPrice')\
            ,coalesce(col('vbap_erp.CMPRE_FLT'),col('vbap_erp.CMPRE'))\
                .alias('orderingDocumentItemCreditPrice2')\
            ,col('vbap_erp.CMPRE_FLT')\
            ,col('vbap_erp.CMPRE')\
            ,lit(0).alias('orderingDocumentConditionUnit')\
            ,col('vbap_erp.KPEIN').alias('orderingDocumentConditionPricingUnit')\
            ,col('vbap_erp.NETWR').alias('orderingDocumentAmountDCSource')\
            ,lit(0).alias('orderingDocumentAmountLCSource')\
            ,lit(0).alias('orderingDocumentAmountLC')\
            ,when(col('vbap_erp.ZIEME').isNull(), col('vbap_erp.MEINS') ) \
              .when(col('vbap_erp.ZIEME')=='', col('vbap_erp.MEINS') ) \
              .otherwise(col('vbap_erp.ZIEME')).alias('orderingDocumentTargetQuantityUnit')\
          ,col('vbap_erp.ZMENG').alias('orderingDocumentTargetQuantitySUSource')\
          ,col('vbap_erp.ZWERT').alias('orderingDocumentTargetAmountDCSource')\
          ,when(col('vbap_erp.SHKZG')==lit('X'),lit(1)).otherwise(lit(0))\
                  .alias('orderingDocumentReturnsItem')\
          ,when(col('vbap_erp.UEBTK')==lit('X'),lit(1)).otherwise(lit(0))\
                  .alias('unlimitedOverdeliveryAllowed')\
          ,col('vbap_erp.UEBTO').alias('orderingDocumentoverDeliveryToleranceLimit')\
          ,col('vbap_erp.UNTTO').alias('orderingDocumentUnderdeliveryToleranceLimit')\
          ,col('vbak_erp.ERNAM').alias('orderingDocumentUser')\
          ,col('vbak_erp.AUDAT').alias('orderingDocumentDate')\
          ,col('t001_erp.BUKRS').alias('orderingDocumentCompanyCode')\
          ,col('vbak_erp.WAERK').alias('orderingDocumentCurrency')\
          ,col('t001_erp.WAERS').alias('orderingDocumentLocalCurrency')\
          ,col('vbak_erp.ERDAT').alias('orderingDocumentDateOfEntry')\
          ,concat(lit('000000'),col('vbak_erp.ERZET')).alias('orderingDocumentTimeOfEntry_')\
          ,col('vbak_erp.ERZET')\
          ,col('vbap_erp.NETPR')\
          ,lit(0).alias('orderingDocumentAmountLCEFFSource')\
          ,lit(0).alias('orderingDocumentAmountLCEFF')\
          ,lit(0).alias('ordringDocumentTargetQuantityisDerived')\
          ,col('vbap_erp.ERLRE')\
          ,col('oDocType.KPMGInterpretedCalculationRule')\
          ,col('vbap_erp.UMZIN')\
          ,col('vbap_erp.UMZIZ')\
          ,lit(current_date()).alias('recordCreated')\
          ,col('vbap_erp.MANDT').alias('orderingDocumentERPclient')\
          ,col('vbak_erp.VKORG').alias('orderingDocumentSalesOrganization')\
          ,col('vbap_erp.KLMENG').alias('orderingDocumentQuantityBaseUnit')\
          ,col('vbak_erp.KKBER').alias('orderingDocumentCreditControlArea')\
          ,coalesce(col('vbpa_pos.KUNNR'),col('vbpa_kopf.KUNNR')).alias('orderingDocumentPayer')\
        )
    otc_L1_TD_SalesOrder_tmp = otc_L1_TD_SalesOrder_tmp\
          .withColumn("orderingDocumentTimeOfEntry",F.concat(F.expr("substring(orderingDocumentTimeOfEntry_,1,2)"),lit(':')\
              ,F.expr("substring(orderingDocumentTimeOfEntry_,3,2)"),F.lit(':'),F.expr("substring(orderingDocumentTimeOfEntry_,5,2)")))
    
    #orderingDocumentTypeKPMGSubType
    v_orderingDocumentTypeKPMGSubType=when(col('orderingDocumentTypeClient')=='G'\
       ,when(((col('orderingDocumentAmountDCSource') != 0 ) \
              & (col('orderingDocumentTargetQuantitySUSource') == 1) \
              & (col('orderingDocumentTargetAmountDCSource') != 0))\
            ,lit('Value Contract')\
           )\
        .when(((col('orderingDocumentAmountDCSource') != 0 ) \
               & (col('orderingDocumentTargetQuantitySUSource') != 0) \
               & (col('orderingDocumentTargetAmountDCSource') == 0)) \
            ,lit('Quantity Contract')\
           )\
        .otherwise(lit('Unknown Contract Type')))\
      .otherwise(lit(''))

    #orderingDocumentQuantitySU
    v_orderingDocumentQuantitySU=col('orderingDocumentQuantitySUSource')*col('_KPMGInterpretedCalculationRule')

    #orderingDocumentQuantitySKUSource
    v_orderingDocumentQuantitySKUSource =when(col('VBTYP_1')=='1',col('KWMENG_UMVKZ_UMVKN'))\
            .when(col('VBTYP_1')=='2'\
              ,when(col('orderingDocumentTargetQuantitySUSource') !=0 ,col('ZMENG_UMZIZ_UMZIN')).otherwise(0))\
            .when(col('VBTYP_1')=='3',col('ZMENG_UMZIZ_UMZIN'))\
            .otherwise(0)

    #orderingDocumentQuantitySKU
    v_orderingDocumentQuantitySKU=when(col('VBTYP_1')=='1',\
                    (col('KWMENG_UMVKZ_UMVKN')*col('_KPMGInterpretedCalculationRule')))\
          .when(col('VBTYP_1')=='2'\
              ,when(col('orderingDocumentTargetQuantitySUSource') !=0 ,\
                    (col('ZMENG_UMZIZ_UMZIN')*col('_KPMGInterpretedCalculationRule')))\
                .otherwise(0))\
          .when(col('VBTYP_1')=='3',(col('ZMENG_UMZIZ_UMZIN')*col('_KPMGInterpretedCalculationRule')))\
            .otherwise(0)
    #orderingDocumentAmountDC
    v_orderingDocumentAmountDC= col('orderingDocumentAmountDCSource')*col('_KPMGInterpretedCalculationRule')

    #orderingDocumentTargetQuantitySU
    v_orderingDocumentTargetQuantitySU=col('orderingDocumentTargetQuantitySUSource')\
        *col('_KPMGInterpretedCalculationRule')

    #orderingDocumentTargetAmountDC
    v_orderingDocumentTargetAmountDC=col('orderingDocumentTargetAmountDCSource')*col('_KPMGInterpretedCalculationRule')

    #OrderingDocumentTargetQuantitySKU
    v_OrderingDocumentTargetQuantitySKU=col('ZMENG_UMZIZ_UMZIN')*col('_KPMGInterpretedCalculationRule')

    #OrderingDocumentTargetQuantitySKUSource
    v_OrderingDocumentTargetQuantitySKUSource=col('ZMENG_UMZIZ_UMZIN')

    #orderingDocumentQuantitySUEFFSource
    v_orderingDocumentQuantitySUEFFSource =\
        when(col('VBTYP_1')=='1'\
             ,when(col('orderingDocumentQuantitySUSource') > 0,col('orderingDocumentQuantitySUSource'))\
             .when(((col('orderingDocumentQuantitySUSource') == 0 ) \
                    & (col('orderingDocumentTargetQuantitySUSource') > 0) \
                    & (col('orderingDocumentAmountDCSource') > 0)\
                    & ( (col('NETPR')*col('orderingDocumentTargetQuantitySUSource'))==\
                         col('orderingDocumentAmountDCSource') ))\
                    ,col('orderingDocumentTargetQuantitySUSource'))\
              .otherwise(0))\
        .when(col('VBTYP_1')=='2',col('orderingDocumentTargetQuantitySUSource'))\
        .when(col('VBTYP_1')=='3',col('orderingDocumentTargetQuantitySUSource'))\
        .otherwise(0)

    #orderingDocumentQuantitySUEFF
    v_orderingDocumentQuantitySUEFF=\
        when(col('VBTYP_1')=='1'\
             ,when(col('orderingDocumentQuantitySUSource') > 0, \
                   col('orderingDocumentQuantitySUSource') *col('_KPMGInterpretedCalculationRule') )\
             .when(((col('orderingDocumentQuantitySUSource') == 0 ) \
                    & (col('orderingDocumentTargetQuantitySUSource') > 0) \
                    & (col('orderingDocumentAmountDCSource') > 0)\
                    & ( (col('NETPR')*col('orderingDocumentTargetQuantitySUSource'))==\
                       col('orderingDocumentAmountDCSource') ))\
                    ,col('orderingDocumentTargetQuantitySUSource')*col('_KPMGInterpretedCalculationRule'))\
              .otherwise(0))\
        .when(col('VBTYP_1')=='2',col('orderingDocumentTargetQuantitySUSource')*col('_KPMGInterpretedCalculationRule'))\
        .when(col('VBTYP_1')=='3',col('orderingDocumentTargetQuantitySUSource')*col('_KPMGInterpretedCalculationRule'))\
        .otherwise(0)

    #orderingDocumentQuantitySKUEFFSource
    v_orderingDocumentQuantitySKUEFFSource=\
         when(col('VBTYP_1')=='1',col('KWMENG_UMVKZ_UMVKN'))\
         .when(col('VBTYP_1')=='2'\
           ,when(col('orderingDocumentTargetQuantitySUSource') !=0 ,col('ZMENG_UMZIZ_UMZIN')).otherwise(0))\
         .when(col('VBTYP_1')=='3',col('ZMENG_UMZIZ_UMZIN'))\
         .otherwise(0)    

    #orderingDocumentQuantitySKUEFF
    v_orderingDocumentQuantitySKUEFF=\
         when(col('VBTYP_1')=='1',col('KWMENG_UMVKZ_UMVKN')*col('_KPMGInterpretedCalculationRule'))\
         .when(col('VBTYP_1')=='2'\
           ,when(col('orderingDocumentTargetQuantitySUSource') !=0\
                 ,col('ZMENG_UMZIZ_UMZIN')*col('_KPMGInterpretedCalculationRule')).otherwise(0))\
         .when(col('VBTYP_1')=='3',col('ZMENG_UMZIZ_UMZIN')*col('_KPMGInterpretedCalculationRule'))\
         .otherwise(0)

    #orderingDocumentAmountDCEFFSource
    v_orderingDocumentAmountDCEFFSource=\
         when(col('VBTYP_1')=='1',col('orderingDocumentAmountDCSource'))\
         .when(col('VBTYP_1')=='2',col('orderingDocumentAmountDCSource'))\
         .when(col('VBTYP_1')=='3'\
              ,when(col('ERLRE') =='E' ,col('orderingDocumentTargetAmountDCSource'))\
               .otherwise(col('orderingDocumentAmountDCSource'))\
              )\
         .otherwise(0)

    ##orderingDocumentAmountDCEFF
    v_orderingDocumentAmountDCEFF=\
         when(col('VBTYP_1')=='1',col('orderingDocumentAmountDCSource')*col('_KPMGInterpretedCalculationRule'))\
         .when(col('VBTYP_1')=='2',col('orderingDocumentAmountDCSource')*col('_KPMGInterpretedCalculationRule'))\
         .when(col('VBTYP_1')=='3'\
              ,when(col('ERLRE') =='E' ,col('orderingDocumentTargetAmountDCSource')\
                    *col('_KPMGInterpretedCalculationRule'))\
               .otherwise(col('orderingDocumentAmountDCSource')*col('_KPMGInterpretedCalculationRule'))\
              )\
         .otherwise(0)   

    otc_L1_TD_SalesOrder_tmp = otc_L1_TD_SalesOrder_tmp\
      .withColumn('orderingDocumentTypeKPMGSubType',v_orderingDocumentTypeKPMGSubType)\
      .withColumn('orderingDocumentQuantitySU',v_orderingDocumentQuantitySU)\
      .withColumn('orderingDocumentQuantitySKUSource',v_orderingDocumentQuantitySKUSource)\
      .withColumn('orderingDocumentQuantitySKU',v_orderingDocumentQuantitySKU)\
      .withColumn('orderingDocumentAmountDC',v_orderingDocumentAmountDC)\
      .withColumn('orderingDocumentTargetQuantitySU',v_orderingDocumentTargetQuantitySU)\
      .withColumn('orderingDocumentTargetAmountDC',v_orderingDocumentTargetAmountDC)\
      .withColumn('OrderingDocumentTargetQuantitySKU',v_OrderingDocumentTargetQuantitySKU)\
      .withColumn('OrderingDocumentTargetQuantitySKUSource',v_OrderingDocumentTargetQuantitySKUSource)\
      .withColumn('orderingDocumentQuantitySUEFFSource',v_orderingDocumentQuantitySUEFFSource)\
      .withColumn('orderingDocumentQuantitySUEFF',v_orderingDocumentQuantitySUEFF)\
      .withColumn('orderingDocumentQuantitySKUEFFSource',v_orderingDocumentQuantitySKUEFFSource)\
      .withColumn('orderingDocumentQuantitySKUEFF',v_orderingDocumentQuantitySKUEFF)\
      .withColumn('orderingDocumentAmountDCEFFSource',v_orderingDocumentAmountDCEFFSource)\
      .withColumn('orderingDocumentAmountDCEFF',v_orderingDocumentAmountDCEFF)\

    v_orderingDocumentUnitPriceSUDC=when(abs(col('slod1.orderingDocumentQuantitySUEFF')) >0,\
        abs(col('slod1.orderingDocumentAmountDCEFF')/col('slod1.orderingDocumentQuantitySUEFF')))\
        .otherwise(lit(0))

    v_orderingDocumentUnitPriceSKUDC=when(abs(col('slod1.orderingDocumentQuantitySKUEFF')) >0,\
                      abs(col('slod1.orderingDocumentAmountDCEFF')/col('slod1.orderingDocumentQuantitySKUEFF')))\
                      .otherwise(lit(0))

    otc_L1_TD_SalesOrder = otc_L1_TD_SalesOrder_tmp.alias('slod1')\
          .join (gen_L1_STG_MessageStatus.alias('msgs1')\
                 ,((col("msgs1.objectKey").eqNullSafe(col("slod1.orderingDocumentNumber")))\
                   & (col("msgs1.messageRank").eqNullSafe(lit(1)))\
                     ),"left")\
          .join (knw_LK_CD_ReportingSetup.alias('lkrs')\
                 ,(col("slod1.orderingDocumentCompanyCode").eqNullSafe(col("lkrs.companyCode"))\
                  ),"left")\
          .join (knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd')\
                 ,((col("lkbd.businessDatatype").eqNullSafe(lit("Document Message Status")))\
                   & (col("lkbd.sourceSystemValue").eqNullSafe(col('msgs1.processingStatus')))\
                   & (col("lkbd.targetLanguageCode").eqNullSafe(col("lkrs.KPMGDataReportingLanguage")))\
                   & (col("lkbd.sourceERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
                  ),"left")\
           .join (knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd1')\
                 ,((col("lkbd1.businessDatatype").eqNullSafe(lit("Document Message Type")))\
                   & (col("lkbd1.sourceSystemValue").eqNullSafe(col('msgs1.transmissionMedium')))\
                   & (col("lkbd1.targetLanguageCode").eqNullSafe(col("lkrs.KPMGDataReportingLanguage")))\
                   & (col("lkbd1.sourceERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
                  ),"left")\
            .select(col('slod1.*')\
                    ,coalesce(col('lkbd.targetSystemValueDescription'),lit('Processing status unknown'))\
                        .alias('orderingDocumentMessageStatus')\
                    ,coalesce(col('lkbd1.targetSystemValueDescription'),lit('Message type unknown'))\
                        .alias('orderingDocumentMessageType')\
                    ,lit(v_orderingDocumentUnitPriceSUDC).alias("orderingDocumentUnitPriceSUDC")\
                    ,lit(v_orderingDocumentUnitPriceSKUDC).alias("orderingDocumentUnitPriceSKUDC")\
                   )\

    ls_curcyResult=currencyConversion_L1_TD_SalesOrder(otc_L1_TD_SalesOrder)
    if(ls_curcyResult[0] == LOG_EXECUTION_STATUS.FAILED): 
         raise Exception(ls_curcyResult[1])
        
    otc_L1_TD_SalesOrder=ls_curcyResult[2]
    
    otc_L1_TD_SalesOrder =  objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_TD_SalesOrder,'otc','L1_TD_SalesOrder',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_TD_SalesOrder populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
  
  
def currencyConversion_L1_TD_SalesOrder(df_fromSource):
 
    objGenHelper = gen_genericHelper()
    sourceschema='otc'
    sourcetable='L1_TD_SalesOrder'
    sourceCompanyCode='orderingDocumentCompanyCode'
    sourceCurrencyField='orderingDocumentCurrency'
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
        .drop(*('orderingDocumentUnitPriceSULC', 'orderingDocumentUnitPriceSKULC'\
                             ,'orderingDocumentAmountLC','orderingDocumentAmountLCSource'\
                             ,'orderingDocumentAmountLCEFF' ,'orderingDocumentAmountLCEFFSource'\
                             ))\
        .select(col('src.*'))\
        .withColumn("sourceDate", expr(v_sourceDate))

      targetTmpPath = gl_layer0Temp_temp + "GCC_SRC_" + sourcetable +".delta"

      objGenHelper.gen_writeToFile_perfom(df_source,targetTmpPath,mode='overwrite',isMergeSchema = True) 
      df_source=objGenHelper.gen_readFromFile_perform(targetTmpPath)   

      #1
      #orderingDocumentUnitPriceSUDC
      sourceAmountField='orderingDocumentUnitPriceSUDC'
      targetAmountField='orderingDocumentUnitPriceSULC'
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
      #orderingDocumentUnitPriceSKUDC
      sourceAmountField='orderingDocumentUnitPriceSKUDC'
      targetAmountField='orderingDocumentUnitPriceSKULC'
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
      #orderingDocumentAmountDC
      sourceAmountField='orderingDocumentAmountDC'
      targetAmountField='orderingDocumentAmountLC'
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
      #orderingDocumentAmountDCSource
      sourceAmountField='orderingDocumentAmountDCSource'
      targetAmountField='orderingDocumentAmountLCSource'
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
      #orderingDocumentAmountDCEFF
      sourceAmountField='orderingDocumentAmountDCEFF'
      targetAmountField='orderingDocumentAmountLCEFF'
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
      #orderingDocumentAmountDCEFFSource
      sourceAmountField='orderingDocumentAmountDCEFFSource'
      targetAmountField='orderingDocumentAmountLCEFFSource'
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
      
      df_GenericCurrencyConversion=df_GenericCurrencyConversion_col1\
                   .unionAll(df_GenericCurrencyConversion_col2)\
                   .unionAll(df_GenericCurrencyConversion_col3)\
                   .unionAll(df_GenericCurrencyConversion_col4)\
                   .unionAll(df_GenericCurrencyConversion_col5)\
                   .unionAll(df_GenericCurrencyConversion_col6)

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
        .join(df_result.alias('b').filter(col('sourceColName')=='orderingDocumentUnitPriceSUDC')\
             ,((col("a.orderingDocumentCompanyCode").eqNullSafe(col("b.sourceCompanyCode")))\
               & (col("a.orderingDocumentCurrency").eqNullSafe(col("b.sourceCurrency")))\
               & (col("a.sourceDate").eqNullSafe(col("b.sourceDate")))\
               & (col("a.orderingDocumentUnitPriceSUDC").eqNullSafe(col("b.sourceAmount")))\
              ),"left")\
        .join(df_result.alias('c').filter(col('sourceColName')=='orderingDocumentUnitPriceSKUDC')\
             ,((col("a.orderingDocumentCompanyCode").eqNullSafe(col("c.sourceCompanyCode")))\
               & (col("a.orderingDocumentCurrency").eqNullSafe(col("c.sourceCurrency")))\
               & (col("a.sourceDate").eqNullSafe(col("c.sourceDate")))\
               & (col("a.orderingDocumentUnitPriceSKUDC").eqNullSafe(col("c.sourceAmount")))\
              ),"left")\
        .join(df_result.alias('d').filter(col('sourceColName')=='orderingDocumentAmountDC')\
             ,((col("a.orderingDocumentCompanyCode").eqNullSafe(col("d.sourceCompanyCode")))\
               & (col("a.orderingDocumentCurrency").eqNullSafe(col("d.sourceCurrency")))\
               & (col("a.sourceDate").eqNullSafe(col("d.sourceDate")))\
               & (col("a.orderingDocumentAmountDC").eqNullSafe(col("d.sourceAmount")))\
              ),"left")\
        .join(df_result.alias('e').filter(col('sourceColName')=='orderingDocumentAmountDCSource')\
             ,((col("a.orderingDocumentCompanyCode").eqNullSafe(col("e.sourceCompanyCode")))\
               & (col("a.orderingDocumentCurrency").eqNullSafe(col("e.sourceCurrency")))\
               & (col("a.sourceDate").eqNullSafe(col("e.sourceDate")))\
               & (col("a.orderingDocumentAmountDCSource").eqNullSafe(col("e.sourceAmount")))\
              ),"left")\
        .join(df_result.alias('f').filter(col('sourceColName')=='orderingDocumentAmountDCEFF')\
             ,((col("a.orderingDocumentCompanyCode").eqNullSafe(col("f.sourceCompanyCode")))\
               & (col("a.orderingDocumentCurrency").eqNullSafe(col("f.sourceCurrency")))\
               & (col("a.sourceDate").eqNullSafe(col("f.sourceDate")))\
               & (col("a.orderingDocumentAmountDCEFF").eqNullSafe(col("f.sourceAmount")))\
              ),"left")\
        .join(df_result.alias('g').filter(col('sourceColName')=='orderingDocumentAmountDCEFFSource')\
             ,((col("a.orderingDocumentCompanyCode").eqNullSafe(col("g.sourceCompanyCode")))\
               & (col("a.orderingDocumentCurrency").eqNullSafe(col("g.sourceCurrency")))\
               & (col("a.sourceDate").eqNullSafe(col("g.sourceDate")))\
               & (col("a.orderingDocumentAmountDCEFFSource").eqNullSafe(col("g.sourceAmount")))\
              ),"left")\
        .select(col('a.*')\
               ,col('b.targetAmount').alias('orderingDocumentUnitPriceSULC')\
               ,col('c.targetAmount').alias('orderingDocumentUnitPriceSKULC')\
               ,col('d.targetAmount').alias('orderingDocumentAmountLC')\
               ,col('e.targetAmount').alias('orderingDocumentAmountLCSource')\
               ,col('f.targetAmount').alias('orderingDocumentAmountLCEFF')\
               ,col('g.targetAmount').alias('orderingDocumentAmountLCEFFSource')\
            ,col('a.sourceDate'))

      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus,df_out]     

    except Exception as err:        
      executionStatus = objGenHelper.gen_exceptionDetails_log()
      print(executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus,df_out]   
