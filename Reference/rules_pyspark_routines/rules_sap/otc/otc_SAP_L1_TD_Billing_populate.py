# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce,abs,date_format
from pyspark.sql.window import Window

def otc_SAP_L1_TD_Billing_populate(): 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global otc_L1_TD_Billing    
    clientProcessID_OTC=1
    erpSAPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))

    knw_vw_LK_GD_ClientKPMGDocumentType = gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType']

    col_RowID = Window.partitionBy(col('SFAKN')).orderBy(col('VBELN').asc(),col('MANDT').asc())

    df_L1_CancelledBillingDocuments= erp_VBRK.alias('vbrk_erp')\
        .select("MANDT","VBELN","SFAKN",F.dense_rank().over(col_RowID).alias("col_RowID"))\
        .filter(col('SFAKN')!='')
    df_L1_TMP_CancelledBillingDocuments=df_L1_CancelledBillingDocuments\
        .select(col("MANDT").alias("billingDocumentClient")\
              ,col("VBELN").alias("billingDocumentNumber")\
              ,col("SFAKN").alias("billingDocumentCancelledDocumentNumber")\
             )\
        .filter(col('col_RowID')==1)

    otc_L1_TD_Billing_tmp = erp_VBRP.alias('vbrp_erp')\
      .join (erp_VBRK.alias('vbrk_erp')\
             ,((col("vbrp_erp.MANDT") == col("vbrk_erp.MANDT"))\
               & (col("vbrp_erp.VBELN") == col("vbrk_erp.VBELN"))\
              ),"inner")\
      .join (erp_VBAK.alias('vbak_erp')\
             ,((col("vbak_erp.MANDT").eqNullSafe(col("vbrp_erp.MANDT")))\
               & (col("vbak_erp.VBELN").eqNullSafe(col("vbrp_erp.AUBEL")))\
              ),"left")\
      .join (erp_TVAK.alias('tvak_erp')\
             ,((col("tvak_erp.MANDT").eqNullSafe(col("vbak_erp.MANDT")))\
               & (col("tvak_erp.AUART").eqNullSafe(col("vbak_erp.AUART")))\
              ),"left")\
      .join (erp_TVFK.alias('tvfk_erp')\
             ,((col("tvfk_erp.MANDT").eqNullSafe(col("vbrk_erp.MANDT")))\
               & (col("tvfk_erp.FKART").eqNullSafe(col("vbrk_erp.FKART")))\
              ),"left")\
      .join (erp_T001.alias('t001_erp')\
             ,((col("t001_erp.MANDT").eqNullSafe(col("vbrk_erp.MANDT")))\
               & (col("t001_erp.BUKRS").eqNullSafe(col("vbrk_erp.BUKRS")))\
              ),"left")\
      .join (knw_vw_LK_GD_ClientKPMGDocumentType.alias('bDocType')\
             ,((col("bDocType.documentTypeClient").eqNullSafe(col("vbrk_erp.VBTYP")))\
               & (col("bDocType.documentTypeClientProcessID").eqNullSafe(lit(clientProcessID_OTC)))\
               & (when(col("vbrp_erp.SHKZG")==lit(''),lit('0'))\
                  .when(col("vbrp_erp.SHKZG").isNull(),lit('0'))\
                  .otherwise(lit('1')).eqNullSafe(col("bDocType.isReversed")))\
               & (col("bDocType.ERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
              ),"left")\
      .join (knw_vw_LK_GD_ClientKPMGDocumentType.alias('bDocType2')\
             ,((col("bDocType2.documentTypeClient").eqNullSafe(col("vbrk_erp.VBTYP")))\
               & (col("bDocType2.documentTypeClientProcessID").eqNullSafe(lit(clientProcessID_OTC)))\
               & (col("bDocType2.isReversed").eqNullSafe(lit(0)))\
               & (col("bDocType.ERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
              ),"left")\
      .join (knw_vw_LK_GD_ClientKPMGDocumentType.alias('bPredDocType')\
             ,((col("bPredDocType.documentTypeClient").eqNullSafe(col("vbrp_erp.VGTYP")))\
               & (col("bPredDocType.documentTypeClientProcessID").eqNullSafe(lit(clientProcessID_OTC)))\
               & (col("bPredDocType.isReversed").eqNullSafe(lit(0)))\
               & (col("bPredDocType.ERPSystemID").eqNullSafe(lit(erpSAPSystemID) ))\
              ),"left")\
      .join (df_L1_TMP_CancelledBillingDocuments.alias('cncl')\
             ,((col("cncl.billingDocumentCancelledDocumentNumber").eqNullSafe(col("vbrk_erp.VBELN")))\
               & (col("cncl.billingDocumentClient").eqNullSafe(col("vbrk_erp.MANDT")))\
              ),"left")\
      .join (erp_KNVV.alias('erp_knvv')\
             ,((col("erp_knvv.MANDT").eqNullSafe(col("vbrk_erp.MANDT")))\
               & (col("erp_knvv.KUNNR").eqNullSafe(col("vbrk_erp.KUNAG")))\
               & (col("erp_knvv.VKORG").eqNullSafe(col("vbrk_erp.VKORG")))\
               & (col("erp_knvv.VTWEG").eqNullSafe(col("vbrk_erp.VTWEG")))\
               & (col("erp_knvv.SPART").eqNullSafe(col("vbrk_erp.SPART")))\
              ),"left")\
      .select(col('vbrp_erp.MANDT').alias('billingDocumentClient')\
            ,col('vbrp_erp.VBELN').alias('billingDocumentNumber')\
            ,col('vbrp_erp.POSNR').alias('billingDocumentLineItem')\
            ,col('vbrp_erp.UEPOS').alias('billingDocumentLineItemHigherbom')\
            ,col('vbrk_erp.FKART').alias('billingDocumentTypeSubClient')\
            ,col('vbrk_erp.VBTYP').alias('billingDocumentTypeClient')\
            ,coalesce(col('bDocType.documentTypeClientID'),col('bDocType2.documentTypeClientID'))\
                .alias('billingDocumentTypeClientID')\
            ,coalesce(col('bDocType.documentTypeClientDescription'),col('bDocType2.documentTypeClientDescription'))\
                .alias('billingDocumentTypeClientDescription')\
            ,coalesce(col('bDocType.documentTypeKPMG'),col('bDocType2.documentTypeKPMG'))\
                .alias('billingDocumentTypeKPMG')\
            ,coalesce(col('bDocType.documentTypeKPMGInterpreted'),col('bDocType2.documentTypeKPMGInterpreted'))\
                .alias('documentTypeKPMGInterpreted')\
            ,coalesce(col('bDocType.interpretedDocumentTypeClientID'),col('bDocType2.interpretedDocumentTypeClientID'))\
                .alias('interpretedDocumentTypeClientID')\
            ,coalesce(col('bDocType.documentTypeKPMGShort'),col('bDocType2.documentTypeKPMGShort'))\
                .alias('billingDocumentTypeKpmgShort')\
            ,coalesce(col('bDocType.KPMGInterpretedCalculationRule'),col('bDocType2.KPMGInterpretedCalculationRule'))\
                .alias('_KPMGInterpretedCalculationRule')\
            ,lit('').alias('billingDocumentTypeKPMGSubType')\
            ,col('vbrp_erp.VBELV').alias('billingDocumentOriginatingDocumentNumber')\
            ,col('vbrp_erp.POSNV').alias('billingDocumentOriginatingDocumentLineItem')\
            ,col('vbrp_erp.VGBEL').alias('billingDocumentPredecessorDocumentNumber')\
            ,col('vbrp_erp.VGPOS').alias('billingDocumentPredecessorDocumentLineItem')\
            ,col('vbrp_erp.VGTYP').alias('billingDocumentPredecessorDocumentTypeClient')\
            ,col('bPredDocType.documentTypeClientDescription')\
                  .alias('billingDocumentPredecessordocumentTypeClientDescription')\
            ,col('bPredDocType.documentTypeKPMG')\
                  .alias('billingDocumentPredecessorDocumentTypeKpmg')\
            ,col('bPredDocType.documentTypeKPMGShort')\
                  .alias('billingDocumentPredecessorDocumentTypeKpmgShort')\
            ,when(col('vbrp_erp.AUPOS').isNull(), lit('') ) \
                .when(col('vbrp_erp.AUPOS')=='', lit('') ) \
                .otherwise(col('vbrp_erp.AUBEL')).alias('billingDocumentPredecessorOrderingDocumentNumber')\
            ,when(col('vbrp_erp.AUPOS').isNull(), lit('') ) \
                .otherwise(col('vbrp_erp.AUPOS')).alias('billingDocumentPredecessorOrderingDocumentLineItem')\
            ,col('vbrp_erp.AUTYP').alias('billingDocumentPredecessorOrderingDocumentTypeClient')\
            ,col('vbrk_erp.ERNAM').alias('billingDocumentUser')\
            ,col('vbrk_erp.FKDAT').alias('billingDocumentDate')\
            ,col('vbrk_erp.BUKRS').alias('billingDocumentCompanyCode')\
            ,col('vbrk_erp.WAERK').alias('billingDocumentCurrency')\
            ,col('t001_erp.WAERS').alias('billingDocumentLocalCurrency')\
            ,col('vbrp_erp.ERDAT').alias('billingDocumentDateOfEntry')\
            ,concat(lit('000000'),col('vbrp_erp.ERZET')).alias('billingDocumentTimeOfEntry_')\
            ,col('vbrp_erp.MATNR').alias('billingDocumentProduct')\
            ,col('vbrp_erp.MATNR').alias('billingDocumentProductArtificialID')\
            ,col('vbrk_erp.KUNRG').alias('billingDocumentCustomerPayer')\
            ,col('vbrk_erp.KUNAG').alias('billingDocumentCustomerSoldTo')\
            ,col('vbrp_erp.FKIMG').alias('billingDocumentQuantitySUSource')\
            ,col('vbrp_erp.VRKME').alias('billingDocumentSU')\
            ,col('vbrp_erp.MEINS').alias('billingDocumentSKU')\
            ,col('vbrp_erp.FKLMG').alias('billingDocumentQuantitySKUSource')\
            ,lit(0).alias('billingDocumentUnitPriceSULC')\
            ,col('vbrp_erp.NETWR').alias('billingDocumentAmountDCSource')\
            ,lit(0).alias('billingDocumentAmountLCSource')\
            ,lit(0).alias('billingDocumentAmountLC')\
            ,when(col('vbrp_erp.SHKZG').isNull(), lit(0))\
                .when(col('vbrp_erp.SHKZG')=='', lit(0))\
                .otherwise(lit(1)).alias('billingDocumentReturnsItem')\
            ,lit(0).alias('billingDocumentUnitPriceSKULC')\
            ,col('vbrp_erp.NETWR').alias('billingDocumentAmountDCEFFSource')\
            ,lit('').alias('billingDocumentAmountDCEFF')\
            ,lit(0).alias('billingDocumentAmountLCEFFSource')\
            ,lit(0).alias('billingDocumentAmountLCEFF')\
            ,when(col('vbrk_erp.FKSTO')==lit('X'),lit(1)).otherwise(lit(0))\
                  .alias('billingDocumentCancelled')\
            ,col('vbrk_erp.SFAKN').alias('billingDocumentCancelledDocumentNumber')\
            ,col('cncl.billingDocumentNumber').alias('billingDocumentCancellationDocumentNumber')\
            ,date_format(to_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd HH:mm:ss.SSS').alias('recordCreated')\
            ,col('vbrk_erp.INCO1').alias('billingDocumentInternationalCommercialTerm')\
            ,col('vbrk_erp.INCO2').alias('billingDocumentInternationalCommercialTermDetail')\
            ,col('vbrk_erp.VKORG').alias('billingDocumentSalesOrganization')\
            ,col('vbrk_erp.VTWEG').alias('billingDocumentDistributionChannel')\
            ,col('vbrk_erp.SPART').alias('billingDocumentDivision')\
            ,coalesce(col('erp_knvv.INCO1'),lit('#NA#')).alias('masterDataInternationalCommercialTerm')\
            ,when(col('tvak_erp.KLIMP').isNull(), lit('') ) \
              .otherwise(col('tvak_erp.KLIMP')).alias('_KLIMP')\
            ,when(col('tvfk_erp.XFILKD').isNull(), lit('') ) \
              .otherwise(col('tvfk_erp.XFILKD')).alias('_XFILKD')\
            ,when(((col('vbrp_erp.FKLMG') >0) & (col('vbrp_erp.UMVKZ') !=0)),\
                 col('vbrp_erp.FKLMG') * (col('vbrp_erp.UMVKN')/col('vbrp_erp.UMVKZ')))\
              .otherwise(lit(0)).alias('_FKLMG_UMVKN_UMVKZ')\
            ,when(((col('vbrp_erp.FKIMG') >0) & (col('vbrp_erp.UMVKN') !=0)),\
                 col('vbrp_erp.FKIMG') * (col('vbrp_erp.UMVKZ')/col('vbrp_erp.UMVKN')))\
              .otherwise(lit(0)).alias('_FKIMG_UMVKZ_UMVKN')\
        )
    otc_L1_TD_Billing_tmp = otc_L1_TD_Billing_tmp\
              .withColumn("billingDocumentTimeOfEntry",F.concat(F.expr("substring(billingDocumentTimeOfEntry_,1,2)"),lit(':')\
              ,F.expr("substring(billingDocumentTimeOfEntry_,3,2)"),F.lit(':'),F.expr("substring(billingDocumentTimeOfEntry_,5,2)")))
    
    #billingDocumentCustomerForwardedToGL
    v_billingDocumentCustomerForwardedToGL=\
        when((col('_KLIMP')== lit(''))& (col('_XFILKD')=='A')\
          ,col('billingDocumentCustomerSoldTo'))\
        .otherwise(col('billingDocumentCustomerPayer')).alias('billingDocumentCustomerForwardedToGL')\

    #billingDocumentQuantitySU
    v_billingDocumentQuantitySU=\
        (col('billingDocumentQuantitySUSource') *col('_KPMGInterpretedCalculationRule') \
        ).alias('billingDocumentQuantitySU')\

    #billingDocumentQuantitySKU
    v_billingDocumentQuantitySKU=\
        (col('billingDocumentQuantitySKUSource') *col('_KPMGInterpretedCalculationRule') \
        ).alias('billingDocumentQuantitySKU')\

    #billingDocumentAmountDC
    v_billingDocumentAmountDC=\
        (col('billingDocumentAmountDCSource') *col('_KPMGInterpretedCalculationRule') \
        ).alias('billingDocumentAmountDC')\

    #billingDocumentQuantitySUEFFSource
    v_billingDocumentQuantitySUEFFSource=\
         when(col('billingDocumentQuantitySUSource') > 0,col('billingDocumentQuantitySUSource'))\
        .otherwise(col('_FKLMG_UMVKN_UMVKZ')).alias('billingDocumentQuantitySUEFFSource')\

    #billingDocumentQuantitySUEFF
    v_billingDocumentQuantitySUEFF=\
         when(col('billingDocumentQuantitySUSource') > 0\
            ,(col('billingDocumentQuantitySUSource') *col('_KPMGInterpretedCalculationRule')))\
        .otherwise((col('_FKLMG_UMVKN_UMVKZ')*col('_KPMGInterpretedCalculationRule')))\
        .alias('billingDocumentQuantitySUEFF')

    #billingDocumentQuantitySKUEFFSource
    v_billingDocumentQuantitySKUEFFSource=\
         when(col('billingDocumentQuantitySKUSource') > 0\
            ,col('billingDocumentQuantitySKUSource'))\
        .otherwise(col('_FKIMG_UMVKZ_UMVKN'))\
        .alias('billingDocumentQuantitySKUEFFSource')

    #billingDocumentQuantitySKUEFF
    v_billingDocumentQuantitySKUEFF=\
         when(col('billingDocumentQuantitySKUSource') > 0\
            ,col('billingDocumentQuantitySKUSource')*col('_KPMGInterpretedCalculationRule'))\
        .otherwise(col('_FKIMG_UMVKZ_UMVKN')*col('_KPMGInterpretedCalculationRule'))\
        .alias('billingDocumentQuantitySKUEFF')\

    #billingDocumentAmountDCEFF
    v_billingDocumentAmountDCEFF=\
        (col('billingDocumentAmountDCEFFSource')*col('_KPMGInterpretedCalculationRule'))\
        .alias('billingDocumentAmountDCEFF')\

    otc_L1_TD_Billing_tmp = otc_L1_TD_Billing_tmp\
      .withColumn('billingDocumentCustomerForwardedToGL',v_billingDocumentCustomerForwardedToGL)\
      .withColumn('billingDocumentQuantitySU',v_billingDocumentQuantitySU)\
      .withColumn('billingDocumentQuantitySKU',v_billingDocumentQuantitySKU)\
      .withColumn('billingDocumentAmountDC',v_billingDocumentAmountDC)\
      .withColumn('billingDocumentQuantitySUEFFSource',v_billingDocumentQuantitySUEFFSource)\
      .withColumn('billingDocumentQuantitySUEFF',v_billingDocumentQuantitySUEFF)\
      .withColumn('billingDocumentQuantitySKUEFFSource',v_billingDocumentQuantitySKUEFFSource)\
      .withColumn('billingDocumentQuantitySKUEFF',v_billingDocumentQuantitySKUEFF)\
      .withColumn('billingDocumentAmountDCEFF',v_billingDocumentAmountDCEFF)\
    
    v_billingDocumentUnitPriceSUDC=when(abs(col('blng1.billingDocumentQuantitySUEFF')) >0,\
            abs(col('blng1.billingDocumentAmountDCEFF')/col('blng1.billingDocumentQuantitySUEFF')))\
            .otherwise(lit(0))

    v_billingDocumentUnitPriceSKUDC=when(abs(col('blng1.billingDocumentQuantitySKUEFF')) >0,\
                      abs(col('blng1.billingDocumentAmountDCEFF')/col('blng1.billingDocumentQuantitySKUEFF')))\
                      .otherwise(lit(0))

    otc_L1_TD_Billing = otc_L1_TD_Billing_tmp.alias('blng1')\
      .join (gen_L1_STG_MessageStatus.alias('msgs1')\
           ,((col("msgs1.objectKey").eqNullSafe(col("blng1.billingDocumentNumber")))\
             & (col("msgs1.messageRank").eqNullSafe(lit(1)))\
               ),"left")\
      .join (knw_LK_CD_ReportingSetup.alias('lkrs')\
                 ,(col("blng1.billingDocumentCompanyCode").eqNullSafe(col("lkrs.companyCode"))\
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
      .select(col('blng1.*')\
                    ,coalesce(col('lkbd.targetSystemValueDescription'),lit('Processing status unknown'))\
                        .alias('billingDocumentMessageStatus')\
                    ,coalesce(col('lkbd1.targetSystemValueDescription'),lit('Message type unknown'))\
                        .alias('billingDocumentMessageType')\
                    ,lit(v_billingDocumentUnitPriceSUDC).alias("billingDocumentUnitPriceSUDC")\
                    ,lit(v_billingDocumentUnitPriceSKUDC).alias("billingDocumentUnitPriceSKUDC")\
                   )    
   
    otc_L1_TD_Billing = otc_L1_TD_Billing.select(col('billingDocumentNumber'),col('billingDocumentLineItem')\
        ,col('billingDocumentLineItemHigherbom'),col('billingDocumentTypeSubClient')\
        ,col('billingDocumentTypeClient')\
        ,col('billingDocumentTypeClientDescription'),col('billingDocumentTypeKPMG')\
        ,col('billingDocumentTypeKpmgShort')\
        ,col('billingDocumentTypeKPMGSubType'),col('billingDocumentOriginatingDocumentNumber')\
                             ,col('billingDocumentOriginatingDocumentLineItem')\
                             ,col('billingDocumentPredecessorDocumentNumber')\
                             ,col('billingDocumentPredecessorDocumentLineItem')\
                             ,col('billingDocumentPredecessorDocumentTypeClient')\
                             ,col('billingDocumentPredecessordocumentTypeClientDescription')\
                             ,col('billingDocumentPredecessorDocumentTypeKpmg')\
                             ,col('billingDocumentPredecessorDocumentTypeKpmgShort')\
                             ,col('billingDocumentPredecessorOrderingDocumentNumber')\
                             ,col('billingDocumentUser'),col('billingDocumentDate')\
                             ,col('billingDocumentCompanyCode')\
                             ,col('billingDocumentCurrency'),col('billingDocumentLocalCurrency')\
                             ,col('billingDocumentDateOfEntry')\
                             ,col('billingDocumentTimeOfEntry'),col('billingDocumentProduct')\
                             ,col('billingDocumentProductArtificialID')\
                             ,col('billingDocumentCustomerPayer'),col('billingDocumentCustomerSoldTo')\
                             ,col('billingDocumentCustomerForwardedToGL')\
                             ,col('billingDocumentQuantitySU'),col('billingDocumentSU')\
                             ,col('billingDocumentSKU'),col('billingDocumentQuantitySKU')\
                             ,col('billingDocumentUnitPriceSUDC'),col('billingDocumentUnitPriceSULC')\
                             ,col('billingDocumentUnitPriceSKUDC')\
                             ,col('billingDocumentUnitPriceSKULC'),col('billingDocumentAmountDC')\
                             ,col('billingDocumentAmountLC')\
                             ,col('billingDocumentReturnsItem')\
                             ,col('billingDocumentPredecessorOrderingDocumentLineItem')\
                             ,col('billingDocumentQuantitySUEFF'),col('billingDocumentQuantitySKUEFF')\
                             ,col('billingDocumentAmountDCEFF')\
                             ,col('billingDocumentAmountLCEFF'),col('billingDocumentMessageStatus')\
                             ,col('billingDocumentMessageType')\
                             ,col('billingDocumentCancelled'),col('billingDocumentCancelledDocumentNumber')\
                             ,col('billingDocumentCancellationDocumentNumber')\
                             ,col('recordCreated')\
                             ,col('billingDocumentPredecessorOrderingDocumentTypeClient')\
                             ,col('billingDocumentInternationalCommercialTerm')\
                             ,col('billingDocumentInternationalCommercialTermDetail')\
                             ,col('billingDocumentClient')\
                             ,col('documentTypeKPMGInterpreted')\
                             ,col('interpretedDocumentTypeClientID')\
                             ,col('billingDocumentAmountDCSource'),col('billingDocumentAmountDCEFFSource')\
                             ,col('billingDocumentAmountLCSource')\
                             ,col('billingDocumentAmountLCEFFSource')\
                             ,col('billingDocumentQuantitySKUSource'),col('billingDocumentQuantitySKUEFFSource')\
                             ,col('billingDocumentQuantitySUSource'),col('billingDocumentQuantitySUEFFSource')\
                             ,col('billingDocumentTypeClientID'),col('billingDocumentSalesOrganization')\
                             ,col('billingDocumentDistributionChannel'),col('billingDocumentDivision')\
                             ,col('masterDataInternationalCommercialTerm')\
                            )
    
    ls_curcyResult=currencyConversion_L1_TD_Billing(otc_L1_TD_Billing)
    if(ls_curcyResult[0] == LOG_EXECUTION_STATUS.FAILED): 
      raise Exception(ls_curcyResult[1])
    otc_L1_TD_Billing=ls_curcyResult[2]
    
    otc_L1_TD_Billing =  objDataTransformation.gen_convertToCDMandCache \
       (otc_L1_TD_Billing,'otc','L1_TD_Billing',targetPath=gl_CDMLayer1Path)

    executionStatus = "L1_TD_Billing populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
   


def currencyConversion_L1_TD_Billing(df_fromSource):
 
  objGenHelper = gen_genericHelper()
  sourceschema='otc'
  sourcetable='L1_TD_Billing'
  sourceCompanyCode='billingDocumentCompanyCode'
  sourceCurrencyField='billingDocumentCurrency'
  df_out=None
  executionStatus="Currency Conversion Validation is passed"
  try: 
    #ls_SourceDate=currencyConversionSourceDate_get(sourceschemaName="otc",sourcetableName="L1_TD_Billing",overrideWithDate='2020-01-01')
    lstValidationStatus=currencyConversionSourceDate_get(sourceschemaName=sourceschema,sourcetableName=sourcetable,overrideWithDate=None)

    if(lstValidationStatus[0] == LOG_EXECUTION_STATUS.FAILED): 
       raise Exception(lstValidationStatus[1])

    v_sourceDate=lstValidationStatus[2]
    col_RowID= F.row_number().over(Window.orderBy(lit('')))
    
    #otc.L1_TD_Billing
    df_source=df_fromSource.alias('src')\
      .drop(*('billingDocumentUnitPriceSULC', 'billingDocumentUnitPriceSKULC'\
                           ,'billingDocumentAmountLC','billingDocumentAmountLCSource'\
                           ,'billingDocumentAmountLCEFF' ,'billingDocumentAmountLCEFFSource'\
                           ))\
      .select(col('src.*'))\
      .withColumn("sourceDate", expr(v_sourceDate))

    targetTmpPath = gl_layer0Temp_temp + "GCC_SRC_" + sourcetable +".delta"
    
    objGenHelper.gen_writeToFile_perfom(df_source,targetTmpPath,mode='overwrite',isMergeSchema = True) 
    df_source=objGenHelper.gen_readFromFile_perform(targetTmpPath)   
    #1
    #billingDocumentUnitPriceSUDC
    sourceAmountField='billingDocumentUnitPriceSUDC'
    targetAmountField='billingDocumentUnitPriceSULC'
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
    #billingDocumentUnitPriceSKUDC
    sourceAmountField='billingDocumentUnitPriceSKUDC'
    targetAmountField='billingDocumentUnitPriceSKULC'
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
    #billingDocumentAmountDC
    sourceAmountField='billingDocumentAmountDC'
    targetAmountField='billingDocumentAmountLC'
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
    #billingDocumentAmountDCSource
    sourceAmountField='billingDocumentAmountDCSource'
    targetAmountField='billingDocumentAmountLCSource'
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
    #billingDocumentAmountDCEFF
    sourceAmountField='billingDocumentAmountDCEFF'
    targetAmountField='billingDocumentAmountLCEFF'
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
    #billingDocumentAmountDCEFFSource
    sourceAmountField='billingDocumentAmountDCEFFSource'
    targetAmountField='billingDocumentAmountLCEFFSource'
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
      .join(df_result.alias('b').filter(col('sourceColName')=='billingDocumentAmountDC')\
           ,((col("a.billingDocumentCompanyCode").eqNullSafe(col("b.sourceCompanyCode")))\
             & (col("a.billingDocumentCurrency").eqNullSafe(col("b.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("b.sourceDate")))\
             & (col("a.billingDocumentAmountDC").eqNullSafe(col("b.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('c').filter(col('sourceColName')=='billingDocumentUnitPriceSKUDC')\
           ,((col("a.billingDocumentCompanyCode").eqNullSafe(col("c.sourceCompanyCode")))\
             & (col("a.billingDocumentCurrency").eqNullSafe(col("c.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("c.sourceDate")))\
             & (col("a.billingDocumentUnitPriceSKUDC").eqNullSafe(col("c.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('d').filter(col('sourceColName')=='billingDocumentAmountDCSource')\
           ,((col("a.billingDocumentCompanyCode").eqNullSafe(col("d.sourceCompanyCode")))\
             & (col("a.billingDocumentCurrency").eqNullSafe(col("d.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("d.sourceDate")))\
             & (col("a.billingDocumentAmountDCSource").eqNullSafe(col("d.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('e').filter(col('sourceColName')=='billingDocumentAmountDCEFFSource')\
           ,((col("a.billingDocumentCompanyCode").eqNullSafe(col("e.sourceCompanyCode")))\
             & (col("a.billingDocumentCurrency").eqNullSafe(col("e.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("e.sourceDate")))\
             & (col("a.billingDocumentAmountDCEFFSource").eqNullSafe(col("e.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('f').filter(col('sourceColName')=='billingDocumentAmountDCEFF')\
           ,((col("a.billingDocumentCompanyCode").eqNullSafe(col("f.sourceCompanyCode")))\
             & (col("a.billingDocumentCurrency").eqNullSafe(col("f.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("f.sourceDate")))\
             & (col("a.billingDocumentAmountDCEFF").eqNullSafe(col("f.sourceAmount")))\
            ),"left")\
      .join(df_result.alias('g').filter(col('sourceColName')=='billingDocumentUnitPriceSUDC')\
           ,((col("a.billingDocumentCompanyCode").eqNullSafe(col("g.sourceCompanyCode")))\
             & (col("a.billingDocumentCurrency").eqNullSafe(col("g.sourceCurrency")))\
             & (col("a.sourceDate").eqNullSafe(col("g.sourceDate")))\
             & (col("a.billingDocumentUnitPriceSUDC").eqNullSafe(col("g.sourceAmount")))\
            ),"left")\
      .select(col('a.*')\
             ,col('b.targetAmount').alias('billingDocumentAmountLC')\
             ,col('c.targetAmount').alias('billingDocumentUnitPriceSKULC')\
             ,col('d.targetAmount').alias('billingDocumentAmountLCSource')\
             ,col('e.targetAmount').alias('billingDocumentAmountLCEFFSource')\
             ,col('f.targetAmount').alias('billingDocumentAmountLCEFF')\
             ,col('g.targetAmount').alias('billingDocumentUnitPriceSULC')\
          ,col('a.sourceDate'))
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus,df_out]     

  except Exception as err:        
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus,df_out]   
