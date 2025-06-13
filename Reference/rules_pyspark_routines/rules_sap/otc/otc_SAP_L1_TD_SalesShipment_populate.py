# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import row_number,col,date_format

def otc_SAP_L1_TD_SalesShipment_populate(): 
    try:
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
      global otc_L1_TD_SalesShipment

      clientProcessID_OTC = 1
      erpSAPSystemID      = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))

      otc_L1_TD_SalesShipment = erp_LIKP.alias("likp_erp").join(erp_LIPS.alias("lips_erp")\
      ,((col("likp_erp.MANDT")==col("lips_erp.MANDT"))\
      &(col("likp_erp.VBELN")==col("lips_erp.VBELN"))),how="inner")\
      .join(gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType'].alias("shipDocType")\
      ,((col("likp_erp.VBTYP")==col("shipDocType.documentTypeClient"))\
      &(col("shipDocType.documentTypeClientProcessID")==lit(clientProcessID_OTC))\
      &(when(col("lips_erp.SHKZG")=='X',lit(1)).otherwise(lit(0)) == col("shipDocType.isReversed"))\
      &(col("shipDocType.ERPSystemID")==lit(erpSAPSystemID))),how="left")\
      .join(gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType'].alias("shipDocType2")\
      ,((col("likp_erp.VBTYP")==col("shipDocType2.documentTypeClient"))\
      &(col("shipDocType2.documentTypeClientProcessID")==lit(clientProcessID_OTC))\
      &(col("shipDocType2.isReversed") == lit(0))\
      &(col("shipDocType2.ERPSystemID")==lit(erpSAPSystemID))),how="left")\
      .join(gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType'].alias("ShipPredDocType")\
      ,((col("lips_erp.VGTYP")==col("ShipPredDocType.documentTypeClient"))\
      &(col("ShipPredDocType.documentTypeClientProcessID")==lit(clientProcessID_OTC))\
      &(col("ShipPredDocType.isReversed") == lit(0))\
      &(col("ShipPredDocType.ERPSystemID")==lit(erpSAPSystemID))),how="left")\
      .join(erp_TVKO.alias("tvko_erp"),((col("likp_erp.MANDT") == col("tvko_erp.MANDT"))\
      &(col("likp_erp.VKORG")==col("tvko_erp.VKORG"))),how="left")\
      .join(erp_T001.alias("t001_erp"),((col("likp_erp.MANDT")==col("t001_erp.MANDT"))\
      &(col("tvko_erp.BUKRS")==col("t001_erp.BUKRS"))),how="left")\
      .join(gen_L1_STG_MessageStatus.alias("msgs1"),(col("msgs1.objectKey")==col("lips_erp.VBELN"))\
      &(col("msgs1.messageRank")==1),how="left")\
      .join(knw_LK_CD_ReportingSetup.alias("lkrs"),col("t001_erp.BUKRS")==	col("lkrs.companyCode"),how="left")\
      .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("lkbd"),(col("lkbd.businessDatatype")== lit("Document Message Status"))\
      &(col("msgs1.processingStatus") == col("lkbd.sourceSystemValue"))\
      &(col("lkbd.targetLanguageCode")== col("lkrs.KPMGDataReportingLanguage"))\
      &(col("lkbd.sourceERPSystemID")	== lit(erpSAPSystemID)),how="left")\
      .join(knw_LK_GD_BusinessDatatypeValueMapping.alias("lkbd1"),(col("lkbd1.businessDatatype")== lit("Document Message Type"))\
      &(col("msgs1.transmissionMedium") == col("lkbd1.sourceSystemValue"))\
      &(col("lkbd1.targetLanguageCode")== col("lkrs.KPMGDataReportingLanguage"))\
      &(col("lkbd1.sourceERPSystemID")	== lit(erpSAPSystemID)),how="left")\
      .filter(col("lips_erp.FKREL") != "")\
      .withColumn("TimeOfEntry",substring(concat(lit("000000"),"likp_erp.ERZET"),-6,6))\
      .select(col("lips_erp.VBELN").alias("shippingDocumentNumber")\
      ,col("lips_erp.POSNR").alias("shippingDocumentLineItem")\
      ,col("likp_erp.LFART").alias("shippingDocumentTypeSubClient")\
      ,col("likp_erp.VBTYP").alias("shippingDocumentTypeClient")\
      ,coalesce(col("shipDocType.documentTypeClientID"),col("shipDocType2.documentTypeClientID")).alias("shippingDocumentTypeClientID")\
      ,coalesce(col("shipDocType.documentTypeClientDescription"),col("shipDocType2.documentTypeClientDescription"))\
            .alias("shippingDocumentTypeClientDescription")\
      ,coalesce(col("shipDocType.documentTypeKPMG"),col("shipDocType2.documentTypeKPMG")).alias("shippingDocumentTypeKpmg")\
      ,coalesce(col("shipDocType.documentTypeKPMGInterpreted"),col("shipDocType2.documentTypeKPMGInterpreted"))\
              .alias("documentTypeKPMGInterpreted")\
      ,coalesce(col("shipDocType.interpretedDocumentTypeClientID"),col("shipDocType2.interpretedDocumentTypeClientID"))\
              .alias("interpretedDocumentTypeClientID")\
      ,coalesce(col("shipDocType.documentTypeKPMGShort"),col("shipDocType2.documentTypeKPMGShort"))\
              .alias("shippingDocumentTypeKpmgShort")\
      ,lit("").alias("shippingDocumentTypeKPMGSubType")\
      ,col("lips_erp.VGBEL").alias("shippingDocumentPredecessorDocumentNumber")\
      ,col("lips_erp.VGPOS").alias("shippingDocumentPredecessorDocumentLineItem")\
      ,col("lips_erp.VGTYP").alias("shippingDocumentPredecessorDocumentTypeClient")\
      ,col("shipPredDocType.documentTypeClientDescription").alias("shippingDocumentPredecessordocumentTypeClientDescription")\
      ,col("shipPredDocType.documentTypeKPMG").alias("shippingDocumentPredecessorDocumentTypeKpmg")\
      ,col("shipPredDocType.documentTypeKPMGShort").alias("shippingDocumentPredecessorDocumentTypeKpmgShort")\
      ,col("lips_erp.UECHA").alias("shippingDocumentLineItemHigherBatch")\
      ,col("likp_erp.ERNAM").alias("shippingDocumentUser")\
      ,col("likp_erp.LFDAT").alias("shippingDocumentDate")\
      ,col("t001_erp.BUKRS").alias("shippingDocumentCompanycode")\
      ,col("likp_erp.ERDAT").alias("shippingDocumentDateOfEntry")\
      ,concat(substring("TimeOfEntry",1,2),lit(":"),substring("TimeOfEntry",3,2),lit(":"),substring("TimeOfEntry",5,2)).alias("shippingDocumentTimeOfEntry")\
      ,col("lips_erp.MATNR").alias("shippingDocumentProduct")\
      ,col("lips_erp.MATNR").alias("shippingDocumentProductArtificialID")\
      ,col("likp_erp.KUNNR").alias("shippingDocumentCustomerShipTo")\
      ,col("likp_erp.KUNAG").alias("shippingDocumentCustomerSoldTo")\
      ,col("lips_erp.LGMNG").alias("shippingDocumentQuantitySKUSource")\
      ,(col("lips_erp.LGMNG") * coalesce(col("shipDocType.KPMGInterpretedCalculationRule")\
                               ,col("shipDocType2.KPMGInterpretedCalculationRule"))).alias("shippingDocumentQuantitySKU")\
      ,when(col("likp_erp.VBTYP").isin('8','J','T'),
            when(col("lips_erp.LGMNG") > 0,col("lips_erp.LGMNG"))\
            .otherwise(when(col("lips_erp.KCMENG") > 0,col("lips_erp.KCMENG"))\
                      .when((col("lips_erp.LFIMG") > 0)&(col("lips_erp.UMVKN") != 0),col("lips_erp.LFIMG") * (col("lips_erp.UMVKZ") / col("lips_erp.UMVKN")))\
                      .otherwise(lit(0))\
                      )\
             )\
      .when(col("likp_erp.VBTYP").isin('7'),\
            when(col("lips_erp.LGMNG") > 0,col("lips_erp.LGMNG"))\
            .otherwise(when((col("lips_erp.LFIMG") > 0) & (col("lips_erp.UMVKN") != 0),col("LFIMG") * (col("lips_erp.UMVKZ") / col("lips_erp.UMVKN")))\
                      .otherwise(lit(0))\
                      )\
           ).alias("ShippingDocumentQuantitySKUEFFSource")\
      ,when(col("likp_erp.VBTYP").isin('8','J','T'),
            when(col("lips_erp.LGMNG") > 0,\
                 col("lips_erp.LGMNG")*coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
            .otherwise(when(col("lips_erp.KCMENG") > 0,\
                            col("lips_erp.KCMENG")*coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
                      .when((col("lips_erp.LFIMG") > 0)&(col("lips_erp.UMVKN") != 0),col("lips_erp.LFIMG") * (col("lips_erp.UMVKZ") / col("lips_erp.UMVKN"))\
                           * coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule"))\
                           )\
                      .otherwise(lit(0))\
                      )\
             )\
      .when(col("likp_erp.VBTYP").isin('7'),\
            when(col("lips_erp.LGMNG") > 0,\
                 col("lips_erp.LGMNG")* coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
            .otherwise(when((col("lips_erp.LFIMG") > 0) & (col("lips_erp.UMVKN") != 0),col("LFIMG") * (col("lips_erp.UMVKZ") / col("lips_erp.UMVKN"))\
                           * coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule"))\
                           )\
                      .otherwise(lit(0))\
                      )\
           ).alias("shippingDocumentQuantitySKUEFF")\
      ,col("lips_erp.MEINS").alias("shippingDocumentSKU")\
      ,col("lips_erp.LFIMG").alias("shippingDocumentQuantitySUSource")\
      ,(col("lips_erp.LFIMG") * coalesce(col("shipDocType.KPMGInterpretedCalculationRule")\
                               ,col("shipDocType2.KPMGInterpretedCalculationRule"))).alias("shippingDocumentQuantitySU")\
      ,when(col("likp_erp.VBTYP").isin('8','J','T'),
            when(col("lips_erp.LGMNG") > 0,col("lips_erp.LFIMG"))\
            .otherwise(when((col("lips_erp.LGMNG") > 0) &(col("lips_erp.UMVKZ") != 0),col("lips_erp.LGMNG")*\
                            (col("lips_erp.UMVKN") / col("lips_erp.UMVKZ")))\
                      .when((col("lips_erp.KCMENG") > 0)&(col("lips_erp.UMVKZ") != 0),col("lips_erp.KCMENG") *\
                            (col("lips_erp.UMVKN") / col("lips_erp.UMVKZ")))\
                      .otherwise(lit(0))\
                      )\
             )\
      .when(col("likp_erp.VBTYP").isin('7'),\
            when(col("lips_erp.LFIMG") > 0,col("lips_erp.LFIMG"))\
            .otherwise(when((col("lips_erp.LGMNG") > 0) & (col("lips_erp.UMVKZ") != 0),col("LGMNG") *\
                            (col("lips_erp.UMVKN") / col("lips_erp.UMVKZ")))\
                      .when((col("lips_erp.KCMENG")> 0) & (col("lips_erp.UMVKZ") != 0),col("lips_erp.KCMENG") *\
                            (col("lips_erp.UMVKN")/col("lips_erp.UMVKZ")))\
                      .otherwise(lit(0))\
                      )\
           ).alias("shippingDocumentQuantitySUEFFSource")\
      ,when(col("likp_erp.VBTYP").isin('8','J','T'),
            when(col("lips_erp.LGMNG") > 0,col("lips_erp.LFIMG")*\
                 coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
            .otherwise(when((col("lips_erp.LGMNG") > 0) &(col("lips_erp.UMVKZ") != 0),col("lips_erp.LGMNG")*\
                            (col("lips_erp.UMVKN") / col("lips_erp.UMVKZ"))*\
                            coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
                      .when((col("lips_erp.KCMENG") > 0)&(col("lips_erp.UMVKZ") != 0),col("lips_erp.KCMENG") *\
                            (col("lips_erp.UMVKN") / col("lips_erp.UMVKZ"))\
                           *coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
                      .otherwise(lit(0))\
                      )\
             )\
      .when(col("likp_erp.VBTYP").isin('7'),\
            when(col("lips_erp.LFIMG") > 0,col("lips_erp.LFIMG")*\
                coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
            .otherwise(when((col("lips_erp.LGMNG") > 0) & (col("lips_erp.UMVKZ") != 0),col("LGMNG") * (col("lips_erp.UMVKN") / col("lips_erp.UMVKZ"))*\
                           coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
                      .when((col("lips_erp.KCMENG")> 0) & (col("lips_erp.UMVKZ") != 0),col("lips_erp.KCMENG") * (col("lips_erp.UMVKN")/col("lips_erp.UMVKZ"))*\
                           coalesce(col("shipDocType.KPMGInterpretedCalculationRule"),col("shipDocType2.KPMGInterpretedCalculationRule")))\
                      .otherwise(lit(0))\
                      )\
           ).alias("shippingDocumentQuantitySUEFF")\
      ,col("lips_erp.VRKME").alias("shippingDocumentSU")
      ,when(col("lips_erp.UEBTK")=="X",lit(1)).otherwise(lit(0)).alias("unlimitedOverDeliveryAllowed")\
      ,col("lips_erp.UEBTO").alias("shippingDocumentOverdeliveryToleranceLimit")\
      ,col("lips_erp.UNTTO").alias("shippingDocumentUnderdeliveryToleranceLimit")\
      ,when(col("lips_erp.SHKZG")=="X",lit(1)).otherwise(lit(0)).alias("shippingDocumentReturnsItem")\
      ,col("lips_erp.KCMENG").alias("shippingDocumentQuantityBatchSKUSource")\
      ,(col("lips_erp.KCMENG") * coalesce(col("shipDocType.KPMGInterpretedCalculationRule")\
                                ,col("shipDocType2.KPMGInterpretedCalculationRule"))).alias("shippingDocumentQuantityBatchSKU")\
      ,coalesce(col("lkbd.targetSystemValueDescription"),lit("Processing status unknown")).alias("shippingDocumentMessageStatus")\
      ,coalesce(col("lkbd1.targetSystemValueDescription"),lit("Message type unknown")).alias("shippingDocumentMessageType")\
      ,col("likp_erp.INCO1").alias("shippingDocumentInternationalCommercialTerm")\
      ,col("likp_erp.INCO2").alias("shippingDocumentInternationalCommercialTermDetail")\
      ,col("likp_erp.WADAT_IST").alias("shippingDocumentDateOfActualGoodsMovement")\
      ,col("likp_erp.PODAT").alias("shippingDocumentDateOfPOD")\
      ,when(col("likp_erp.POTIM").isNull(),lit("00:00:00")).otherwise(concat(substring(col("likp_erp.POTIM"),1,2),lit(":")\
                                                                             ,substring(col("likp_erp.POTIM"),3,2),lit(":")\
                                                                             ,substring(col("likp_erp.POTIM"),5,2))).alias("shippingDocumentTImeOFPOD")        
      ,col("lips_erp.MANDT").alias("client")\
      ,col("lips_erp.WERKS").alias("shippingPlant")\
      ,col("lips_erp.FKREL").alias("shippingBillingRelevenceFlag")\
      ,col("likp_erp.BLDAT").alias("documentDateInDocument")\
      ,col("likp_erp.WADAT_IST").alias("shippingMovementDate")\
      ,date_format(to_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd HH:mm:ss.SSS').alias('recordCreated')\
      ,col("likp_erp.LIFNR").alias("shippingDocumentVendor")\
      ,col("likp_erp.VKORG").alias("shippingDocumentSalesOrganization"))

      otc_L1_TD_SalesShipment =  objDataTransformation.gen_convertToCDMandCache \
      (otc_L1_TD_SalesShipment,'otc','L1_TD_SalesShipment',targetPath=gl_CDMLayer1Path)
      
      executionStatus = "L1_TD_SalesShipment populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
