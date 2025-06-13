# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import col,expr,lit,when,coalesce,row_number,expr,dense_rank,current_date,current_timestamp,upper,count,sum,abs
from pyspark.sql.window import Window
from pyspark.sql.functions import date_format,min,year
from pyspark.sql.types import IntegerType 
from datetime import datetime,date
from decimal import Decimal
import pandas as pd


def otc_L1_STG_40_SalesFlowTreeDocument_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

    global otc_L1_STG_40_SalesSoDBase
    global otc_L1_STG_40_InspectTreeFirstOrderingDocumentOfTransaction
    global otc_L1_STG_40_InspectTreeQuantitativeAnalysis
    global otc_L1_STG_40_SalesInvoiceSelector
    global otc_L1_STG_40_InspectSalesTreeDocumentClassification
    global otc_L1_STG_40_SalesFlowTreeDocumentCore
    global otc_L1_STG_40_SalesFlowTreeDocumentBase 

    erpSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')
    finYear = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR'))
    periodStart = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START'))
    periodEnd = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END'))
    startDate = parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    endDate = parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
    
    clientProcessID_OTC  = 1
    clientProcessID_PTP  = 2

    v_JoinCond1 = when(col('sftp26.salesDocumentTypeClientID')==lit(30),coalesce(col('po1.purchaseOrderDocumentTypeClientID'),lit(30)))\
       .otherwise(col('sftp26.salesDocumentTypeClientID'))

    Vr_LCtoGLRevenue = coalesce(col('ord1.orderingDocumentAmountLCEFF'),col('bil1.billingDocumentAmountLC'),col('po1.purchaseOrderAmountLC'))	
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_26_SalesFlowTreeParent.alias('sftp26')\
         .join(knw_LK_GD_ClientKPMGDocumentTypeClient.alias('CKDT'),\
    	       (col('sftp26.salesDocumentTypeClientID').eqNullSafe(col('CKDT.documentTypeClientID'))),'left')\
    	 .join(otc_L1_TD_SalesOrder.alias('ord1'),\
    	       ((col('sftp26.salesDocumentNumber').eqNullSafe(col('ord1.orderingDocumentNumber')))&\
    		    (col('sftp26.salesdocumentLineItem').eqNullSafe(col('ord1.orderingDocumentLineItem')))&\
    			(col('sftp26.sourceTypeName').eqNullSafe(lit('Order')))),'left')\
    	 .join(otc_L1_TD_SalesShipment.alias('shp1'),\
    	       ((col('sftp26.salesDocumentNumber').eqNullSafe(col('shp1.shippingDocumentNumber')))&\
    		    (col('sftp26.salesdocumentLineItem').eqNullSafe(col('shp1.shippingDocumentLineItem')))&\
    			(col('sftp26.sourceTypeName').eqNullSafe(lit('Shipping')))),'left')\
    	 .join(otc_L1_TD_Billing.alias('bil1'),\
    	       ((col('sftp26.salesDocumentNumber').eqNullSafe(col('bil1.billingDocumentNumber')))&\
    		    (col('sftp26.salesdocumentLineItem').eqNullSafe(col('bil1.billingDocumentLineItem')))&\
    			(col('sftp26.sourceTypeName').eqNullSafe(lit('Billing')))),'left')\
    	 .join(ptp_L1_TD_PurchaseOrder.alias('po1'),\
    	       ((col('sftp26.salesDocumentNumber').eqNullSafe(col('po1.purchaseOrderNumber')))&\
    		    (col('sftp26.salesdocumentLineItem').eqNullSafe(col('po1.purchaseOrdeLineItem2')))&\
    			(col('sftp26.sourceTypeName').eqNullSafe(lit('Purchase Order')))),'left')\
    	 .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('CKDT1'),\
    	       (col('CKDT1.documentTypeClientID').eqNullSafe(lit(v_JoinCond1))),'left')\
    .select(lit(1).alias('documentTypeClientProcessID')\
       	   ,col('sftp26.ID').alias('childID')\
       	   ,col('sftp26.parentID').alias('parentID'),col('sftp26.salesOrderSubTransactionID').alias('salesOrderSubTransactionID')\
       	   ,col('sftp26.salesTransactionID').alias('salesTransactionID')\
       	   ,col('sftp26.salesParentTransactionID').alias('salesParentTransactionID')\
       	   ,col('sftp26.salesDocumentNumber').alias('salesDocumentNumber'),col('sftp26.salesdocumentLineItem').alias('salesDocumentLineItem')\
       	   ,lit(None).alias('salesDocumentPostingYear'),lit('').alias('salesDocumentAdditionalFilter')\
           ,lit('').alias('salesDocumentPlaceHolderFilter')\
           ,lit(v_JoinCond1).alias('salesDocumentTypeClientID'),col('sftp26.sourceTypeName').alias('sourceTypeName')\
	      ,when(col('sftp26.salesDocumentTypeClientID')==lit(30),coalesce(col('po1.purchaseOrderType'),col('sftp26.salesDocumentTypeClient')))\
               .otherwise(col('sftp26.salesDocumentTypeClient')).alias('salesDocumentTypeClient')\
	      ,coalesce(col('ord1.orderingDocumentDate'),when(col('shp1.shippingDocumentDateOfActualGoodsMovement').isNull(),\
	          col('shp1.shippingDocumentDate')).otherwise(col('shp1.shippingDocumentDateOfActualGoodsMovement')),\
	   	   col('bil1.billingDocumentDate'),col('po1.purchaseOrderConversionDate')).alias('salesDocumentDate')\
	      ,coalesce(col('ord1.orderingDocumentDateOfEntry'),col('shp1.shippingDocumentDateOfEntry')\
	               ,col('bil1.billingDocumentDateOfEntry'),col('po1.purchaseOrderCreationDate')).alias('salesDocumentDateOfEntry')\
	      ,coalesce(col('ord1.orderingDocumentTimeOfEntry'),col('shp1.shippingDocumentTimeOfEntry')\
	              ,col('bil1.billingDocumentTimeOfEntry'),col('po1.purchaseOrderCreationTime')).alias('salesDocumentTimeOfEntry')\
	      ,lit('').alias('salesDocumentSUSKUChoice'),lit('').alias('salesDocumentSUSKUChoiceBasedOnBilling')\
	      ,lit(0).alias('salesDocumentQuantityUnitForMatching'),lit(0).alias('salesDocumentQuantityForMatching')\
	      ,coalesce(col('ord1.orderingDocumentQuantitySUEFF'),when(col('sftp26.hasTransport')==lit(1),lit(0))\
               .otherwise(col('shp1.shippingDocumentQuantitySUEFF'))\
	               ,col('bil1.billingDocumentQuantitySUEFF'),col('po1.purchaseOrderQuantity')).alias('salesDocumentQuantityForMatchingSU')\
	      ,coalesce(col('ord1.orderingDocumentQuantitySKUEFF'),when(col('sftp26.hasTransport')==lit(1),lit(0))\
                    .otherwise(col('shp1.shippingDocumentQuantitySKUEFF'))\
	               ,col('bil1.billingDocumentQuantitySKUEFF'),col('po1.purchaseOrderQuantitySKU')).alias('salesDocumentQuantityForMatchingSKU')\
	      ,coalesce(col('ord1.orderingDocumentQuantitySU'),col('shp1.shippingDocumentQuantitySU')\
                    ,col('bil1.billingDocumentQuantitySU'),col('po1.purchaseOrderQuantity')).alias('salesDocumentQuantitySU')\
          ,coalesce(col('ord1.orderingDocumentQuantitySKU'),col('shp1.shippingDocumentQuantitySKU')\
                    ,col('bil1.billingDocumentQuantitySKU'),col('po1.purchaseOrderQuantitySKU')).alias('salesDocumentQuantitySKU')\
         ,lit(0).alias('salesDocumentPriceForMatching'),lit(0).alias('salesDocumentPriceForExtDiffCalc')\
         ,coalesce(col('ord1.orderingDocumentUnitPriceSUDC'),col('bil1.billingDocumentUnitPriceSUDC')\
                   ,col('po1.purchaseOrderUnitPriceSUDC')).alias('salesDocumentUnitPriceSUDC')\
         ,coalesce(col('ord1.orderingDocumentUnitPriceSULC'),col('bil1.billingDocumentUnitPriceSULC')\
                   ,col('po1.purchaseOrderUnitPriceSULC')).alias('salesDocumentUnitPriceSULC')\
         ,coalesce(col('ord1.orderingDocumentUnitPriceSKUDC'),col('bil1.billingDocumentUnitPriceSKUDC')\
                   ,col('po1.purchaseOrderUnitPriceSKUDC')).alias('salesDocumentUnitPriceSKUDC')\
         ,coalesce(col('ord1.orderingDocumentUnitPriceSKULC'),col('bil1.billingDocumentUnitPriceSKULC')\
                   ,col('po1.purchaseOrderUnitPriceSKULC')).alias('salesDocumentUnitPriceSKULC')\
         ,coalesce(col('ord1.orderingDocumentCurrency'),col('bil1.billingDocumentCurrency')\
                   ,col('po1.purchaseOrderDocumentCurrency')).alias('salesDocumentCurrency')\
         ,coalesce(col('ord1.orderingDocumentLocalCurrency'),col('bil1.billingDocumentLocalCurrency')\
                   ,col('po1.purchaseOrderLocalCurrency')).alias('salesDocumentLocalCurrency')\
         ,coalesce(col('ord1.orderingDocumentCompanyCode'),col('shp1.shippingDocumentCompanycode')\
                   ,col('bil1.billingDocumentCompanyCode'),col('po1.purchaseOrderCompanyCode')\
                   ,col('sftp26.companyCode')).alias('salesDocumentCompanyCode')\
         ,coalesce(col('ord1.orderingDocumentSU'),col('shp1.shippingDocumentSU')\
                   ,col('bil1.billingDocumentSU'),col('po1.purchaseOrderMeasureUnit')\
                   ).alias('salesDocumentSU')\
         ,coalesce(col('ord1.orderingDocumentSKU'),col('shp1.shippingDocumentSKU')\
                   ,col('bil1.billingDocumentSKU'),col('po1.purchaseOrderSKU')\
                   ).alias('salesDocumentSKU')\
         ,coalesce(col('ord1.orderingDocumentAmountDCEFF'),col('bil1.billingDocumentAmountDC')\
                   ,col('po1.purchaseOrderAmountDC')\
                   ).alias('salesDocumentAmountDC')\
         ,coalesce(col('ord1.orderingDocumentAmountLCEFF'),col('bil1.billingDocumentAmountLC')\
                   ,col('po1.purchaseOrderAmountLC')\
                   ).alias('salesDocumentAmountLC')\
         ,when(col('sftp26.revenueAmount').isNull(),lit(0)).otherwise(col('sftp26.revenueAmount')).alias('revenueAmount')\
         ,col('sftp26.isInTimeFrame').alias('isInTimeFrame'),col('sftp26.isArtificial').alias('isArtificial')\
         ,lit(False).alias('isCurrencyUnitInconsistent'),lit(0).alias('isQuantityUnitInconsistent')\
         ,lit(0).alias('isCurrencyUnitInconsistentExcludingOrderingDocument'),lit(0).alias('isQuantityUnitInconsistentExcludingOrderingDocument')\
         ,col('bil1.billingDocumentCancelled').alias('billingDocumentCancelled'),col('sftp26.isTransport').alias('isTransport')\
         ,col('sftp26.hasTransport').alias('hasTransport'),col('CKDT.interpretedDocumentTypeClientID').alias('interpretedDocumentTypeClientID')\
         ,col('CKDT.documentTypeClientID').alias('documentTypeClientID'),lit(0).alias('isDiffLocalCurrencyBillingOrdering')\
         ,when(((col('CKDT1.documentTypeClientProcessID')==lit(clientProcessID_OTC))&(col('CKDT1.documentTypeKPMGGroupID')==3)),\
          when(lit(Vr_LCtoGLRevenue).isNull(),lit(0)).otherwise(lit(Vr_LCtoGLRevenue))-\
	      when(col('sftp26.revenueAmount').isNull(),lit(0)).otherwise(col('sftp26.revenueAmount')))\
    .otherwise(lit(0)).alias('salesDocumentDifferenceAmountLCtoGLRevenue'))
       
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.withColumn("salesDocumentAmountRC",lit(None))\
       .withColumn("revenueAmountRC",lit(None))\
       .withColumn("salesDocumentDifferenceAmountRCtoGLRevenueRC",lit(None))

    vr = (when(col('SFTB.salesDocumentAmountRC').isNull(),lit(0)).otherwise(col('SFTB.salesDocumentAmountRC')))-\
         (when(col('SFTB.revenueAmountRC').isNull(),lit(0)).otherwise(col('SFTB.revenueAmountRC')))
    
    UPD_SalesFlowTreeDocumentBase_1 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
         .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('CKDT'),\
		      ((col('CKDT.documentTypeClientID')==(col('SFTB.salesDocumentTypeClientID')))\
			   &(col('CKDT.documentTypeClientProcessID')==(lit(clientProcessID_OTC)))\
			   &(col('CKDT.documentTypeKPMGGroupID')==(lit(3)))),'inner')\
         .select(col('childID'),lit(vr).alias('vr_salesDocumentDifferenceAmountRCtoGLRevenueRC'))

    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
        .join(UPD_SalesFlowTreeDocumentBase_1.alias('lat'),(col('org.childID').eqNullSafe(col('lat.childID'))),'left')\
        .select(col('org.*'),col('lat.vr_salesDocumentDifferenceAmountRCtoGLRevenueRC')\
               ,when(col('lat.vr_salesDocumentDifferenceAmountRCtoGLRevenueRC').isNull()\
                     ,col('org.salesDocumentDifferenceAmountRCtoGLRevenueRC'))\
                     .otherwise(col('lat.vr_salesDocumentDifferenceAmountRCtoGLRevenueRC')).alias('colDerived'))

    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['salesDocumentDifferenceAmountRCtoGLRevenueRC','vr_salesDocumentDifferenceAmountRCtoGLRevenueRC'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived",'salesDocumentDifferenceAmountRCtoGLRevenueRC')
    
    CTE_OrderingBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTBOrder')\
                  .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('CKTD'),\
				        (col('CKTD.documentTypeClientID')==(col('SFTBOrder.documentTypeClientID')))&\
						(col('CKTD.documentTypeKPMGGroupID')==(lit(1))),'inner')\
				 .select(col('SFTBOrder.salesParentTransactionID').alias('OrderDocumentParentTransactionID')\
				         ,col('SFTBOrder.salesOrderSubTransactionID').alias('OrderDocumentSubTransactionID')\
						 ,col('SFTBOrder.salesDocumentLocalCurrency').alias('OrderDocumentLocalCurrency'))	\
				 .distinct()
    UPD_SalesFlowTreeDocumentBase_2 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
                      .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('CKTDbil'),\
    				      ((col('CKTDbil.documentTypeClientID')==(col('SFTB.documentTypeClientID')))&\
    					  (col('CKTDbil.documentTypeKPMGGroupID')==(lit(3)))),'inner')\
    				  .join(CTE_OrderingBase.alias('COB'),\
    				      ((col('SFTB.salesParentTransactionID')==(col('COB.OrderDocumentParentTransactionID')))&\
    					  (col('SFTB.salesOrderSubTransactionID')==(col('COB.OrderDocumentSubTransactionID')))),'inner')\
                      .select(col('childID')\
         ,when((col('SFTB.salesDocumentLocalCurrency')!=(col('COB.OrderDocumentLocalCurrency')))\
               ,lit(1)).otherwise(lit(0)).alias('vr_isDiffLocalCurrencyBillingOrdering'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase =otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
          .join(UPD_SalesFlowTreeDocumentBase_2.alias('lat'),(col('org.childID').eqNullSafe(col('lat.childID'))),'left')\
          .select(col('org.*')\
                 ,when(col('lat.vr_isDiffLocalCurrencyBillingOrdering').isNull()\
                       ,col('org.isDiffLocalCurrencyBillingOrdering'))\
                       .otherwise(col('lat.vr_isDiffLocalCurrencyBillingOrdering')).alias('colDerived'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['isDiffLocalCurrencyBillingOrdering'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived",'isDiffLocalCurrencyBillingOrdering')
    
    bil1_nullif1 = expr("NULLIF(bil1.masterDataInternationalCommercialTerm,'#NA#')")
    bil1_nullif2 = expr("NULLIF(bil1.billingDocumentInternationalCommercialTerm,'#NA#')")

    otc_L1_STG_40_SalesFlowTreeDocumentCore=\
        otc_L1_STG_26_SalesFlowTreeParent.alias("sftp26")\
          .join(otc_L1_TD_SalesOrder.alias("ord1"),\
          expr("( sftp26.salesDocumentNumber = ord1.orderingDocumentNumber\
         AND sftp26.salesdocumentLineItem = ord1.orderingDocumentLineItem\
         AND sftp26.sourceTypeName ='Order' ) "),"left")\
        .join(otc_L1_TD_SalesShipment.alias("shp1"),\
        expr("( sftp26.salesDocumentNumber = shp1.shippingDocumentNumber\
         AND sftp26.salesdocumentLineItem = shp1.shippingDocumentLineItem\
         AND sftp26.sourceTypeName ='Shipping' ) "),"left")\
        .join(otc_L1_TD_Billing.alias("bil1"),\
        expr("( sftp26.salesDocumentNumber = bil1.billingDocumentNumber\
         AND sftp26.salesdocumentLineItem = bil1.billingDocumentLineItem\
         AND sftp26.sourceTypeName ='Billing' ) "),"left")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("CKDT"),\
        expr("( sftp26.salesDocumentTypeClientID = CKDT.documentTypeClientID ) "),"left")\
        .join(ptp_L1_TD_PurchaseOrder.alias("po1"),\
        expr("( sftp26.salesDocumentNumber = po1.purchaseOrderNumber\
         AND sftp26.salesdocumentLineItem = po1.purchaseOrdeLineItem2\
         AND sftp26.sourceTypeName ='Purchase Order' )"),"left")\
        .select(lit(erpSystemID).alias('ERPSystemID'),col('sftp26.ID').alias('childID')\
               ,col('sftp26.salesOrderSubTransactionID').alias('salesOrderSubTransactionID')\
              ,col('sftp26.salesTransactionID').alias('salesTransactionID')\
               ,col('sftp26.salesParentTransactionID').alias('salesParentTransactionID')\
       ,col('sftp26.salesDocumentNumber').alias('salesDocumentNumber'),col('sftp26.salesdocumentLineItem').alias('salesDocumentLineItem')\
       ,lit(0).alias('GLJEID'),lit(0).alias('GLJELineID'),col('sftp26.hierarchyTypeName').alias('hierarchyTypeName')\
       ,col('sftp26.linkTypeName').alias('linkTypeName')\
       ,col('sftp26.indent').alias('indent'),col('sftp26.parentID').alias('parentID')\
       ,when(col('sftp26.salesDocumentTypeClient')==lit('V'),coalesce(col('po1.purchaseOrderType'),lit('V')))\
              .otherwise(col('sftp26.salesDocumentTypeClient')).alias('salesDocumentTypeClient')\
       ,coalesce(col('ord1.orderingDocumentTypeClientDescription'),col('shp1.shippingDocumentTypeClientDescription')\
        ,col('bil1.billingDocumentTypeClientDescription'),col('po1.purchaseOrderTypeClientDescription'))\
               .alias('salesDocumentTypeClientDescription') \
       ,coalesce(col('ord1.orderingDocumentTypeKPMG'),col('shp1.shippingDocumentTypeKpmg'),col('bil1.billingDocumentTypeKPMG')\
                 ,col('po1.purchaseOrderTypeKPMG')).alias('salesDocumentTypeKPMG')\
       ,coalesce(col('ord1.orderingDocumentTypeKPMGShort'),col('shp1.shippingDocumentTypeKpmgShort'),col('bil1.billingDocumentTypeKpmgShort')\
                 ,col('po1.purchaseOrderTypeKPMGShort')).alias('DocumentTypeKPMGShort')\
       ,col('CKDT.documentTypeClientID').alias('salesDocumentTypeClientID')\
       ,coalesce(col('ord1.orderingDocumentTypeKPMGSubType'),col('shp1.shippingDocumentTypeKPMGSubType')\
                 ,col('bil1.billingDocumentTypeKPMGSubType'),lit('DocumentTypeKPMGSubType')).alias('DocumentTypeKPMGSubType')\
       ,col('sftp26.salesDocumentPredecessorDocumentNumber').alias('DocumentPredecessorDocumentNumber')\
       ,col('sftp26.salesDocumentPredecessorLineItem').alias('DocumentPredecessorLineItem')\
       ,col('sftp26.salesDocumentPredecessorDocumentTypeClient').alias('DocumentPredecessorDocumentTypeClient')\
       ,coalesce(col('ord1.orderingDocumentPredecessordocumentTypeClientDescription'),\
                 col('shp1.shippingDocumentPredecessordocumentTypeClientDescription')\
                ,col('bil1.billingDocumentPredecessordocumentTypeClientDescription'),lit(''))\
               .alias('DocumentPredecessordocumentTypeClientDescription')\
       ,coalesce(col('ord1.orderingDocumentPredecessorDocumentTypeKPMG'),col('shp1.shippingDocumentPredecessorDocumentTypeKpmg')\
                 ,col('bil1.billingDocumentPredecessorDocumentTypeKpmg')).alias('DocumentPredecessordocumentTypeKpmg')\
       ,coalesce(col('ord1.orderingDocumentPredecessorDocumentTypeKPMGShort'),col('shp1.shippingDocumentPredecessorDocumentTypeKpmgShort')\
                ,col('bil1.billingDocumentPredecessorDocumentTypeKpmgShort')).alias('DocumentPredecessordocumentTypeKpmgShort')\
       ,col('sftp26.salesDocumentOriginatingDocumentNumber').alias('DocumentOriginatingDocumentNumber')\
       ,col('sftp26.salesDocumentOriginatingDocumentLineItem').alias('DocumentOriginatingDocumentLineItem')\
       ,col('sftp26.salesDocumentPredecessorOrderingDocumentNumber').alias('DocumentPredecessorOrderingDocumentNumber')\
       ,col('sftp26.salesDocumentPredecessorOrderingDocumentLineItem').alias('DocumentPredecessorOrderingDocumentLineItem')\
       ,col('sftp26.salesDocumentLineItemHigherBOM').alias('DocumentLineItemHigherBOM')\
       ,coalesce(col('ord1.orderingDocumentProduct'),col('shp1.shippingDocumentProduct')\
                ,col('bil1.billingDocumentProduct'),col('po1.purchaseOrderMaterialNumber')).alias('DocumentProduct')\
       ,coalesce(col('ord1.orderingDocumentCustomerSoldTo'),col('shp1.shippingDocumentCustomerSoldTo')\
                ,col('bil1.billingDocumentCustomerSoldTo'),col('po1.purchaseOrderCustomerNumber')).alias('DocumentCustomerSoldTo')\
       ,col('bil1.billingDocumentCustomerForwardedToGL').alias('billingDocumentCustomerForwardedToGL')\
       ,col('po1.purchaseOrderVendorNumber').alias('documentVendorPruchasedFrom')\
       ,coalesce(col('ord1.orderingDocumentUser'),col('shp1.shippingDocumentUser')\
                ,col('bil1.billingDocumentUser'),col('po1.purchaseOrderCreationUser')).alias('DocumentUser')\
       ,coalesce(col('ord1.orderingDocumentCompanyCode'),col('shp1.shippingDocumentCompanycode')\
                 ,col('bil1.billingDocumentCompanyCode'),col('po1.purchaseOrderCompanyCode')\
                 ,col('sftp26.companyCode')).alias('DocumentCompanyCode')\
       ,coalesce(col('ord1.orderingDocumentLocalCurrency'),col('bil1.billingDocumentLocalCurrency')\
                 ,col('po1.purchaseOrderLocalCurrency')).alias('DocumentLocalCurrency')\
       ,lit(0).alias('DocumentValueConsistencyDeviationDCSU')\
       ,coalesce(col('ord1.orderingDocumentReturnsItem'),col('shp1.shippingDocumentReturnsItem')\
                 ,col('bil1.billingDocumentReturnsItem'),lit(False)).alias('DocumentReturnsItem')\
       ,col('ord1.orderingDocumentTargetQuantityUnit').alias('orderingDocumentTargetQuantityUnit')\
       ,col('ord1.orderingDocumentTargetQuantitySU').alias('orderingDocumentTargetQuantity')\
       ,col('ord1.orderingDocumentTargetAmountDC').alias('orderingDocumentTargetAmountDC')\
       ,coalesce(col('ord1.unlimitedOverdeliveryAllowed'),col('po1.isUnlimitedOverdeliveryAllowed')\
                 ,lit(False)).alias('unlimitedOverdeliveryAllowed')\
       ,coalesce(col('ord1.orderingDocumentoverDeliveryToleranceLimit'),col('po1.purchaseOrderOverdeliveryToleranceLimit')\
                 ).alias('overdeliveryToleranceLimit')\
       ,coalesce(col('ord1.orderingDocumentUnderdeliveryToleranceLimit'),col('po1.purchaseOrderUnderdeliveryToleranceLimit')\
                 ).alias('underdeliveryToleranceLimit')\
       ,col('sftp26.salesDocumentLevel').alias('salesDocumentLevel')\
       ,when(col('sftp26.isTransactionArtificial')==0,lit(False)).otherwise(lit(True)).alias('isTransactionArtificial')\
       ,col('sftp26.isParentTransactionArtificial').alias('isParentTransactionArtificial')\
       ,col('sftp26.isTransactionService').alias('isTransactionService')\
       ,col('sftp26.isMaterialNumberInconsistent').alias('isMaterialNumberInconsistent')\
       ,col('sftp26.isCircularReverenceVictim').alias('isCircularReverenceVictim')\
       ,coalesce(col('ord1.orderingDocumentMessageStatus'),col('bil1.billingDocumentMessageStatus')\
                 ,col('shp1.shippingDocumentMessageStatus'),col('po1.purchaseOrderMessageStatus'),lit('Processing status unknown'))\
                 .alias('DocumentMessageStatus')\
       ,coalesce(col('ord1.orderingDocumentMessageType'),col('bil1.billingDocumentMessageType')\
                 ,col('shp1.shippingDocumentMessageType'),col('po1.purchaseOrderMessgeType'),lit('Message type unknown'))\
               .alias('DocumentMessageType')\
       ,when(col('shp1.shippingDocumentInternationalCommercialTerm').isNull(),col('bil1.billingDocumentInternationalCommercialTerm'))\
               .otherwise(col('shp1.shippingDocumentInternationalCommercialTerm')).alias('DocumentInternationalCommercialTerm')\
       ,when(col('shp1.shippingDocumentInternationalCommercialTermDetail').isNull(),col('bil1.billingDocumentInternationalCommercialTermDetail'))\
               .otherwise(col('shp1.shippingDocumentInternationalCommercialTermDetail'))\
               .alias('DocumentInternationalCommercialTermDetail')\
        ,col('shp1.shippingDocumentInternationalCommercialTerm').alias('shippingInternationalCommercialTerm')\
        ,col('shp1.shippingDocumentInternationalCommercialTermDetail').alias('shippingInternationalCommercialTermDetail')\
        ,col('bil1.billingDocumentInternationalCommercialTerm').alias('billingInternationalCommercialTerm')\
        ,col('bil1.billingDocumentInternationalCommercialTermDetail').alias('billingInternationalCommercialTermDetail')\
        ,lit(None).alias('isShippingInternationalCommercialTermStandard'),lit(None).alias('isBillingInternationalCommercialTermStandard')\
        ,col('shp1.shippingDocumentDateOfActualGoodsMovement').alias('shippingDocumentDateOfActualGoodsMovement')\
        ,col('shp1.shippingDocumentDateOfPOD').alias('shippingDocumentDateOfPointOfDelivery')\
        ,col('shp1.shippingDocumentTImeOFPOD').alias('shippingDocumentTimeOFPointOfDelivery'),lit('').alias('OrderDocumentTiming')\
        ,col('sftp26.isRevenueRelevant').alias('isRevenueRelevant')\
        ,col('sftp26.salesDocumentPredecessorOrderingDocumentTypeClient').alias('DocumentPredecessorOrderingDocumentTypeClient')\
        ,col('sftp26.salesDocumentEAN_UPCCode').alias('DocumentProductEANUPCCode')\
        ,when(col('shp1.shippingDocumentQuantityBatchSKU').isNull(),lit(0)).otherwise(col('shp1.shippingDocumentQuantityBatchSKU'))\
                .alias('shippingDocumentQuantityBatchSKU')\
        ,col('bil1.billingDocumentCancelledDocumentNumber').alias('billingDocumentCancelledDocumentNumber')\
        ,col('bil1.billingDocumentCancellationDocumentNumber').alias('billingDocumentCancellationDocumentNumber')\
        ,coalesce(col('ord1.orderingDocumentTimeOfEntry'),col('shp1.shippingDocumentTimeOfEntry')\
                  ,col('bil1.billingDocumentTimeOfEntry'),col('po1.purchaseOrderCreationTime')).alias('documentTimeOfEntry')\
        ,lit(0).alias('salesTypeChainID'),lit(0).alias('salesTypeTransactionChainID')\
        ,col('CKDT.interpretedDocumentTypeClientID').alias('interpretedDocumentTypeClientID')\
        ,col('CKDT.isReversed').alias('isReversed'),col('CKDT.documentTypeKPMGInterpreted').alias('salesDocumentTypeKPMGInterpreted')\
        ,col('CKDT.KPMGInterpretedCalculationRule').alias('KPMGInterpretedCalculationRule')\
        ,col('bil1.billingDocumentSalesOrganization').alias('billingDocumentSalesOrganization')\
        ,col('bil1.billingDocumentDistributionChannel').alias('billingDocumentDistributionChannel')\
        ,col('bil1.billingDocumentDivision').alias('billingDocumentDivision')\
        ,col('bil1.masterDataInternationalCommercialTerm').alias('masterDataInternationalCommercialTerm')\
        ,when(((lit(bil1_nullif1).isNull())|(lit(bil1_nullif2).isNull())),lit("#NA#"))\
                .otherwise(when((col('bil1.masterDataInternationalCommercialTerm'))==\
                    (col('bil1.billingDocumentInternationalCommercialTerm')),lit(0)).otherwise(lit(1))).alias('isIncotermChanged'))\
        .withColumn("DocumentNumberasINT",dense_rank().over(Window.orderBy("salesDocumentNumber")))\
        .withColumn("recordCreated",current_timestamp()).cache()
    
    UPD_SalesFlowTreeDocumentCore_3 = otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('SFTC')\
      .join(otc_L1_STG_27_SalesFlowTreeParentTransactionBase.alias('sftb27'),\
           (col('SFTC.salesParentTransactionID')==(col('sftb27.salesParentTransactionID'))),'inner')\
       .select(col('SFTC.childID'),col('sftb27.salesTypeChainID').alias('vr_salesTypeChainID'))

    otc_L1_STG_40_SalesFlowTreeDocumentCore =otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('org')\
          .join(UPD_SalesFlowTreeDocumentCore_3.alias('lat'),(col('org.childID').eqNullSafe(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_salesTypeChainID')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.salesTypeChainID'))\
                       .otherwise(col('lat.vr_salesTypeChainID')).alias('colDerived'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                     .drop(*['salesTypeChainID','vr_salesTypeChainID'])
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                        .withColumnRenamed("colDerived",'salesTypeChainID')

    UPD_SalesFlowTreeDocumentCore_4 = otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('SFTC')\
        .join(otc_L1_STG_24_SalesFlowTreeTransaction.alias('sftt1'),(col('SFTC.salesTransactionID')==(col('sftt1.transactionID'))),'inner')\
        .select(col('SFTC.childID'),col('sftt1.salesTypeTransactionChainID').alias('vr_salesTypeTransactionChainID'))

    otc_L1_STG_40_SalesFlowTreeDocumentCore =otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('org')\
          .join(UPD_SalesFlowTreeDocumentCore_4.alias('lat'),(col('org.childID').eqNullSafe(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_salesTypeTransactionChainID')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.salesTypeTransactionChainID'))\
                       .otherwise(col('lat.vr_salesTypeTransactionChainID')).alias('colDerived'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                     .drop(*['salesTypeTransactionChainID','vr_salesTypeTransactionChainID'])
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                        .withColumnRenamed("colDerived",'salesTypeTransactionChainID')   

    UPD_SalesFlowTreeDocumentCore_5 = otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('SFTC')\
       .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('ckdt'),\
             (col('SFTC.salesDocumentTypeClientID')==(col('ckdt.documentTypeClientID'))),'leftouter')\
       .select(col('SFTC.childID'),col('ckdt.documentTypeKPMG').alias('vr_salesDocumentTypeKPMG')\
              ,col('ckdt.documentTypeClientDescription').alias('vr_salesDocumentTypeClientDescription'))\
       .filter(col('SFTC.salesDocumentTypeKPMG').isNull())

    otc_L1_STG_40_SalesFlowTreeDocumentCore =otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('org')\
          .join(UPD_SalesFlowTreeDocumentCore_5.alias('lat'),(col('org.childID').eqNullSafe(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_salesDocumentTypeKPMG'),col('lat.vr_salesDocumentTypeClientDescription')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.salesDocumentTypeKPMG'))\
                       .otherwise(col('lat.vr_salesDocumentTypeKPMG')).alias('colDerived')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.salesDocumentTypeClientDescription'))\
                       .otherwise(col('lat.vr_salesDocumentTypeClientDescription')).alias('colDerived2'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                     .drop(*['salesDocumentTypeKPMG','vr_salesDocumentTypeKPMG','salesDocumentTypeClientDescription'\
                             ,'vr_salesDocumentTypeClientDescription'])
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                        .withColumnRenamed("colDerived",'salesDocumentTypeKPMG')\
                        .withColumnRenamed("colDerived2",'salesDocumentTypeClientDescription')

    UPD_SalesFlowTreeDocumentCore_6 = otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('SFTC')\
       .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('ckdt1'),\
             ((col('SFTC.DocumentPredecessorDocumentTypeClient')==(col('ckdt1.documentTypeClient')))&
            (col('ckdt1.documentTypeClientProcessID')==(lit(clientProcessID_OTC)))&
            (col('ckdt1.ERPSystemID')==(lit(erpSystemID)))),'leftouter')\
       .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('ckdt2'),\
             ((col('SFTC.DocumentPredecessorDocumentTypeClient')==(col('ckdt2.documentTypeClient')))&
            (col('ckdt2.documentTypeClientProcessID')==(lit(clientProcessID_PTP)))&
            (col('ckdt2.ERPSystemID')==(lit(erpSystemID)))),'leftouter')\
      .filter(col('SFTC.DocumentPredecessordocumentTypeKpmg').isNull())\
      .select(col('SFTC.childID')\
              ,coalesce(col('ckdt2.documentTypeKPMG'),col('ckdt1.documentTypeKPMG')).alias('vr_DocumentPredecessordocumentTypeKpmg')\
             ,coalesce(col('ckdt2.documentTypeClientDescription'),col('ckdt1.documentTypeClientDescription'))\
                     .alias('vr_DocumentPredecessordocumentTypeClientDescription'))

    otc_L1_STG_40_SalesFlowTreeDocumentCore =otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('org')\
          .join(UPD_SalesFlowTreeDocumentCore_6.alias('lat'),(col('org.childID').eqNullSafe(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_DocumentPredecessordocumentTypeKpmg')\
                  ,col('lat.vr_DocumentPredecessordocumentTypeClientDescription')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.DocumentPredecessordocumentTypeKpmg'))\
                       .otherwise(col('lat.vr_DocumentPredecessordocumentTypeKpmg')).alias('colDerived')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.DocumentPredecessordocumentTypeClientDescription'))\
                       .otherwise(col('lat.vr_DocumentPredecessordocumentTypeClientDescription')).alias('colDerived2'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                     .drop(*['DocumentPredecessordocumentTypeKpmg','vr_DocumentPredecessordocumentTypeKpmg'\
                             ,'DocumentPredecessordocumentTypeClientDescription'\
                             ,'vr_DocumentPredecessordocumentTypeClientDescription'])
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                    .withColumnRenamed("colDerived",'DocumentPredecessordocumentTypeKpmg')\
                    .withColumnRenamed("colDerived2",'DocumentPredecessordocumentTypeClientDescription')

    UPD_SalesFlowTreeDocumentCore_7 = otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('SFTC')\
       .join(knw_LK_GD_StandardInternationalCommercialTerm.alias('sincship'),\
            ((upper(col('SFTC.shippingInternationalCommercialTerm'))) == (upper(col('sincship.standardIncoterm')))),'left')\
       .join(knw_LK_GD_StandardInternationalCommercialTerm.alias('sincbil'),\
             (upper(col('SFTC.billingInternationalCommercialTerm')) == upper(col('sincbil.standardIncoterm'))),'left')\
       .select(col('SFTC.childID')\
               ,when(when(col('sincship.standardIncoterm').isNull(),lit('')).otherwise(col('sincship.standardIncoterm'))==lit(''),\
              lit('Is IncoTerm Standard - No')).otherwise(lit('Is IncoTerm Standard - Yes'))\
               .alias('vr_isShippingInternationalCommercialTermStandard')\
              ,when(when(col('sincbil.standardIncoterm').isNull(),lit('')).otherwise(col('sincbil.standardIncoterm'))==lit(''),\
              lit('Is IncoTerm Standard - No')).otherwise(lit('Is IncoTerm Standard - Yes'))\
               .alias('vr_isBillingInternationalCommercialTermStandard'))

    otc_L1_STG_40_SalesFlowTreeDocumentCore =otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('org')\
          .join(UPD_SalesFlowTreeDocumentCore_7.alias('lat'),(col('org.childID').eqNullSafe(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isShippingInternationalCommercialTermStandard')\
                  ,col('lat.vr_isBillingInternationalCommercialTermStandard')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isShippingInternationalCommercialTermStandard'))\
                       .otherwise(col('lat.vr_isShippingInternationalCommercialTermStandard')).alias('colDerived')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isBillingInternationalCommercialTermStandard'))\
                       .otherwise(col('lat.vr_isBillingInternationalCommercialTermStandard')).alias('colDerived2'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                     .drop(*['isShippingInternationalCommercialTermStandard','vr_isShippingInternationalCommercialTermStandard'\
                             ,'isBillingInternationalCommercialTermStandard'\
                             ,'vr_isBillingInternationalCommercialTermStandard'])
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                        .withColumnRenamed("colDerived",'isShippingInternationalCommercialTermStandard')\
                        .withColumnRenamed("colDerived2",'isBillingInternationalCommercialTermStandard')
    
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('CKDT'),\
             (col('SFTB.documentTypeClientID')==(col('CKDT.documentTypeClientID'))),'inner')\
        .distinct()\
        .select(col('SFTB.documentTypeClientProcessID').alias('documentTypeClientProcessID')\
               ,col('SFTB.childID').alias('childID')\
               ,when(col('CKDT.isMatchingRelevant')==1,lit(True)).otherwise(lit(False)).alias('isValid')\
               ,when(col('CKDT.isMatchingRelevant')==0,lit(10)).otherwise(lit(0)).alias('reasonForInvalidityID'))

    CTE_40InspectTreesPromoDocFilter = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
        .join(otc_L1_STG_27_SalesFlowTreeSubTransactionBase.alias('SFTS'),\
             ((col('SFTB.salesParentTransactionID')==(col('SFTS.salesParentTransactionID')))&\
             (col('SFTB.salesOrderSubTransactionID')==(col('SFTS.salesOrderSubTransactionID')))),'inner')\
        .join(otc_L1_STG_22_SalesFlowTreeBranch.alias('slf22'),\
             (col('SFTB.childID')==(col('slf22.ID'))),'inner')\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('DTC'),\
             (col('SFTB.salesDocumentTypeClientID')==(col('DTC.documentTypeClientID'))),'inner')\
        .distinct()\
        .select(col('SFTB.salesTransactionID'),col('SFTB.salesOrderSubTransactionID'),col('slf22.branchID')\
               ,col('SFTB.salesDocumentQuantityForMatching'),col('SFTB.salesDocumentPriceForMatching'),col('SFTB.salesDocumentAmountDC')\
               ,col('SFTS.ordering'),col('SFTS.billing'))\
        .filter((col('DTC.documentTypeKPMGGroupID').isin(1,3))&\
            (col('SFTB.salesDocumentQuantityForMatching')>0)&\
            (col('SFTB.salesDocumentPriceForMatching')==0)&\
            (col('SFTB.salesDocumentAmountDC')==0)&\
            (col('SFTS.ordering')>=1)&\
            (col('SFTS.billing')>=1))\
        .groupBy(col('SFTB.salesTransactionID'),col('SFTB.salesOrderSubTransactionID'),col('slf22.branchID'))\
             .agg(count(lit(1)).alias('numberOfDocs'))\
        .filter(col('numberOfDocs')==2)\
        .select(col('SFTB.salesTransactionID').alias('salesTransactionID')\
               ,col('SFTB.salesOrderSubTransactionID').alias('salesOrderSubTransactionID')\
              ,col('slf22.branchID').alias('branchID'),col('numberOfDocs'))
    
    DF_inner = otc_L1_STG_26_SalesFlowTreeParent.alias('sftp')\
        .join(CTE_40InspectTreesPromoDocFilter.alias('ctepr'),((col('sftp.salesParentTransactionID')==(col('ctepr.salesTransactionID')))&\
                   (col('sftp.salesOrderSubTransactionID')==(col('ctepr.salesOrderSubTransactionID')))),'inner')\
        .join(otc_L1_STG_22_SalesFlowTreeBranch.alias('sftb1'),((col('sftb1.ID')==(col('sftp.ID')))&\
                    (col('sftb1.branchID')==(col('ctepr.branchID')))),'inner')\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('DTC'),\
              (col('sftb1.documentTypeClientID')==(col('DTC.documentTypeClientID'))),'inner')\
        .select(col('sftb1.ID').alias('ID'))\
        .filter(col('DTC.documentTypeKPMGGroupID').isin(1,3))
 
    UDP_SalesTreeDocumentClassification_8 = otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('indc1')\
           .join(DF_inner.alias('prdcs'),(col('indc1.childID')==(col('prdcs.ID'))),'inner')\
           .filter(col('indc1.documentTypeClientProcessID')==(lit(clientProcessID_OTC)))\
           .select(col('indc1.childID'),lit(False).alias('vr_isValid'),lit(1).alias('vr_reasonForInvalidityID'))
    
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('org')\
          .join(UDP_SalesTreeDocumentClassification_8.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isValid'),col('lat.vr_reasonForInvalidityID')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isValid'))\
                       .otherwise(col('lat.vr_isValid')).alias('colDerived')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.reasonForInvalidityID'))\
                       .otherwise(col('lat.vr_reasonForInvalidityID')).alias('colDerived2'))
    
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                     .drop(*['isValid','vr_isValid','reasonForInvalidityID','vr_reasonForInvalidityID'])
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                        .withColumnRenamed("colDerived",'isValid')\
                        .withColumnRenamed("colDerived2",'reasonForInvalidityID').cache()
    UDP_SalesTreeDocumentClassification_9 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
      .join(otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('indc1'),\
            ((col('SFTB.documentTypeClientProcessID')==(col('indc1.documentTypeClientProcessID')))&\
            (col('SFTB.childID')==(col('indc1.childID')))),'inner')\
      .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('ckdt1'),\
           (col('SFTB.salesDocumentTypeClientID')==(col('ckdt1.documentTypeClientID'))),'inner')\
      .filter(col('ckdt1.documentTypeKPMGShortID').isin(11,20))\
      .select(col('indc1.childID'),lit(False).alias('vr_isValid'),lit(2).alias('vr_reasonForInvalidityID'))

    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('org')\
          .join(UDP_SalesTreeDocumentClassification_9.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isValid'),col('lat.vr_reasonForInvalidityID')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isValid'))\
                       .otherwise(col('lat.vr_isValid')).alias('colDerived')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.reasonForInvalidityID'))\
                       .otherwise(col('lat.vr_reasonForInvalidityID')).alias('colDerived2'))
    
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                     .drop(*['isValid','vr_isValid','reasonForInvalidityID','vr_reasonForInvalidityID'])
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                        .withColumnRenamed("colDerived",'isValid')\
                        .withColumnRenamed("colDerived2",'reasonForInvalidityID').cache()
    
    vr_reasonForInvalidityID = when(col('SFTB.billingDocumentCancelled')==1,lit(3))\
              .when(col('ckdt1.documentTypeKPMGID').isin(16,17,18),lit(4))
  
    UDP_SalesTreeDocumentClassification_10 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
          .join(otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('indc1'),\
               ((col('SFTB.documentTypeClientProcessID')==(col('indc1.documentTypeClientProcessID')))&\
               (col('SFTB.childID')==(col('indc1.childID')))),'inner')\
          .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('ckdt1'),\
               ((col('SFTB.documentTypeClientID')==(col('ckdt1.documentTypeClientID')))),'inner')\
          .filter((col('SFTB.billingDocumentCancelled')==1)&(col('ckdt1.documentTypeKPMGID').isin(16,17,18)))\
          .select(col('indc1.childID'),lit(False).alias('vr_isValid'),lit(vr_reasonForInvalidityID).alias('vr_reasonForInvalidityID'))
    
    
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('org')\
          .join(UDP_SalesTreeDocumentClassification_10.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isValid'),col('lat.vr_reasonForInvalidityID')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isValid'))\
                       .otherwise(col('lat.vr_isValid')).alias('colDerived')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.reasonForInvalidityID'))\
                       .otherwise(col('lat.vr_reasonForInvalidityID')).alias('colDerived2'))
    
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                     .drop(*['isValid','vr_isValid','reasonForInvalidityID','vr_reasonForInvalidityID'])
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                        .withColumnRenamed("colDerived",'isValid')\
                        .withColumnRenamed("colDerived2",'reasonForInvalidityID').cache()

    UDP_SalesTreeDocumentClassification_11 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
      .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('DTC'),\
           (col('SFTB.salesDocumentTypeClientID')==(col('DTC.documentTypeClientID'))),'inner')\
      .join(otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('indc1'),\
           ((col('SFTB.documentTypeClientProcessID')==(col('indc1.documentTypeClientProcessID')))&\
           (col('SFTB.childID')==(col('indc1.childID')))),'inner')\
      .filter((col('DTC.documentTypeKPMGGroupID')==3)&(col('SFTB.revenueAmount')==0)&\
             (col('SFTB.salesDocumentAmountDC')==0)&(col('SFTB.salesDocumentAmountLC')==0)&\
             (col('SFTB.salesDocumentUnitPriceSUDC')==0)&(col('SFTB.salesDocumentUnitPriceSULC')==0)&\
             (col('SFTB.salesDocumentUnitPriceSKUDC')==0)&(col('SFTB.salesDocumentUnitPriceSKULC')==0)&\
             (col('DTC.ERPSystemID')==lit(erpSystemID))&(col('DTC.documentTypeClientProcessID')==lit(clientProcessID_OTC))&\
             (col('DTC.isMatchingRelevant')==lit(1)))\
      .select(col('indc1.childID'),lit(False).alias('vr_isValid'),lit(5).alias('vr_reasonForInvalidityID'))

    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('org')\
          .join(UDP_SalesTreeDocumentClassification_11.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isValid'),col('lat.vr_reasonForInvalidityID')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isValid'))\
                       .otherwise(col('lat.vr_isValid')).alias('colDerived')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.reasonForInvalidityID'))\
                       .otherwise(col('lat.vr_reasonForInvalidityID')).alias('colDerived2'))
    
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                     .drop(*['isValid','vr_isValid','reasonForInvalidityID','vr_reasonForInvalidityID'])
    otc_L1_STG_40_InspectSalesTreeDocumentClassification = otc_L1_STG_40_InspectSalesTreeDocumentClassification\
                        .withColumnRenamed("colDerived",'isValid')\
                        .withColumnRenamed("colDerived2",'reasonForInvalidityID').cache()

    otc_L1_STG_40_InspectSalesTreeDocumentClassification  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_40_InspectSalesTreeDocumentClassification,'otc','L1_STG_40_InspectSalesTreeDocumentClassification',False)

    CTE_DocTypeKPMGCount = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
       .join(otc_L1_STG_40_InspectSalesTreeDocumentClassification.alias('itd40'),\
            ((col('SFTB.documentTypeClientProcessID')==(col('itd40.documentTypeClientProcessID')))&\
            (col('SFTB.childID')==(col('itd40.childID')))),'inner')\
       .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('DTC'),\
            (col('SFTB.documentTypeClientID')==(col('DTC.documentTypeClientID'))),'inner')\
       .filter((col('SFTB.documentTypeClientProcessID')==lit(clientProcessID_OTC))&\
               (col('itd40.isValid')==1)&(col('DTC.documentTypeKPMGGroupID')==3))\
       .groupBy(col('SFTB.documentTypeClientProcessID'),col('SFTB.salesOrderSubTransactionID'),col('SFTB.salesParentTransactionID'))\
       .agg(count(when(col('DTC.documentTypeKPMGID')==23,lit(1)).otherwise(lit(None))).alias('invoiceSalesInvoiceCount')\
            ,count(when(col('DTC.documentTypeKPMGID')==3,lit(1)).otherwise(lit(None))).alias('invoiceDebitMemoCount')\
           ,count(when(col('DTC.documentTypeKPMGID')==2,lit(1)).otherwise(lit(None))).alias('invoiceCreditMemoCount')\
            ,count(when(col('DTC.documentTypeKPMGID')==50,lit(1)).otherwise(lit(None))).alias('invoiceIntercompanyCreditMemoCount')\
           ,count(when(col('DTC.documentTypeKPMGID')==22,lit(1)).otherwise(lit(None))).alias('invoiceProFormaInvoiceCount')\
            ,count(when(col('DTC.documentTypeKPMGID')==21,lit(1)).otherwise(lit(None))).alias('invoiceIntercompanyInvoiceCount'))\
       .select(col('SFTB.documentTypeClientProcessID').alias('documentTypeClientProcessID')\
              ,col('SFTB.salesOrderSubTransactionID').alias('orderSubTransactionID')\
              ,col('SFTB.salesParentTransactionID').alias('parentTransactionID')\
              ,'invoiceSalesInvoiceCount','invoiceDebitMemoCount','invoiceCreditMemoCount','invoiceIntercompanyCreditMemoCount'\
              ,'invoiceProFormaInvoiceCount','invoiceIntercompanyInvoiceCount')

    otc_L1_STG_40_SalesInvoiceSelector = CTE_DocTypeKPMGCount.alias('ctis')\
        .select(col('ctis.documentTypeClientProcessID').alias('documentTypeClientProcessID')\
        ,col('ctis.orderSubTransactionID').alias('orderSubTransactionID')\
        ,col('ctis.parentTransactionID').alias('salesTransactionID')\
        ,when(col('ctis.invoiceSalesInvoiceCount')>=lit(1),lit(23))\
         .when(col('ctis.invoiceDebitMemoCount')>=lit(1),lit(3))\
         .when(col('ctis.invoiceCreditMemoCount')>=lit(1),lit(2))\
         .when(col('ctis.invoiceIntercompanyInvoiceCount')>=lit(1),lit(21))\
         .when(col('ctis.invoiceIntercompanyCreditMemoCount')>=lit(1),lit(50))\
         .otherwise(lit(0)).alias('invoiceToUse')\
        ,when((col('ctis.invoiceSalesInvoiceCount')>=1) & ((col('ctis.invoiceIntercompanyInvoiceCount')>=1)|\
            (col('ctis.invoiceIntercompanyCreditMemoCount')>=1)),lit(3))\
        .when(((col('ctis.invoiceSalesInvoiceCount')>=1) | (col('ctis.invoiceDebitMemoCount')>=1)|(col('ctis.invoiceCreditMemoCount')>=1))\
        	&((col('ctis.invoiceIntercompanyInvoiceCount')==0)&(col('ctis.invoiceIntercompanyCreditMemoCount')==0)),lit(1))\
        .when((col('ctis.invoiceSalesInvoiceCount')==0)&((col('ctis.invoiceIntercompanyInvoiceCount')>=1)|\
          (col('ctis.invoiceIntercompanyCreditMemoCount')>=1)),lit(2)).alias('transactionInvoiceCase'))
    
    otc_L1_STG_40_SalesInvoiceSelector  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_40_SalesInvoiceSelector,'otc','L1_STG_40_SalesInvoiceSelector',False)

    otc_L1_STG_40_SalesFlowTreeDocumentBase.createOrReplaceTempView("otc_L1_STG_40_SalesFlowTreeDocumentBase")
    knw_vw_LK_GD_ClientKPMGDocumentType.createOrReplaceTempView("knw_vw_LK_GD_ClientKPMGDocumentType")
    otc_L1_STG_40_SalesInvoiceSelector.createOrReplaceTempView("otc_L1_STG_40_SalesInvoiceSelector")
    
    otc_L1_TMP_40_SalesFlowTreeDocumentInconsistency = spark.sql("SELECT \
			 SFTB.salesTransactionID as salestransactionID\
			,SFTB.salesOrderSubTransactionID as salesOrderSubTransactionID\
			,SFTB.documentTypeClientProcessID as documentTypeClientProcessID\
			,count( DISTINCT \
					CASE WHEN (ST.documentTypeKPMGGroupID <> 3) OR (ST.documentTypeKPMGGroupID = 3 AND \
                                                                    SIS.invoiceToUse = ST.documentTypeKPMGID)\
							THEN nullif(SFTB.salesDocumentCurrency,'')\
					ELSE NULL\
																END) as salesDocumentCurrencyCount\
			,count( DISTINCT \
							CASE WHEN (ST.documentTypeKPMGGroupID <> 3) OR (ST.documentTypeKPMGGroupID = 3 AND \
                                          SIS.invoiceToUse = ST.documentTypeKPMGID)\
																		THEN nullif(SFTB.salesDocumentSU,'')\
																		ELSE NULL\
																END) as salesDocumentSUCount\
			,count( DISTINCT \
								CASE WHEN (ST.documentTypeKPMGGroupID <> 3) OR (ST.documentTypeKPMGGroupID = 3 AND \
                                                             SIS.invoiceToUse = ST.documentTypeKPMGID)\
																		THEN nullif(SFTB.salesDocumentSKU,'')\
																		ELSE NULL\
																END) as salesDocumentSKUCount	\
			,count( DISTINCT \
							CASE WHEN  (ST.documentTypeKPMGGroupID = 3 AND SIS.invoiceToUse = ST.documentTypeKPMGID) \
																 		THEN nullif(SFTB.salesDocumentCurrency,'')\
																 		ELSE NULL \
																 END) as salesDocumentCurrencyCountExcludingOrdering\
			,count( DISTINCT \
							CASE WHEN  (ST.documentTypeKPMGGroupID = 3 AND SIS.invoiceToUse = ST.documentTypeKPMGID) \
                                                                OR ST.documentTypeKPMGGroupID = 2\
																		THEN nullif(SFTB.salesDocumentSU,'')\
																		ELSE NULL\
																END) as salesDocumentSUCountExcludingOrdering\
			,count( DISTINCT \
									CASE WHEN  (ST.documentTypeKPMGGroupID = 3 AND SIS.invoiceToUse = ST.documentTypeKPMGID)\
                                                                OR ST.documentTypeKPMGGroupID = 2\
																		THEN nullif(SFTB.salesDocumentSKU,'')\
																		ELSE NULL\
																END)as salesDocumentSKUCountExcludingOrdering	\
			 FROM \
				otc_L1_STG_40_SalesFlowTreeDocumentBase SFTB\
			 INNER JOIN \
				knw_vw_LK_GD_ClientKPMGDocumentType ST ON \
													(\
														ST.documentTypeClientID = SFTB.documentTypeClientID\
													)\
			 LEFT JOIN \
				otc_L1_STG_40_SalesInvoiceSelector SIS ON\
													(\
															SIS.salesTransactionID = SFTB.salesTransactionID\
														AND SIS.orderSubTransactionID = SFTB.salesOrderSubTransactionID\
													)\
			 WHERE \
				ST.isMatchingRelevant		= 1\
			 GROUP BY \
				 SFTB.salesTransactionID\
				,SFTB.salesOrderSubTransactionID\
				,SFTB.documentTypeClientProcessID\
			 HAVING	count( DISTINCT \
					CASE WHEN (ST.documentTypeKPMGGroupID <> 3) OR (ST.documentTypeKPMGGroupID = 3 AND \
                                                                    SIS.invoiceToUse = ST.documentTypeKPMGID)\
																 THEN nullif(SFTB.salesDocumentCurrency,'')\
																 ELSE NULL\
															END)    <> 1 \
			 OR		count( DISTINCT \
                          CASE WHEN (ST.documentTypeKPMGGroupID <> 3) OR (ST.documentTypeKPMGGroupID = 3 AND \
                                                                          SIS.invoiceToUse = ST.documentTypeKPMGID)\
																 THEN nullif(SFTB.salesDocumentSU,'')\
																 ELSE NULL\
															END)	<> 1 \
			 OR		count( DISTINCT \
								CASE WHEN (ST.documentTypeKPMGGroupID <> 3) OR (ST.documentTypeKPMGGroupID = 3 AND \
                                                                                SIS.invoiceToUse = ST.documentTypeKPMGID)\
																 THEN nullif(SFTB.salesDocumentSKU,'')\
																 ELSE NULL\
															END)	<> 1\
			OR		count( DISTINCT \
							CASE WHEN  (ST.documentTypeKPMGGroupID = 3 AND SIS.invoiceToUse = ST.documentTypeKPMGID)\
																	THEN nullif(SFTB.salesDocumentCurrency,'')\
																	ELSE NULL\
															END)	> 1\
			OR		count( DISTINCT \
							CASE WHEN  (ST.documentTypeKPMGGroupID = 3 AND SIS.invoiceToUse = ST.documentTypeKPMGID)\
                          OR ST.documentTypeKPMGGroupID = 2\
																	THEN nullif(SFTB.salesDocumentSU,'')\
																	ELSE NULL\
															END)	 > 1\
			OR		count( DISTINCT \
						CASE WHEN  (ST.documentTypeKPMGGroupID = 3 AND SIS.invoiceToUse = ST.documentTypeKPMGID) \
                          OR ST.documentTypeKPMGGroupID = 2\
																	THEN nullif(SFTB.salesDocumentSKU,'')\
																	ELSE NULL\
															END)	 > 1")

    UDP_SalesFlowTreeDocumentBase_12 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
        .join(otc_L1_TMP_40_SalesFlowTreeDocumentInconsistency.alias('stdi1'),\
             ((col('SFTB.salesTransactionID')==(col('stdi1.salesTransactionID')))&\
             (col('SFTB.salesOrderSubTransactionID')==(col('stdi1.salesOrderSubTransactionID')))&\
             (col('SFTB.documentTypeClientProcessID')==(col('stdi1.documentTypeClientProcessID')))),'inner')\
        .filter(col('stdi1.salesDocumentCurrencyCount') != 1 )\
        .select(col('SFTB.childID'),lit(True).alias('vr_isCurrencyUnitInconsistent'))

    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
          .join(UDP_SalesFlowTreeDocumentBase_12.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isCurrencyUnitInconsistent')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isCurrencyUnitInconsistent'))\
                       .otherwise(col('lat.vr_isCurrencyUnitInconsistent')).alias('colDerived'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['isCurrencyUnitInconsistent','vr_isCurrencyUnitInconsistent'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived",'isCurrencyUnitInconsistent').cache()
    
    UDP_SalesFlowTreeDocumentBase_13 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
        .join(otc_L1_TMP_40_SalesFlowTreeDocumentInconsistency.alias('stdi1'),\
              ((col('SFTB.salesTransactionID')==(col('stdi1.salesTransactionID')))&\
             (col('SFTB.salesOrderSubTransactionID')==(col('stdi1.salesOrderSubTransactionID')))&\
             (col('SFTB.documentTypeClientProcessID')==(col('stdi1.documentTypeClientProcessID')))),'inner')\
        .filter(col('stdi1.salesDocumentCurrencyCountExcludingOrdering')>1)\
        .select(col('SFTB.childID'),lit(1).alias('vr_isCurrencyUnitInconsistentExcludingOrderingDocument'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
       .join(UDP_SalesFlowTreeDocumentBase_13.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
       .select(col('org.*'),col('lat.vr_isCurrencyUnitInconsistentExcludingOrderingDocument')\
              ,when(col('lat.childID').isNull()\
                    ,col('org.isCurrencyUnitInconsistentExcludingOrderingDocument'))\
                    .otherwise(col('lat.vr_isCurrencyUnitInconsistentExcludingOrderingDocument')).alias('colDerived'))

    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['isCurrencyUnitInconsistentExcludingOrderingDocument','vr_isCurrencyUnitInconsistentExcludingOrderingDocument'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived",'isCurrencyUnitInconsistentExcludingOrderingDocument').cache()

    UDP_SalesFlowTreeDocumentBase_14 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
       .join(otc_L1_TMP_40_SalesFlowTreeDocumentInconsistency.alias('stdi1'),\
             ((col('SFTB.salesTransactionID')==(col('stdi1.salesTransactionID')))&\
            (col('SFTB.salesOrderSubTransactionID')==(col('stdi1.salesOrderSubTransactionID')))&\
            (col('SFTB.documentTypeClientProcessID')==(col('stdi1.documentTypeClientProcessID')))),'inner')\
       .filter((col('stdi1.salesDocumentSUCount')!=1)&(col('stdi1.salesDocumentSKUCount')!=1))\
       .select(col('SFTB.childID'),lit(1).alias('vr_isQuantityUnitInconsistent'))

    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
          .join(UDP_SalesFlowTreeDocumentBase_14.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isQuantityUnitInconsistent')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isQuantityUnitInconsistent'))\
                       .otherwise(col('lat.vr_isQuantityUnitInconsistent')).alias('colDerived'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['isQuantityUnitInconsistent','vr_isQuantityUnitInconsistent'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived",'isQuantityUnitInconsistent')
    
    UDP_SalesFlowTreeDocumentBase_15 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
        .join(otc_L1_TMP_40_SalesFlowTreeDocumentInconsistency.alias('stdi1'),\
             ((col('SFTB.salesTransactionID')==(col('stdi1.salesTransactionID')))&\
             (col('SFTB.salesOrderSubTransactionID')==(col('stdi1.salesOrderSubTransactionID')))&\
             (col('SFTB.documentTypeClientProcessID')==(col('stdi1.documentTypeClientProcessID')))),'inner')\
        .filter(((col('stdi1.salesDocumentSUCountExcludingOrdering')>1)&(col('stdi1.salesDocumentSKUCountExcludingOrdering')>1)))\
        .select(col('SFTB.childID'),lit(1).alias('vr_isQuantityUnitInconsistentExcludingOrderingDocument'))

    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
          .join(UDP_SalesFlowTreeDocumentBase_15.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isQuantityUnitInconsistentExcludingOrderingDocument')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isQuantityUnitInconsistentExcludingOrderingDocument'))\
                       .otherwise(col('lat.vr_isQuantityUnitInconsistentExcludingOrderingDocument')).alias('colDerived'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['isQuantityUnitInconsistentExcludingOrderingDocument','vr_isQuantityUnitInconsistentExcludingOrderingDocument'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived",'isQuantityUnitInconsistentExcludingOrderingDocument').cache()
    
    CTE_40_SalesFlowTreeDocumentShipping =otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTB')\
         .filter(expr("SFTB.sourceTypeName ='Shipping' AND SFTB.hasTransport =0"))\
         .groupBy("SFTB.salesOrderSubTransactionID","SFTB.salesTransactionID")\
         .agg(expr("count(*) as numberOfDeliveries")\
         ,expr("sum(CASE WHEN (salesDocumentQuantityForMatchingSU<>0 OR (salesDocumentQuantityForMatchingSU=0 AND salesDocumentQuantityForMatchingSKU=0)) THEN 1 ELSE 0 END) as numberOfDeliveriesSUGTZero")\
         ,expr("sum(CASE WHEN (salesDocumentQuantityForMatchingSKU<>0 OR (salesDocumentQuantityForMatchingSU=0 AND salesDocumentQuantityForMatchingSKU=0)) THEN 1 ELSE 0 END) as numberOfDeliveriesSKUGTZero"))\
         .select(col('SFTB.salesOrderSubTransactionID').alias('orderSubTransactionID')\
                 ,col('SFTB.salesTransactionID').alias('salesTransactionID')\
         ,'numberOfDeliveries','numberOfDeliveriesSUGTZero','numberOfDeliveriesSKUGTZero')

    otc_L1_TMP_40_InspectTreeQuantitativeAnalysisShipping = CTE_40_SalesFlowTreeDocumentShipping.alias('cte')\
      .select(col('cte.orderSubTransactionID'),col('cte.salesTransactionID')\
             ,when((col('cte.numberOfDeliveries')==(col('cte.numberOfDeliveriesSUGTZero'))),lit(True)).otherwise(lit(False))\
                .alias('inspectTreeQuantitativeAnalysisDeliverySUIsConsistent')\
             ,when((col('cte.numberOfDeliveries')==(col('cte.numberOfDeliveriesSKUGTZero'))),lit(True)).otherwise(lit(False))\
                .alias('inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent'))

    if objGenHelper.gen_lk_cd_parameter_get('GLOBAL','FixedThresholdUpperBoundary')==None:
        fixedThresholdUpperBoundary = 100
        fixedThresholdUpperBoundary = int(fixedThresholdUpperBoundary)
    else:
        fixedThresholdUpperBoundary = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','FixedThresholdUpperBoundary')

    if objGenHelper.gen_lk_cd_parameter_get('GLOBAL','FixedThresholdValue')==None:
        FixedThresholdValue = 0.01
        FixedThresholdValue = format(FixedThresholdValue,'.6f')
        FixedThresholdValue = Decimal(FixedThresholdValue)
    else:
        FixedThresholdValue = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','FixedThresholdValue')==None
    
    if objGenHelper.gen_lk_cd_parameter_get('GLOBAL','VariableThresholdFactor')==None:
        variableThresholdFactor = 0.0002
        variableThresholdFactor = format(variableThresholdFactor,'.6f')
        variableThresholdFactor = Decimal(variableThresholdFactor)
    else:
        variableThresholdFactor = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','VariableThresholdFactor')

    CTE_40_SalesFlowTreeDocumentOrderBilling = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('SFTDB')\
                    .filter(col('SFTDB.sourceTypeName').isin('Order','Billing','Purchase Order'))\
                    .select(col('SFTDB.salesOrderSubTransactionID'),col('SFTDB.salesTransactionID'),col('SFTDB.sourceTypeName')\
                           ,col('SFTDB.salesDocumentAmountDC'),col('SFTDB.salesDocumentUnitPriceSUDC')\
                           ,col('SFTDB.salesDocumentQuantityForMatchingSU'),col('SFTDB.salesDocumentUnitPriceSKUDC')\
                           ,col('SFTDB.salesDocumentQuantityForMatchingSKU'))\
                    .groupBy(col('SFTDB.salesOrderSubTransactionID'),col('SFTDB.salesTransactionID'))\
                    .agg(count(lit(1)).alias('numberOfRecords')\
                        ,sum(when(col('SFTDB.sourceTypeName')==lit('Billing'),lit(1))\
                             .otherwise(lit(0))).alias('numberOfBillingRecords')\
                        ,sum(when(abs(abs(col('SFTDB.salesDocumentAmountDC')) - \
             (abs(col('SFTDB.salesDocumentUnitPriceSUDC')*(col('SFTDB.salesDocumentQuantityForMatchingSU'))))) <= \
           when((abs(col('SFTDB.salesDocumentAmountDC')) < (lit(fixedThresholdUpperBoundary_))),lit(FixedThresholdValue_))\
                .otherwise(lit(variableThresholdFactor)*(abs(col('SFTDB.salesDocumentAmountDC')))),lit(1))\
           .otherwise(lit(None))).alias('numberOfRecordsSUIsConsistent')\
                        ,sum(when(abs(abs(col('SFTDB.salesDocumentAmountDC')) - \
             (abs(col('SFTDB.salesDocumentUnitPriceSKUDC')*(col('SFTDB.salesDocumentQuantityForMatchingSKU'))))) <= 
           when((abs(col('SFTDB.salesDocumentAmountDC')) < (lit(fixedThresholdUpperBoundary_))),lit(FixedThresholdValue_))\
                .otherwise(lit(variableThresholdFactor)*(abs(col('SFTDB.salesDocumentAmountDC')))),lit(1))\
                .otherwise(lit(None))).alias('numberOfRecordsSKUIsConsistent')\
                        ,sum(when(col('SFTDB.sourceTypeName')==lit('Billing'),
           when(abs(abs(col('SFTDB.salesDocumentAmountDC')) - \
             (abs(col('SFTDB.salesDocumentUnitPriceSUDC')*(col('SFTDB.salesDocumentQuantityForMatchingSU'))))) <= 
           when((abs(col('SFTDB.salesDocumentAmountDC')) < (lit(fixedThresholdUpperBoundary_))),lit(FixedThresholdValue_))\
             .otherwise(lit(variableThresholdFactor)*(abs(col('SFTDB.salesDocumentAmountDC')))),lit(1))\
              .otherwise(lit(None))).otherwise(lit(None))).alias('numberOfBillingRecordsSUIsConsistent')\
                        ,sum(when(col('SFTDB.sourceTypeName')==lit('Billing'),
           when(abs(abs(col('SFTDB.salesDocumentAmountDC')) - \
               (abs(col('SFTDB.salesDocumentUnitPriceSKUDC')*(col('SFTDB.salesDocumentQuantityForMatchingSKU'))))) <= 
           when((abs(col('SFTDB.salesDocumentAmountDC')) < (lit(fixedThresholdUpperBoundary_))),lit(FixedThresholdValue_))\
             .otherwise(lit(variableThresholdFactor)*(abs(col('SFTDB.salesDocumentAmountDC')))),lit(1))\
             .otherwise(lit(None))).otherwise(lit(None))).alias('numberOfBillingRecordsSKUIsConsistent'))\
                    .select(col('SFTDB.salesOrderSubTransactionID').alias('orderSubTransactionID')\
                            ,col('SFTDB.salesTransactionID').alias('TransactionID')\
                            ,'numberOfRecords','numberOfBillingRecords','numberOfRecordsSUIsConsistent'\
                           ,'numberOfRecordsSKUIsConsistent','numberOfBillingRecordsSUIsConsistent','numberOfBillingRecordsSKUIsConsistent')
    
    otc_L1_TMP_40_InspectTreeQuantitativeAnalysisOrderBilling  = CTE_40_SalesFlowTreeDocumentOrderBilling.alias('cte')\
           .select(col('cte.orderSubTransactionID').alias('orderSubTransactionID')\
                  ,col('cte.TransactionID').alias('salesTransactionID')\
                  ,when((col('cte.numberOfRecords')==(col('cte.numberOfRecordsSUIsConsistent'))),lit(True)).otherwise(lit(False))\
                   .alias('inspectTreeQuantitativeAnalysisOrderingBillingSUIsConsistent')\
                  ,when((col('cte.numberOfRecords')==(col('cte.numberOfRecordsSKUIsConsistent'))),lit(True)).otherwise(lit(False))\
                  .alias('inspectTreeQuantitativeAnalysisOrderingBillingSKUIsConsistent')\
                  ,when((col('cte.numberOfBillingRecords')==(col('cte.numberOfBillingRecordsSUIsConsistent'))),lit(True)).otherwise(lit(False))\
                  .alias('inspectTreeQuantitativeAnalysisBillingSUIsConsistent')\
                  ,when((col('cte.numberOfBillingRecords')==(col('cte.numberOfBillingRecordsSKUIsConsistent'))),lit(True)).otherwise(lit(False))\
                  .alias('inspectTreeQuantitativeAnalysisBillingSKUIsConsistent'))
    
    otc_L1_STG_40_InspectTreeQuantitativeAnalysis = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias("slf40")\
      .join(otc_L1_TMP_40_InspectTreeQuantitativeAnalysisShipping.alias("itqad"),\
	     ((col('slf40.salesOrderSubTransactionID').eqNullSafe(col('itqad.orderSubTransactionID')))&\
	     (col('slf40.salesTransactionID').eqNullSafe(col('itqad.salesTransactionID')))),'left')\
      .join(otc_L1_TMP_40_InspectTreeQuantitativeAnalysisOrderBilling.alias("itqaob"),\
           ((col('slf40.salesOrderSubTransactionID').eqNullSafe(col('itqaob.orderSubTransactionID')))&\
           (col('slf40.salesTransactionID').eqNullSafe(col('itqaob.salesTransactionID')))),'left')\
      .join(otc_L1_TMP_40_SalesFlowTreeDocumentInconsistency.alias("stdi1"),\
              ((col('slf40.salesTransactionID').eqNullSafe(col('stdi1.salesTransactionID')))&\
              (col('slf40.salesOrderSubTransactionID').eqNullSafe(col('stdi1.salesOrderSubTransactionID')))&\
              (col('slf40.documentTypeClientProcessID').eqNullSafe(col('stdi1.documentTypeClientProcessID')))),'left')\
      .select(col('slf40.salesOrderSubTransactionID').alias('orderSubTransactionID')\
          ,col('slf40.salesTransactionID').alias('salesTransactionID')\
          ,when(col('itqad.inspectTreeQuantitativeAnalysisDeliverySUIsConsistent').isNull(),lit(False))\
                     .otherwise(col('itqad.inspectTreeQuantitativeAnalysisDeliverySUIsConsistent'))\
                     .alias('inspectTreeQuantitativeAnalysisDeliverySUIsConsistent')\
          ,when(col('itqad.inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent').isNull(),lit(False))\
                     .otherwise(col('itqad.inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent'))\
                     .alias('inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent')\
          ,when(col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSUIsConsistent').isNull(),lit(False))\
                     .otherwise(col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSUIsConsistent'))\
                     .alias('inspectTreeQuantitativeAnalysisOrderingBillingSUIsConsistent')\
          ,when(col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSKUIsConsistent').isNull(),lit(False))\
                     .otherwise(col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSKUIsConsistent'))\
                     .alias('inspectTreeQuantitativeAnalysisOrderingBillingSKUIsConsistent')\
          ,when((((col('itqad.inspectTreeQuantitativeAnalysisDeliverySUIsConsistent')==1)|\
	  	       (col('itqad.inspectTreeQuantitativeAnalysisDeliverySUIsConsistent').isNull()))\
	  		   &((col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSUIsConsistent')==1)|\
	  		    (col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSUIsConsistent').isNull()))\
	  		   &(when(col('stdi1.salesDocumentSUCount').isNull(),lit(1)).otherwise(col('stdi1.salesDocumentSUCount'))==lit(1))),lit('SU'))\
	  	   .when((((col('itqad.inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent')==1)|\
	  	    (col('itqad.inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent').isNull()))\
	  	    &((col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSKUIsConsistent')==1)|\
	  	     (col('itqaob.inspectTreeQuantitativeAnalysisOrderingBillingSKUIsConsistent').isNull()))\
	  	    &(when(col('stdi1.salesDocumentSKUCount').isNull(),lit(1)).otherwise(col('stdi1.salesDocumentSUCount'))==lit(1)))\
                   ,lit('SKU'))\
           .otherwise(lit('NONE')).alias('salesDocumentSUSKUChoice')\
          ,when((((col('itqad.inspectTreeQuantitativeAnalysisDeliverySUIsConsistent')==1)|\
	  	       (col('itqad.inspectTreeQuantitativeAnalysisDeliverySUIsConsistent').isNull()))\
	  		   &((col('itqaob.inspectTreeQuantitativeAnalysisBillingSUIsConsistent')==1)|\
	  		    (col('itqaob.inspectTreeQuantitativeAnalysisBillingSUIsConsistent').isNull()))\
	  		   &(when(col('stdi1.salesDocumentSUCountExcludingOrdering').isNull(),lit(1))\
                      .otherwise(col('stdi1.salesDocumentSUCountExcludingOrdering'))==lit(1))),lit('SU'))\
	  	   .when((((col('itqad.inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent')==1)|\
	  	       (col('itqad.inspectTreeQuantitativeAnalysisDeliverySKUIsConsistent').isNull()))\
	  		   &((col('itqaob.inspectTreeQuantitativeAnalysisBillingSKUIsConsistent')==1)|\
	  		    (col('itqaob.inspectTreeQuantitativeAnalysisBillingSKUIsConsistent').isNull()))\
	  		   &(when(col('stdi1.salesDocumentSKUCountExcludingOrdering').isNull(),lit(1))\
                   .otherwise(col('stdi1.salesDocumentSKUCountExcludingOrdering'))==lit(1))),lit('SKU'))\
	  	    .otherwise(lit('NONE')).alias('salesDocumentSUSKUChoiceBasedOnBilling')\
          ,when(col('itqaob.inspectTreeQuantitativeAnalysisBillingSUIsConsistent').isNull(),lit(False))\
	        .otherwise(col('itqaob.inspectTreeQuantitativeAnalysisBillingSUIsConsistent'))\
	         .alias('inspectTreeQuantitativeAnalysisBillingSUIsConsistent')\
	      ,when(col('itqaob.inspectTreeQuantitativeAnalysisBillingSKUIsConsistent').isNull(),lit(False))\
	         .otherwise(col('itqaob.inspectTreeQuantitativeAnalysisBillingSKUIsConsistent'))\
	        .alias('inspectTreeQuantitativeAnalysisBillingSKUIsConsistent'))\
          .distinct()
    
    otc_L1_STG_40_InspectTreeQuantitativeAnalysis  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_40_InspectTreeQuantitativeAnalysis,'otc','L1_STG_40_InspectTreeQuantitativeAnalysis',False)

    UDP_SalesFlowTreeDocumentBase_16 = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('sftd1')\
        .join(otc_L1_STG_40_InspectTreeQuantitativeAnalysis.alias('inqa1'),\
             ((col('sftd1.salesOrderSubTransactionID')==(col('inqa1.orderSubTransactionID')))&\
             (col('sftd1.salesTransactionID')==(col('inqa1.salesTransactionID')))),'inner')\
        .select(col('sftd1.childID'),col('inqa1.salesDocumentSUSKUChoice').alias('vr_salesDocumentSUSKUChoice')\
               ,col('inqa1.salesDocumentSUSKUChoiceBasedOnBilling').alias('vr_salesDocumentSUSKUChoiceBasedOnBilling'))

    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
          .join(UDP_SalesFlowTreeDocumentBase_16.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_salesDocumentSUSKUChoice'),col('lat.vr_salesDocumentSUSKUChoiceBasedOnBilling')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.salesDocumentSUSKUChoice'))\
                       .otherwise(col('lat.vr_salesDocumentSUSKUChoice')).alias('colDerived')\
    			 ,when(col('lat.childID').isNull()\
                       ,col('org.salesDocumentSUSKUChoiceBasedOnBilling'))\
                       .otherwise(col('lat.vr_salesDocumentSUSKUChoiceBasedOnBilling')).alias('colDerived2'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['salesDocumentSUSKUChoice','vr_salesDocumentSUSKUChoice',\
    				 'salesDocumentSUSKUChoiceBasedOnBilling','vr_salesDocumentSUSKUChoiceBasedOnBilling'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived","salesDocumentSUSKUChoice")\
                        .withColumnRenamed("colDerived2","salesDocumentSUSKUChoiceBasedOnBilling").cache()
    
    firstDayOfFinancialYear = datetime(finYear, 1, 1).date()
    periodeStartDate = firstDayOfFinancialYear.replace(month=firstDayOfFinancialYear.month)
    periodeStartDate =  str(periodeStartDate)
    
    if(periodStart < periodEnd):
        firstDayOfFinancialYear = str(firstDayOfFinancialYear)
        new_date = pd.to_datetime(firstDayOfFinancialYear)+pd.DateOffset(months=periodEnd)
        new_date=str(new_date.date())
        periodeEndDate = pd.to_datetime(new_date)-pd.DateOffset(days=1)
        periodeEndDate = str(periodeEndDate.date())
    else:
        FirstDayOfFinancialEndYear = str(datetime(finYear+1, 1, 1).date())
        new_date2 = pd.to_datetime(FirstDayOfFinancialEndYear)+pd.DateOffset(months=periodEnd)
        new_date2 = str(new_date2.date())
        periodeEndDate = pd.to_datetime(new_date2)-pd.DateOffset(days=1)
        periodeEndDate = str(periodeEndDate.date())
        
    periodeStartDateINT = pd.to_datetime(periodeStartDate).strftime('%Y%m%d')
    periodeEndDateINT = pd.to_datetime(periodeEndDate).strftime('%Y%m%d')
    
    
    CTE_InspectTreeFirstOrderingDocumentOfTransaction = otc_L1_STG_40_SalesFlowTreeDocumentBase_sql.alias('slf40')\
           .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('ckdt1'),\
                (col('slf40.documentTypeClientID')==(col('ckdt1.documentTypeClientID'))),'inner')\
           .filter(col('ckdt1.documentTypeKPMGGroup')==lit('Ordering'))\
           .select(col('slf40.salesTransactionID'),col('slf40.salesOrderSubTransactionID'),col('slf40.salesDocumentDateOfEntry'))\
           .groupBy(col('slf40.salesTransactionID'),col('slf40.salesOrderSubTransactionID'))\
           .agg(min(when(col('slf40.salesDocumentDateOfEntry').isNull(),lit('19010101'))\
                   .otherwise(col('slf40.salesDocumentDateOfEntry'))).alias('Min_Value'))\
           .select(col('slf40.salesTransactionID').alias('salesTransactionID')\
                  ,date_format(col('Min_Value'),"yyyyMMdd").cast(IntegerType()).alias('firstsalesDocumentDateOfEntry')\
                  ,col('slf40.salesOrderSubTransactionID').alias('salesOrderSubTransactionID'))
    
    otc_L1_STG_40_InspectTreeFirstOrderingDocumentOfTransaction = CTE_InspectTreeFirstOrderingDocumentOfTransaction.alias('cte')\
           .select(col('cte.salesTransactionID')\
                  ,when((col('cte.firstsalesDocumentDateOfEntry')< lit(periodeStartDateINT)),lit(1))\
                  .when(((col('cte.firstsalesDocumentDateOfEntry')>= lit(periodeStartDateINT))&\
                        (col('cte.firstsalesDocumentDateOfEntry')<= lit(periodeEndDateINT))),lit(2))\
                  .when((col('cte.firstsalesDocumentDateOfEntry')< lit(periodeEndDateINT)),lit(3))\
                  .alias('orderingDocumentTimingID')\
                  ,col('cte.salesOrderSubTransactionID')).distinct()	

    otc_L1_STG_40_InspectTreeFirstOrderingDocumentOfTransaction  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_40_InspectTreeFirstOrderingDocumentOfTransaction,'otc','L1_STG_40_InspectTreeFirstOrderingDocumentOfTransaction',False)

    fin_L1_TMP_RevenueUniqueDocument = fin_L1_STG_Revenue.alias('lsr')\
        .join(fin_L1_TD_Journal_ReferenceId.alias('ltjr'),\
             (col('lsr.billingdocumentnumber').eqNullSafe(col('ltjr.referenceid'))),'left')\
        .filter(col('lsr.billingDocumentNumber').isNotNull())\
        .select(when(col('ltjr.referenceSubledgerDocumentNumber').isNull(),col('lsr.billingDocumentNumber'))\
                    .otherwise(col('ltjr.referenceSubledgerDocumentNumber')).alias('billingDocumentNumber')\
                ,col('lsr.postingDate').alias('postingDate'),col('lsr.financialPeriod').alias('financialPeriod'))\
       .distinct()

    UDP_SalesFlowTreeDocumentBase_17 = otc_L1_STG_40_SalesFlowTreeDocumentBase_sql.alias('sftd1')\
       .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('dtks1'),\
            (col('sftd1.salesDocumentTypeClientID')==(col('dtks1.documentTypeClientID'))),'inner')\
       .join(fin_L1_TMP_RevenueUniqueDocument.alias('revs1'),\
            ((col('sftd1.salesDocumentNumber').eqNullSafe(col('revs1.billingDocumentNumber')))&\
            (col('sftd1.isInTimeFrame')==1)),'left')\
       .join(fin_L1_TMP_RevenueUniqueDocument.alias('revs2'),\
            (col('sftd1.salesDocumentNumber').eqNullSafe(col('revs2.billingDocumentNumber'))),'left')\
       .select(col('sftd1.childID')\
              ,when(((col('sftd1.sourceTypeName')==lit('Billing'))|(col('dtks1.documentTypeKPMGGroupID')==3))&\
	   				       (col('revs1.billingDocumentNumber').isNotNull()),lit(1))\
	   			    .when(((col('sftd1.sourceTypeName')==lit('Billing'))|(col('dtks1.documentTypeKPMGGroupID')==3))&\
	   				       (col('revs1.billingDocumentNumber').isNull()),lit(2))\
	   				.otherwise(lit(0)).alias('vr_isInTimeFrame')\
              ,col('revs2.postingDate').alias('vr_billingDocumentDateRevenuePosting')\
              ,col('revs2.financialPeriod').alias('vr_billingDocumentDateFinancialPeriod')\
              ,year(col('revs2.postingDate')).alias('vr_salesDocumentPostingYear'))
              
    otc_L1_STG_40_SalesFlowTreeDocumentBase_ = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('org')\
          .join(UDP_SalesFlowTreeDocumentBase_17.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isInTimeFrame'),col('lat.vr_billingDocumentDateRevenuePosting')\
                  ,col('vr_billingDocumentDateFinancialPeriod'),col('vr_salesDocumentPostingYear')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isInTimeFrame'))\
                       .otherwise(col('lat.vr_isInTimeFrame')).alias('colDerived')\
    			 ,when(col('lat.childID').isNull()\
                       ,col('org.billingDocumentDateRevenuePosting'))\
                       .otherwise(col('lat.vr_billingDocumentDateRevenuePosting')).alias('colDerived2')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.billingDocumentDateFinancialPeriod'))\
                       .otherwise(col('lat.vr_billingDocumentDateFinancialPeriod')).alias('colDerived3')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.salesDocumentPostingYear'))\
                       .otherwise(col('lat.vr_salesDocumentPostingYear')).alias('colDerived4'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                     .drop(*['isInTimeFrame','vr_isInTimeFrame',\
    				 'billingDocumentDateRevenuePosting','vr_billingDocumentDateRevenuePosting'\
                      'billingDocumentDateFinancialPeriod','vr_billingDocumentDateFinancialPeriod'\
                      'salesDocumentPostingYear','vr_salesDocumentPostingYear'])
    otc_L1_STG_40_SalesFlowTreeDocumentBase = otc_L1_STG_40_SalesFlowTreeDocumentBase\
                        .withColumnRenamed("colDerived",'isInTimeFrame')\
    					.withColumnRenamed("colDerived2",'billingDocumentDateRevenuePosting')\
    					.withColumnRenamed("colDerived3",'billingDocumentDateFinancialPeriod')\
    					.withColumnRenamed("colDerived4",'salesDocumentPostingYear').cache()

    otc_L1_STG_40_SalesFlowTreeDocumentBase  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_40_SalesFlowTreeDocumentBase,'otc','L1_STG_40_SalesFlowTreeDocumentBase',False)
    
    Df_inner_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('sftd1')\
        .select(col('sftd1.salesParentTransactionID')).filter(col('isInTimeFrame')==lit(1))

    UDP_SalesFlowTreeDocumentCore_18 = otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('sftd1')\
        .join(Df_inner_SalesFlowTreeDocumentCore.alias('x'),\
             (col('sftd1.salesParentTransactionID')==(col('x.salesParentTransactionID'))),'leftouter')\
        .select(col('sftd1.childID')\
		        ,when(col('x.salesParentTransactionID').isNotNull(),lit(1))\
				      .otherwise(lit(0)).alias('vr_isTransactionIDInTimeFrame'))

    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('org')\
          .join(UDP_SalesFlowTreeDocumentCore_18.alias('lat'),(col('org.childID')==(col('lat.childID'))),'left')\
          .select(col('org.*'),col('lat.vr_isTransactionIDInTimeFrame')\
                 ,when(col('lat.childID').isNull()\
                       ,col('org.isTransactionIDInTimeFrame'))\
                       .otherwise(col('lat.vr_isTransactionIDInTimeFrame')).alias('colDerived'))
    
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                     .drop(*['isTransactionIDInTimeFrame','vr_isTransactionIDInTimeFrame'])
    otc_L1_STG_40_SalesFlowTreeDocumentCore = otc_L1_STG_40_SalesFlowTreeDocumentCore\
                        .withColumnRenamed("colDerived",'isTransactionIDInTimeFrame').cache()
    
    otc_L1_STG_40_SalesFlowTreeDocumentCore  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_40_SalesFlowTreeDocumentCore,'otc','L1_STG_40_SalesFlowTreeDocumentCore',False)

    otc_L1_STG_40_SalesSoDBase = otc_L1_STG_40_SalesFlowTreeDocumentBase.alias('sftb40')\
                 .join(otc_L1_STG_40_SalesFlowTreeDocumentCore.alias('sftcore40'),\
				       (col('sftb40.childID').eqNullSafe(col('sftcore40.childID'))),'left')\
				 .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('dtks1'),\
				      (col('sftcore40.salesDocumentTypeClientID').eqNullSafe(col('dtks1.documentTypeClientID'))),'left')\
				 .filter((col('dtks1.isSoDRelevant')==lit(1))&\
				         (col('sftb40.isInTimeFrame')!=lit(2)))\
				 .select(lit(1).alias('processID'),col('sftcore40.salesTransactionID').alias('transactionID')\
				         ,col('sftcore40.childID').alias('childID'),col('sftcore40.DocumentCompanyCode').alias('CompanyCode')\
						 ,col('sftcore40.salesDocumentNumber').alias('documentNumber')\
                         ,col('sftcore40.salesDocumentLineItem').alias('documentLineItem')\
						 ,lit('').alias('DocumentPostingYear'),lit('').alias('DocumentAdditionalFilter')\
						 ,lit('').alias('DocumentPlaceHolderFilter'),col('sftcore40.DocumentUser').alias('DocumentCreationUser')\
						 ,col('sftb40.salesDocumentDateOfEntry').alias('DocumentCreationDate')\
						 ,col('sftb40.sourceTypeName').alias('sourceTypeName')\
						 ,col('sftcore40.salesDocumentTypeKPMG').alias('documentTypeKPMG')\
                         ,col('sftcore40.salesDocumentTypeClient').alias('documentTypeClient')\
						 ,col('sftb40.salesDocumentDate').alias('documentDate')\
                         ,col('sftcore40.documentTimeOfEntry').alias('documentDateTimeOfEntry')\
						 ,col('sftb40.salesDocumentQuantityUnitForMatching').alias('documentQuantityUnitForMatching')\
						 ,col('sftb40.salesDocumentQuantityForMatching').alias('DocumentQuantityForMatching')\
						 ,col('sftb40.salesDocumentCurrency').alias('documentCurrency')\
                         ,col('sftb40.salesDocumentAmountDC').alias('documentAmountDC')\
						 ,col('sftb40.salesDocumentAmountLC').alias('documentAmountLC')\
                         ,col('sftb40.salesDocumentAmountRC').alias('documentAmountRC')\
						 ,col('sftb40.revenueAmount').alias('FIAmount'),col('sftb40.revenueAmountRC').alias('FIAmountRC')\
						 ,col('sftb40.isInTimeFrame').alias('isDocumentInTimeFrame'),lit('').alias('isTransactionIDInTimeFrame')\
						 ,col('sftb40.billingDocumentDateRevenuePosting').alias('documentDateFIPosting')\
						 ,col('sftb40.billingDocumentDateFinancialPeriod').alias('documentDateFinancialPeriod')\
						 ,col('sftb40.isArtificial').alias('isArtificial')\
                         ,col('sftcore40.isTransactionArtificial').alias('isTransactionHasArtificial')\
						 ,when(col('sftb40.billingDocumentCancelled').isNull(),lit(False)).otherwise(col('sftb40.billingDocumentCancelled'))\
						 ,col('sftcore40.DocumentProduct').alias('isDocumentCancelled')\
						 ,col('sftcore40.DocumentCustomerSoldTo').alias('DocumentMaterialNumber'),lit('').alias('DocumentCustomerNumber')\
						 ,lit('').alias('DocumentVendorNumber')\
						 ,when(((col('sftcore40.isMaterialNumberInconsistent')==1)|(col('sftb40.isCurrencyUnitInconsistent')==1)\
						|(col('sftb40.isQuantityUnitInconsistent')==1)|(col('sftb40.isCurrencyUnitInconsistentExcludingOrderingDocument')==1)\
						|(col('sftb40.isQuantityUnitInconsistentExcludingOrderingDocument')==1)),lit(1)).otherwise(lit(0))\
						.alias('isDataQualityIssues')) 

    otc_L1_STG_40_SalesSoDBase  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_40_SalesSoDBase,'otc','L1_STG_40_SalesSoDBase',False)
    

    executionStatus = "otc_L1_STG_40_SalesFlowTreeDocument_populate populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

