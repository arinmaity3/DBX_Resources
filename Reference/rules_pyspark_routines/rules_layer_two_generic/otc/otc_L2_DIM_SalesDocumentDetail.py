# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number
from dateutil.parser import parse
from delta.tables import *

def knw_LK_businessDatatypeValue_get(businessDatatype, ERPSystemID, languageCode):
  if ((languageCode == None) and (businessDatatype == None)):
    businessDatatypeValues = spark.sql("""
    
    SELECT   
                 businessDatatype as businessDatatype
				,sourceERPSystemID as ERPSystemID
				,sourceSystemName as sourceSystemName
				,sourceSystemValue as sourceSystemValue
				,targetSystemValue as targetSystemValue
				,targetSystemValueDescription as targetSystemValueDescription	
				,targetLanguageCode as targetLanguageCode
   FROM	knw_LK_GD_BusinessDatatypeValueMapping
    """)
    
  elif((languageCode != None) and (businessDatatype != None)):
      businessDatatypeValues = spark.sql("""
    
          SELECT   
                       businessDatatype as businessDatatype
                      ,sourceERPSystemID as ERPSystemID
                      ,sourceSystemName as sourceSystemName
                      ,sourceSystemValue as sourceSystemValue
                      ,targetSystemValue as targetSystemValue
                      ,targetSystemValueDescription as targetSystemValueDescription	
                      ,targetLanguageCode as targetLanguageCode
         FROM	knw_LK_GD_BusinessDatatypeValueMapping
         where businessDatatype = '{0}' and sourceERPSystemID = '{1}' and targetLanguageCode = '{2}'
     """.format(businessDatatype, ERPSystemID, languageCode))
      
  return businessDatatypeValues


def otc_L2_DIM_SalesOrganization():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global otc_L2_DIM_SalesDocumentDetail

        EndDate = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')
        StartDate = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')
        finYear  = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        
        languageCode = knw_LK_CD_ReportingSetup.select(col("KPMGDataReportingLanguage")).collect()[0][0]
        erpSAPSystemID = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC'))

        joinedData = spark.sql("""
        SELECT DISTINCT
		            SFTC.isTransactionArtificial
		        ,SFTC.isParentTransactionArtificial
		        ,SFTC.isTransactionService
		        ,SFTC.hierarchyTypeName
		        ,SFTC.linkTypeName
		        ,SFTC.indent
                ,(CASE WHEN RECO.billingDocumentNumber IS NULL 
                        THEN SFTC.isRevenueRelevant
                        ELSE (CASE WHEN coalesce(RECO.revenuePostingDate,'1900-01-01')<>'1900-01-01' THEN 1 ELSE 0 END) 
                    END) as isRevenueRelevant
                ,SFTB.isArtificial
		        ,SFTC.isMaterialNumberInconsistent
                ,CASE WHEN MM.priceMatchTypeID IN (0,2) THEN  SFTB.isCurrencyUnitInconsistent ELSE SFTB.isCurrencyUnitInconsistentExcludingOrderingDocument END as isCurrencyUnitInconsistent
		        ,CASE WHEN MM.priceMatchTypeID IN (0,2) THEN  SFTB.isQuantityUnitInconsistent ELSE SFTB.isQuantityUnitInconsistentExcludingOrderingDocument END as isQuantityUnitInconsistent
		        ,SFTB.isTransport
                ,(CASE WHEN RECO.billingDocumentNumber IS NULL 
                        THEN (CASE WHEN SFTB.isInTimeFrame IN (0,1) THEN 1 ELSE 0 END)
                        ELSE (CASE WHEN coalesce(RECO.revenuePostingDate,'1900-01-01') BETWEEN '{0}' AND '{1}' THEN 1 ELSE 0 END)
                    END) as isInTimeFrame
                ,SFTC.DocumentMessageStatus
		        ,SFTC.DocumentMessageType
		        ,SFTB.sourceTypeName
                ,coalesce(NULLIF(SFTC.DocumentPredecessorDocumentTypeClient,''),'#NA#') as DocumentPredecessorDocumentTypeClient		
		        ,coalesce(NULLIF(SFTC.DocumentPredecessordocumentTypeClientDescription,''),'#NA#') as DocumentPredecessordocumentTypeClientDescription
		        ,coalesce(NULLIF(SFTC.DocumentPredecessordocumentTypeKpmg,''),'#NA#') as DocumentPredecessordocumentTypeKpmg
		        ,coalesce(NULLIF(SFTC.DocumentPredecessordocumentTypeKpmgShort,''),'#NA#') as DocumentPredecessordocumentTypeKpmgShort
                ,SFTC.documentVendorPruchasedFrom
		        ,SFTB.salesDocumentSKU
		        ,SFTB.salesDocumentSU
		        ,SFTC.DocumentReturnsItem
		        ,SFTC.orderingDocumentTargetQuantityUnit
		        ,SFTB.hasTransport
		        ,SFTB.salesDocumentQuantityUnitForMatching
                ,CASE WHEN  MM.priceMatchTypeID IN (0,2) THEN SFTB.salesDocumentSUSKUChoice ELSE SFTB.salesDocumentSUSKUChoiceBasedOnBilling END as salesDocumentSUSKUChoice
		        ,coalesce(nullif(SFTC.shippingInternationalCommercialTerm,''),'#NA#') as shippingInternationalCommercialTerm 
		        ,coalesce(nullif(SFTC.shippingInternationalCommercialTermDetail,''),'#NA#') as shippingInternationalCommercialTermDetail
		        ,coalesce(nullif(SFTC.billingInternationalCommercialTerm ,''),'#NA#') as billingInternationalCommercialTerm
		        ,coalesce(nullif(SFTC.billingInternationalCommercialTermDetail,''),'#NA#') as billingInternationalCommercialTermDetail
		        ,coalesce(nullif(SFTC.masterDataInternationalCommercialTerm,''),'#NA#') as masterDataInternationalCommercialTerm
		        ,coalesce(nullif(SFTC.isIncotermChanged,''),'#NA#') as isIncotermChanged
		        FROM
			        otc_L1_STG_40_SalesFlowTreeDocumentBase SFTB
		        INNER JOIN
			        otc_L1_STG_40_SalesFlowTreeDocumentCore SFTC
		        ON 
			        SFTB.childID = SFTC.childID
		        INNER JOIN gen_L1_STG_40_SLFlowTreeDocumentInterpretation FFDI ON
																			        (
																				        FFDI.documentTypeClientProcessID	= SFTB.documentTypeClientProcessID
																			        AND FFDI.ChildID						= SFTB.childID
																			        )
		        INNER JOIN knw_LK_GD_CoreTypeCasePersistentBatchControl MM ON
														        (
															        FFDI.coreTypeCasePersistentID = MM.ID
														        )
		        LEFT JOIN 
			        otc_L1_STG_SLToGLReconciliationResult RECO
		        ON 
			        (
					        RECO.billingDocumentNumber = SFTB.salesDocumentNumber
				        AND (
					            (		RECO.PostedCategoryID					  = 6 
						        AND RECO.SLBillingDocumentMissingCategoryID = 0
					            )
					            OR RECO.SLBillingDocumentMissingCategoryID = 1				
					        )
			        )
        """.format(StartDate, EndDate))

        joinedData.createOrReplaceTempView('joinedData')

        otc_L2_DIM_SalesDocumentDetail_1 = spark.sql("""
			Select row_number() over (ORDER BY (SELECT 0)) as salesDocumentDetailSurrogateKey
				,isTransactionArtificial as isTransactionArtificial
				,isParentTransactionArtificial as isParentTransactionArtificial
				,isTransactionService as isTransactionService
				,coalesce(hierarchyTypeName,'#NA#') as hierarchyTypeName  
				,coalesce(linkTypeName,'#NA#') as linkTypeName
				,coalesce(indent,'#NA#') as indent
				,coalesce(isRevenueRelevant,0) as isRevenueRelevant
				,isArtificial as isArtificial
				,isMaterialNumberInconsistent as isMaterialNumberInconsistent
				,isCurrencyUnitInconsistent as isCurrencyUnitInconsistent
				,isQuantityUnitInconsistent as isQuantityUnitInconsistent
				,isTransport as isTransport
				,isInTimeFrame as isInTimeFrame
				,coalesce(DocumentMessageStatus,'#NA#') as salesDocMessageStatus
				,coalesce(DocumentMessageType,'#NA#') as salesDocMessageType 
				,coalesce(SourceTypeName,'#NA#') as salesDocumentSourceType
				,DocumentPredecessorDocumentTypeClient as salesDocPredecessorDocumentTypeClient
				,DocumentPredecessordocumentTypeClientDescription as salesDocPredecessorDocumentTypeClientDescription
				,DocumentPredecessordocumentTypeKpmg as salesDocPredecessorDocumentTypeKPMG
				,DocumentPredecessordocumentTypeKpmgShort as salesDocPredecessorDocumentTypeKPMGShort
				,coalesce(documentVendorPruchasedFrom,'#NA#') as salesDocumentVendorPurchasedFrom
				,coalesce(salesDocumentSKU,'#NA#') as salesDocumentSKU
				,coalesce(salesDocumentSU,'#NA#') as salesDocumentSU
				,DocumentReturnsItem as salesDocumentReturnsItem
				,coalesce(orderingDocumentTargetQuantityUnit,'#NA#') as OrderingDocTargetQuantityUnit
				,hasTransport as hasTransport
				,coalesce(salesDocumentQuantityUnitForMatching,'#NA#') as salesDocumentQuantityUnitForMatching
				,coalesce(salesDocumentSUSKUChoice,'#NA#') as salesDocumentSUSKUChoice
				,shippingInternationalCommercialTerm as shippingInternationalCommercialTerm
				,shippingInternationalCommercialTermDetail as shippingInternationalCommercialTermDetail
				,billingInternationalCommercialTerm as billingInternationalCommercialTerm
				,billingInternationalCommercialTermDetail as billingInternationalCommercialTermDetail
				,masterDataInternationalCommercialTerm as masterDataInternationalCommercialTerm
				,isIncotermChanged as isIncotermChanged
				From joinedData
			""")

        otc_L2_DIM_SalesDocumentDetail_1.createOrReplaceTempView("otc_L2_DIM_SalesDocumentDetail")
        max_salesDocumentDetailSurrogateKey = spark.sql("SELECT MAX(salesDocumentDetailSurrogateKey) FROM otc_L2_DIM_SalesDocumentDetail").collect()[0][0]

        rowData =[[max_salesDocumentDetailSurrogateKey+1,1,1,0,'#NA#','#NA#','#NA#',0,1,0,0,0,0,1,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#',0,'#NA#',0,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#']
				,[max_salesDocumentDetailSurrogateKey+2,1,1,0,'#NA#','#NA#','#NA#',1,1,0,0,0,0,1,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#',0,'#NA#',0,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#']
				,[max_salesDocumentDetailSurrogateKey+3,1,1,0,'#NA#','#NA#','#NA#',0,1,0,0,0,0,0,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#',0,'#NA#',0,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#']
				,[max_salesDocumentDetailSurrogateKey+4,1,1,0,'#NA#','#NA#','#NA#',1,1,0,0,0,0,0,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#',0,'#NA#',0,'#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#','#NA#']]

        columns = ['salesDocumentDetailSurrogateKey'
					,'isTransactionArtificial'
					,'isParentTransactionArtificial'
					,'isTransactionService'
					,'hierarchyTypeName'
					,'linkTypeName'
					,'indent'
					,'isRevenueRelevant'
					,'isArtificial'
					,'isMaterialNumberInconsistent'
					,'isCurrencyUnitInconsistent'
					,'isQuantityUnitInconsistent'
					,'isTransport'
					,'isInTimeFrame'
					,'salesDocMessageStatus'
					,'salesDocMessageType'
					,'salesDocumentSourceType'
					,'salesDocPredecessorDocumentTypeClient'
					,'salesDocPredecessorDocumentTypeClientDescription'
					,'salesDocPredecessorDocumentTypeKPMG'
					,'salesDocPredecessorDocumentTypeKPMGShort'
					,'salesDocumentVendorPurchasedFrom'
					,'salesDocumentSKU'
					,'salesDocumentSU'
					,'salesDocumentReturnsItem'
					,'OrderingDocTargetQuantityUnit'
					,'hasTransport'
					,'salesDocumentQuantityUnitForMatching'
					,'salesDocumentSUSKUChoice'
					,'shippingInternationalCommercialTerm'
					,'shippingInternationalCommercialTermDetail'
					,'billingInternationalCommercialTerm'
					,'billingInternationalCommercialTermDetail'
					,'masterDataInternationalCommercialTerm'
					,'isIncotermChanged']

        otc_L2_DIM_SalesDocumentDetail_2 = spark.createDataFrame(rowData,columns)
        otc_L2_DIM_SalesDocumentDetail = otc_L2_DIM_SalesDocumentDetail_1.union(otc_L2_DIM_SalesDocumentDetail_2)

        otc_L2_DIM_SalesDocumentDetail.createOrReplaceTempView("otc_L2_DIM_SalesDocumentDetail")
        otc_L2_DIM_SalesDocumentDetail = objDataTransformation.gen_convertToCDMandCache(otc_L2_DIM_SalesDocumentDetail,'otc','L2_DIM_SalesDocumentDetail',True)

        knw_LK_businessDatatypeValue_get('Is Revenue relevant', erpSAPSystemID, languageCode).createOrReplaceTempView('revn')
        knw_LK_businessDatatypeValue_get('Is Artificial', erpSAPSystemID, 'EN').createOrReplaceTempView('arfl')
        knw_LK_businessDatatypeValue_get('Is Material Number Inconsistent', erpSAPSystemID, languageCode).createOrReplaceTempView('mtrl')
        knw_LK_businessDatatypeValue_get('Is Currency Unit Inconsistent', erpSAPSystemID, languageCode).createOrReplaceTempView('cncy')
        knw_LK_businessDatatypeValue_get('Is Quantity Unit Inconsistent', erpSAPSystemID, languageCode).createOrReplaceTempView('qnty')
        knw_LK_businessDatatypeValue_get('Is Transport', erpSAPSystemID, languageCode).createOrReplaceTempView('trnp')
        knw_LK_businessDatatypeValue_get('Is In Time Frame', erpSAPSystemID, languageCode).createOrReplaceTempView('intm')
        knw_LK_businessDatatypeValue_get('Has Transport', erpSAPSystemID, languageCode).createOrReplaceTempView('hstr')
            
        otc_vw_DIM_SalesDocumentDetail = spark.sql("""

			SELECT DISTINCT
				salesDocumentDetailSurrogateKey
				,SDD.hierarchyTypeName
				,SDD.linkTypeName
				,SDD.indent
				,revn.targetSystemValueDescription as isRevenueRelevant
				,arfl.targetSystemValueDescription as isArtificial
				,mtrl.targetSystemValueDescription as isMaterialNumberInconsistent
				,cncy.targetSystemValueDescription as isCurrencyUnitInconsistent
				,qnty.targetSystemValueDescription as isQuantityUnitInconsistent
				,trnp.targetSystemValueDescription as isTransport
				,intm.targetSystemValueDescription as isInTimeFrame
				,hstr.targetSystemValueDescription as hasTransport
				,SDD.salesDocMessageStatus
				,SDD.salesDocMessageType
				,SDD.salesDocumentSourceType
				,SDD.salesDocPredecessorDocumentTypeClient
				,SDD.salesDocPredecessorDocumentTypeClientDescription
				,SDD.salesDocPredecessorDocumentTypeKPMG
				,SDD.salesDocPredecessorDocumentTypeKPMGShort
				,SDD.salesDocumentVendorPurchasedFrom
				,SDD.salesDocumentSKU
				,SDD.salesDocumentSU
				,cast(SDD.salesDocumentReturnsItem as boolean) as salesDocumentReturnsItem
				,SDD.OrderingDocTargetQuantityUnit
				,coalesce(SDD.salesDocumentQuantityUnitForMatching, '') as salesDocumentQuantityUnitForMatching
				,coalesce(SDD.salesDocumentSUSKUChoice,'') as salesDocumentSUSKUChoice
				,SDD.shippingInternationalCommercialTerm
				,SDD.shippingInternationalCommercialTermDetail
				,SDD.billingInternationalCommercialTerm
				,SDD.billingInternationalCommercialTermDetail
				,SDD.masterDataInternationalCommercialTerm
				,SDD.isIncotermChanged 
			from otc_L2_DIM_SalesDocumentDetail SDD
			LEFT JOIN revn on revn.sourceSystemValue = CAST(SDD.isRevenueRelevant as string)
			LEFT JOIN arfl on arfl.sourceSystemValue = CAST(SDD.isArtificial as string)
			LEFT JOIN mtrl on mtrl.sourceSystemValue = CAST(SDD.isMaterialNumberInconsistent as string)
			LEFT JOIN cncy on cncy.sourceSystemValue = CAST(SDD.isCurrencyUnitInconsistent as string)
			LEFT JOIN qnty on qnty.sourceSystemValue = CAST(SDD.isQuantityUnitInconsistent as string)
			LEFT JOIN trnp on trnp.sourceSystemValue = CAST(SDD.isTransport as string)
			LEFT JOIN intm on intm.sourceSystemValue = CAST(SDD.isInTimeFrame as string)
			LEFT JOIN hstr on hstr.sourceSystemValue = CAST(SDD.hasTransport as string)
			LEFT JOIN otc_L2_DIM_InternationalCommercialTerm inco1 ON 
					(
						SDD.shippingInternationalCommercialTerm = inco1.salesDocumentIncoTerm
					AND	SDD.shippingInternationalCommercialTermDetail = inco1.salesDocumentIncoTermDetails
					)
			LEFT JOIN otc_L2_DIM_InternationalCommercialTerm inco2 ON
					(
						SDD.billingInternationalCommercialTerm = inco2.salesDocumentIncoTerm
					AND	SDD.billingInternationalCommercialTermDetail = inco2.salesDocumentIncoTermDetails
					)
      
			UNION ALL

			SELECT
	
			0	as salesDocumentDetailSurrogateKey,							 
			cast('NONE' as string) as hierarchyTypeName,	 
			cast('NONE' as string) as linkTypeName,
			cast('NONE' as string) as indent,
			cast('NONE' as string) as isRevenueRelevant,
			cast('NONE' as string) as isArtificial,
			cast('NONE' as string) as isMaterialNumberInconsistent,
			cast('NONE' as string) as isCurrencyUnitInconsistent,
			cast('NONE' as string) as isQuantityUnitInconsistent,
			cast('NONE' as string) as isTransport,
			cast('NONE' as string) as isInTimeFrame,
			cast('NONE' as string) as hasTransport,
			cast('NONE' as string) as salesDocMessageStatus,
			cast('NONE' as string) as salesDocMessageType,
			cast('NONE' as string) as salesDocumentSourceType,
			cast('NONE' as string) as salesDocPredecessorDocumentTypeClient,
			cast('NONE' as string) as salesDocPredecessorDocumentTypeClientDescription,
			cast('NONE' as string) as salesDocPredecessorDocumentTypeKPMG,
			cast('NONE' as string) as salesDocPredecessorDocumentTypeKPMGShort,
			cast('NONE' as string) as salesDocumentVendorPurchasedFrom,
			cast('NONE' as string) as salesDocumentSKU,
			cast('NONE' as string) as salesDocumentSU,
			cast(0 as boolean) as salesDocumentReturnsItem,
			cast('NONE' as string) as OrderingDocTargetQuantityUnit,
			cast('NONE' as string) as salesDocumentQuantityUnitForMatching,
			cast('NONE' as string) as salesDocumentSUSKUChoice,
			cast('NONE' as string) as shippingInternationalCommercialTerm,
			cast('NONE' as string) as shippingInternationalCommercialTermDetail,
			cast('NONE' as string) as billingInternationalCommercialTerm,
			cast('NONE' as string) as billingInternationalCommercialTermDetail,
			cast('NONE' as string) as masterDataInternationalCommercialTerm,
			cast('NONE' as string) as isIncotermChanged


			""")

        objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_SalesDocumentDetail,gl_CDMLayer2Path +"otc_vw_DIM_SalesDocumentDetail.parquet")

        executionStatus = "L2_DIM_SalesDocumentDetail populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]




