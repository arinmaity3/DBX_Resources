# Databricks notebook source
from pyspark.sql.functions import *

def otc_L2_DIM_InternationalCommercialTerm_populate():
  try:
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global otc_L2_DIM_InternationalCommercialTermChangeLog

      otc_L2_TMP_InternationalCommercialTermChangeLogDetailDocumentCore = spark.sql("""

            SELECT DISTINCT
                        salesDocumentNumber
                    ,DocumentCompanyCode
                    ,billingInternationalCommercialTerm
                    ,shippingInternationalCommercialTerm
            FROM
                    otc_L1_STG_40_SalesFlowTreeDocumentCore
            """)
      otc_L2_TMP_InternationalCommercialTermChangeLogDetailDocumentCore.createOrReplaceTempView('otc_L2_TMP_InternationalCommercialTermChangeLogDetailDocumentCore')
      
      otc_L2_DIM_InternationalCommercialTermChangeLog = spark.sql("""

            SELECT DISTINCT 
		                 DENSE_RANK() OVER(ORDER BY slf40.salesDocumentNumber ASC) as internationCommercialTermChangeLogSurrogateKey
		                ,slf40.salesDocumentNumber as salesDocumentNumber
			            ,coalesce(iccl.incoTermInitialValue,'#NA#') as salesDocumentincoTermInitialValue
			            ,concat(coalesce(iccl.incoTermInitialValue,'#NA#'), ' - ', COALESCE(ictmd3a.description,ictmd3b.description,'#NA#')) as salesDocumentincoTermInitialValueName
			            ,COALESCE(ictmd3a.description,ictmd3b.description,'#NA#') as salesDocumentincoTermInitialValueDescription
		                ,COALESCE(slf40.billingInternationalCommercialTerm,slf40.shippingInternationalCommercialTerm,'#NA#') as salesDocumentIncoTermActualValue
		                ,concat(COALESCE(slf40.billingInternationalCommercialTerm,slf40.shippingInternationalCommercialTerm,'#NA#'), ' - ', COALESCE(ictmd4a.description,ictmd4b.description,'#NA#')) as salesDocumentIncoTermActualValueName  
		                ,COALESCE(ictmd4a.description,ictmd4b.description,'#NA#') as salesDocumentIncoTermActualValueDescription		    
			            ,coalesce(iccl.incoTermChangeValueNew,'#NA#') as salesDocumentIncoTermNewValue
		                ,concat(coalesce(iccl.incoTermChangeValueNew,'#NA#'), ' - ', COALESCE(ictmd1a.description,ictmd1b.description,'#NA#')) as salesDocumentIncoTermNewValueName
		                ,COALESCE(ictmd1a.description,ictmd1b.description, '#NA#') as salesDocumentIncoTermNewValueDescription
		                ,COALESCE(iccl.incoTermChangeValueOld,'#NA#') as salesDocumentIncoTermLatestOldValue
		                ,concat(COALESCE(iccl.incoTermChangeValueOld,'#NA#'), ' - ', COALESCE(ictmd2a.description,ictmd2b.description,'#NA#')) as salesDocumentIncoTermLatestOldValueName
		                , COALESCE(ictmd2a.description,ictmd2b.description,'#NA#') as salesDocumentIncoTermLatestOldValueDescription
		                ,coalesce(nullif(iccl.incoTermLatestChangeDateTime,''),'1900-01-01 00:00:00.000') as salesDocumentIncoTermLatestChangeDateTime
			            ,coalesce(iccl.incoTermTotalChangeNumber,0) as salesDocumentincoTermTotalChangeNumber
		                ,(CASE WHEN coalesce(sic1.standardIncoterm,'') = '' 
                              THEN 'Is IncoTerm Standard - No' 
                              ELSE 'Is IncoTerm Standard - Yes'
                          END) as isSalesDocumentIncoTermNewValueStandard
		                ,(CASE WHEN coalesce(sic2.standardIncoterm,'') = ''
                               THEN 'Is IncoTerm Standard - No'
                               ELSE 'Is IncoTerm Standard - Yes'
                         END) as isSalesDocumentIncoTermOldValueStandard

            FROM otc_L2_TMP_InternationalCommercialTermChangeLogDetailDocumentCore slf40
            INNER JOIN knw_LK_CD_ReportingSetup lkrs ON slf40.DocumentCompanyCode = lkrs.companyCode
            LEFT JOIN otc_L1_TD_InternationalCommercialTermChangeLog iccl ON iccl.incoTermChangeDocumentNumber = slf40.salesDocumentNumber
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd1a ON (
                                                                         lkrs.clientCode						=	ictmd1a.clientCode
                                                                         AND lkrs.clientDataReportingLanguage   =	ictmd1a.languageCode
                                                                         AND iccl.incoTermChangeValueNew		=   ictmd1a.value
                                                                        )						                                              
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd1b ON (
                                                                         lkrs.clientCode							=	ictmd1b.clientCode
                                                                         AND lkrs.clientDataSystemReportingLanguage	=	ictmd1b.languageCode	
                                                                         AND iccl.incoTermChangeValueNew			=   ictmd1b.value
                                                                        )	
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd2a ON (
                                                                         lkrs.clientCode						=	ictmd2a.clientCode
                                                                         AND lkrs.clientDataReportingLanguage	=	ictmd2a.languageCode	
                                                                         AND iccl.incoTermChangeValueOld		=   ictmd2a.value
                                                                        )	
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd2b ON (
                                                                         lkrs.clientCode							=	ictmd2b.clientCode
                                                                         AND lkrs.clientDataSystemReportingLanguage	=	ictmd2b.languageCode	
                                                                         AND iccl.incoTermChangeValueOld			=   ictmd2b.value
                                                                        )	
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd3a ON (
                                                                         lkrs.clientCode						=	ictmd3a.clientCode
                                                                         AND lkrs.clientDataReportingLanguage	=	ictmd3a.languageCode	
                                                                         AND iccl.incoTermInitialValue			=   ictmd3a.value
                                                                        )	
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd3b ON (
                                                                         lkrs.clientCode							=	ictmd3b.clientCode
                                                                         AND lkrs.clientDataSystemReportingLanguage	=	ictmd3b.languageCode
                                                                         AND iccl.incoTermInitialValue				=   ictmd3b.value
                                                                        )	
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd4a ON (
                                                                         lkrs.clientCode																					=	ictmd4a.clientCode
                                                                         AND lkrs.clientDataReportingLanguage																=	ictmd4a.languageCode	
                                                                         AND COALESCE(slf40.billingInternationalCommercialTerm,slf40.shippingInternationalCommercialTerm)	=   ictmd4a.value
                                                                        )	
								             
            LEFT JOIN otc_L1_MD_InternationalCommercialterm ictmd4b ON (
                                                                           lkrs.clientCode																					=	ictmd4b.clientCode
                                                                           AND lkrs.clientDataSystemReportingLanguage														=	ictmd4b.languageCode	
                                                                           AND COALESCE(slf40.billingInternationalCommercialTerm,slf40.shippingInternationalCommercialTerm)	=   ictmd4b.value
                                                                          )	
            LEFT JOIN knw_LK_GD_StandardInternationalCommercialTerm	sic1 ON (
                                                                                 UPPER(iccl.incoTermChangeValueNew) = UPPER(sic1.standardIncoterm)
                                                                                )							
            LEFT JOIN knw_LK_GD_StandardInternationalCommercialTerm	sic2 ON (
                                                                                 UPPER(iccl.incoTermChangeValueOld) = UPPER(sic2.standardIncoterm)
                                                                                )	
            WHERE iccl.incoTermChangeDocumentNumber	IS NOT NULL
            """)

      otc_L2_DIM_InternationalCommercialTermChangeLog.createOrReplaceTempView("otc_L2_DIM_InternationalCommercialTermChangeLog")

      otc_L2_DIM_InternationalCommercialTermChangeLog = objDataTransformation.gen_convertToCDMandCache(otc_L2_DIM_InternationalCommercialTermChangeLog,'otc','L2_DIM_InternationalCommercialTermChangeLog',True)

      otc_vw_DIM_InternationalCommercialTermChangeLog = spark.sql("""

            SELECT	  
	                     internationCommercialTermChangeLogSurrogateKey
                        ,salesDocumentNumber
			            ,salesDocumentincoTermInitialValue
			            ,salesDocumentincoTermInitialValueName
			            ,salesDocumentincoTermInitialValueDescription
                        ,salesDocumentIncoTermActualValue	
                        ,salesDocumentIncoTermActualValueName				
                        ,salesDocumentIncoTermActualValueDescription	
                        ,salesDocumentIncoTermNewValue					
                        ,salesDocumentIncoTermNewValueName				
                        ,salesDocumentIncoTermNewValueDescription	
                        ,salesDocumentIncoTermLatestOldValue			
                        ,salesDocumentIncoTermLatestOldValueName		
                        ,salesDocumentIncoTermLatestOldValueDescription 
                        ,salesDocumentIncoTermLatestChangeDateTime
			            ,salesDocumentincoTermTotalChangeNumber
			            ,'Incoterms Changed' as salesDocumentincoTermTotalChangeStatus 
                        ,isSalesDocumentIncoTermNewValueStandard		
                        ,isSalesDocumentIncoTermOldValueStandard
	            FROM otc_L2_DIM_InternationalCommercialTermChangeLog
 	
	            UNION ALL
	
	            --#Technical_otc_vw_DIM_InternationalCommercialTermChangeLog_msalim_20200317_1140
	            SELECT
		                 cast(0 as integer) AS internationCommercialTermChangeLogSurrogateKey
                        ,'NONE' AS salesDocumentNumber	
			            ,'NONE' AS incoTermInitialValue
			            ,'NONE' AS incoTermInitialValueName
			            ,'NONE' AS incoTermInitialValueDescription
                        ,'NONE' AS salesDocumentIncoTermActualValue
                        ,'NONE' AS salesDocumentIncoTermActualValueName			
                        ,'NONE' AS salesDocumentIncoTermActualValueDescription
                        ,'NONE' AS salesDocumentIncoTermNewValue				
                        ,'NONE' AS salesDocumentIncoTermNewValueName				
                        ,'NONE' AS salesDocumentIncoTermNewValueDescription		    
                        ,'NONE' AS salesDocumentIncoTermLatestOldValue		    
                        ,'NONE' AS salesDocumentIncoTermLatestOldValueName		    
                        ,'NONE' AS salesDocumentIncoTermLatestOldValueDescription
                        ,to_timestamp('1900-01-01 00:00:00.000', 'yyyy-MM-dd HH:mm:ss.SSS') as salesDocumentIncoTermLatestChangeDateTime
			            ,0 AS salesDocumentincoTermTotalChangeNumber
			            ,'Incoterms Not Changed' AS salesDocumentincoTermTotalChangeStatus
                        ,'NOEN' AS isSalesDocumentIncoTermNewValueStandard
                        ,'NONE' AS isSalesDocumentIncoTermOldValueStandard



            """)

      objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_InternationalCommercialTermChangeLog,gl_CDMLayer2Path +"otc_vw_DIM_InternationalCommercialTermChangeLog.parquet")

      executionStatus = "L2_DIM_InternationalCommercialTermChangeLog populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
