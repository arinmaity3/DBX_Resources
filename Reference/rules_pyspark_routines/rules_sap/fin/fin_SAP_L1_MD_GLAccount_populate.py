# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when

def fin_SAP_L1_MD_GLAccount_populate(): 
    try:
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
      global fin_L1_MD_GLAccount
      
      erpSAPSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')

      fin_SAP_L0_TMP_DistinctAccountNumber = erp_SKA1.alias('skat_erp')\
                .join(knw_LK_CD_ReportingSetup.alias('lkrs')\
                       ,((col('lkrs.clientCode') == col('skat_erp.MANDT'))\
                         &(col('lkrs.chartOfAccount') == col('skat_erp.KTOPL'))),how='inner')\
                .select(col('lkrs.clientCode').alias('clientCode')\
                       ,col('lkrs.companyCode').alias('companyCode')\
                       ,col('lkrs.chartOfAccount').alias('chartOfAccount')\
                        ,when(col('skat_erp.XBILK').isNull(),'').otherwise(col('skat_erp.XBILK'))\
                                                   .alias('accountType')\
                       ,col('skat_erp.SAKNR').alias('accountNumber')\
                       ,col('skat_erp.VBUND').alias('companyID'))\
                .distinct()

      fin_SAP_L0_TMP_DistinctAccountNumber.createOrReplaceTempView("fin_SAP_L0_TMP_DistinctAccountNumber")
      knw_LK_CD_ReportingSetup.createOrReplaceTempView("knw_LK_CD_ReportingSetup")
      erp_SKB1.createOrReplaceTempView("erp_SKB1")
      knw_LK_GD_BusinessDatatypeValueMapping.createOrReplaceTempView("knw_LK_GD_BusinessDatatypeValueMapping")

      XBILK_isnull = when(col('SKA1.XBILK').isNull(),'').otherwise(col('SKA1.XBILK'))\
                                               
      L1_TMP_GLAccountDetail_DF1 = erp_SKB1.alias('SKB1')\
                   .join(erp_T001.alias('T001')\
                       ,((col('SKB1.MANDT') == col('T001.MANDT'))\
                         &(col('SKB1.BUKRS') == col('T001.BUKRS'))),how='inner')\
                   .join(erp_SKA1.alias('SKA1')\
                       ,((col('SKA1.MANDT')== col('SKB1.MANDT'))\
                          &(col('SKB1.SAKNR')== col('SKA1.SAKNR'))\
                          &(col('SKB1.BUKRS')==col('T001.BUKRS'))\
                          &(col('SKA1.KTOPL')==col('T001.KTOPL'))),how='leftouter')\
                   .join(knw_LK_CD_ReportingSetup.alias('repsetup')\
                       ,((col('SKB1.MANDT') == col('repsetup.clientCode'))\
                         &(col('SKB1.BUKRS') == col('repsetup.companyCode'))),how='inner')\
                   .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('usfa')\
                       ,((col('usfa.businessDatatype').eqNullSafe(lit('Account Type Indicator')))\
                         &(col('usfa.sourceSystemValue').eqNullSafe(lit(XBILK_isnull)))\
                          &(col('usfa.targetLanguageCode').eqNullSafe(col('repsetup.KPMGDataReportingLanguage')))\
                          &(col('usfa.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))),how='left')\
                  .select(col('SKB1.MANDT').alias('clientCode')\
                         ,col('T001.BUKRS').alias('companyCode')\
                         ,col('SKB1.SAKNR').alias('accountNumber')\
                         ,col('T001.KTOPL').alias('chartOfAccountID')\
                         ,col('usfa.targetSystemValue').alias('accountType')\
                         ,when(col('SKB1.XINTB')=='X',lit(1)).otherwise(lit(0))\
						        .alias('isOnlyAutomatedPostingAllowed')\
                         ,col('SKA1.VBUND').alias('companyID'))


      L1_TMP_GLAccountDetail_DF1.createOrReplaceTempView("L1_TMP_GLAccountDetail_DF1")
      
      fin_L1_TMP_GLAccountDetail = spark.sql("SELECT	 clientCode,companyCode,accountNumber,chartOfAccountID,\
                                      accountType,isOnlyAutomatedPostingAllowed,companyID \
									  FROM L1_TMP_GLAccountDetail_DF1 \
									 UNION  ALL\
									 SELECT	 accNum.clientCode	AS clientCode		 \
									,accNum.companyCode                   AS companyCode		 \
                                    ,accNum.accountNumber                            AS accountNumber   \
                                    ,repSetup.chartOfAccount                         AS chartOfAccountID     \
                                    ,usfa.targetSystemValue                   AS accountType \
                                    ,CASE WHEN SKB1.XINTB='X' THEN 1 ELSE 0 END	AS isOnlyAutomatedPostingAllowed \
                                    ,companyID \
									FROM	fin_SAP_L0_TMP_DistinctAccountNumber accNum \
					INNER JOIN knw_LK_CD_ReportingSetup repSetup	ON \
														( \
															accNum.clientCode			=repsetup.clientCode \
															AND accNum.companyCode		= repsetup.companyCode \
															AND accNum.chartOfAccount	= repsetup.chartOfAccount \
														) \
					LEFT OUTER JOIN erp_SKB1 SKB1 ON  \
											( \
												SKB1.MANDT	= accNum.clientCode 	AND \
												SKB1.SAKNR	= accNum.accountNumber	AND \
												SKB1.BUKRS	= accNum.companyCode \
											) \
                    LEFT JOIN knw_LK_GD_BusinessDatatypeValueMapping usfa	ON \
											(\
												usfa.businessDatatype			= 'Account Type Indicator' \
												AND usfa.sourceSystemValue		= accNum.accountType\
												AND usfa.targetLanguageCode		= repsetup.KPMGDataReportingLanguage\
												AND usfa.sourceERPSystemID		= '"+str(erpSAPSystemID)+"' \
                                            )	\
					WHERE	NOT EXISTS \
								(                            \
									SELECT  1 \
									FROM	erp_SKB1  SKB1 \
									WHERE	\
											accNum.clientCode	= SKB1.MANDT  \
										AND accNum.companyCode	= SKB1.BUKRS \
										  AND accNum.accountNumber = SKB1.SAKNR  	\
								)")

      fin_L1_TMP_GLAccountDetail.createOrReplaceTempView("fin_L1_TMP_GLAccountDetail")

#update1
      update_1 = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                  .join(knw_clientDataReportingLanguage.alias('rept1')\
                                 ,((col('gld1.clientCode')==(col('rept1.clientCode')))\
                                  &(col('gld1.companyCode')==(col('rept1.companyCode')))),how='inner')\
                  .join(knw_LK_ISOLanguageKey.alias('lngkey')\
                                  ,(col('lngkey.targetSystemValue')==(col('rept1.languageCode'))),how='inner')\
                  .join(erp_SKAT.alias('skat')\
                       ,((col('skat.MANDT')==(col('gld1.clientCode')))\
                          &(col('skat.KTOPL')==(col('gld1.chartOfAccountID')))\
                          &(col('skat.SAKNR')==(col('gld1.accountNumber')))\
                          &(col('skat.SPRAS')==(col('lngkey.sourceSystemValue')))),how='inner')\
                 .select(col('gld1.clientCode')\
                        ,col('gld1.companyCode')\
                        ,col('gld1.accountNumber')\
                        ,col('gld1.chartOfAccountID')\
                        ,col('gld1.accountType')\
                        ,col('gld1.isOnlyAutomatedPostingAllowed')\
                        ,col('gld1.companyID')\
                        ,col('skat.TXT50').alias('accountName')\
                        ,col('rept1.languageCode').alias('languageCode'))


      fin_L1_TMP_GLAccountDetail = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                   .join(update_1.alias('upd1')\
                                   ,((col('gld1.clientCode').eqNullSafe(col('upd1.clientCode')))\
                                    &(col('gld1.companyCode').eqNullSafe(col('upd1.companyCode')))\
                                    &(col('gld1.accountNumber').eqNullSafe(col('upd1.accountNumber')))),how='left')\
                       .select(col('gld1.clientCode')\
                        ,col('gld1.companyCode')\
                        ,col('gld1.accountNumber')\
                        ,col('gld1.chartOfAccountID')\
                        ,col('gld1.accountType')\
                        ,col('gld1.isOnlyAutomatedPostingAllowed')\
                        ,col('gld1.companyID')\
                        ,col('upd1.accountName')\
                        ,col('upd1.languageCode'))
#update2
      update_2 = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                  .join(knw_clientDataSystemReportingLanguage.alias('rept1')\
                                 ,((col('gld1.clientCode')==(col('rept1.clientCode')))\
                                  &(col('gld1.companyCode')==(col('rept1.companyCode')))),how='inner')\
                  .join(knw_LK_ISOLanguageKey.alias('lngkey')\
                                    ,(col('lngkey.sourceSystemValue')==(col('rept1.languageCode'))),how='inner')\
                  .join(erp_SKAT.alias('skat')\
                       ,((col('skat.MANDT').eqNullSafe(col('gld1.clientCode')))\
                          &(col('skat.KTOPL').eqNullSafe(col('gld1.chartOfAccountID')))\
                          &(col('skat.SAKNR').eqNullSafe(col('gld1.accountNumber')))\
                           &(col('skat.SPRAS').eqNullSafe(col('lngkey.sourceSystemValue')))),how='left')\
                   .filter(col('gld1.accountName').isNull())\
                  .select(col('gld1.clientCode')\
                         ,col('gld1.companyCode')\
                         ,col('gld1.accountNumber')\
                         ,col('gld1.chartOfAccountID')\
                         ,col('gld1.accountType')\
                         ,col('gld1.isOnlyAutomatedPostingAllowed')\
                         ,col('gld1.companyID')\
                         ,when(col('skat.TXT50').isNull(),'#NA#').otherwise(col('skat.TXT50')).alias('accountName')\
                       ,when(col('gld1.languageCode').isNull(),'NA').otherwise(col('gld1.languageCode')).alias('languageCode'))
   
      fin_L1_TMP_GLAccountDetail = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                   .join(update_2.alias('upd2')\
                                   ,((col('gld1.clientCode').eqNullSafe(col('upd2.clientCode')))\
                                    &(col('gld1.companyCode').eqNullSafe(col('upd2.companyCode')))\
                                    &(col('gld1.accountNumber').eqNullSafe(col('upd2.accountNumber')))),how='left')\
                         .select(col('gld1.clientCode')\
                          ,col('gld1.companyCode')\
                          ,col('gld1.accountNumber')\
                          ,col('gld1.chartOfAccountID')\
                          ,col('gld1.accountType')\
                          ,col('gld1.isOnlyAutomatedPostingAllowed')\
                          ,col('gld1.companyID')\
                       ,when(col('gld1.accountName').isNull(),col('upd2.accountName')).otherwise(col('gld1.accountName')).alias('accountName')\
                      ,when(col('gld1.languageCode').isNull(),col('upd2.languageCode')).otherwise(col('gld1.languageCode')).alias('languageCode'))

#update3
      update_3 = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                  .join(knw_clientDataReportingLanguage.alias('rept1')\
                                 ,((col('gld1.clientCode')==(col('rept1.clientCode')))\
                                  &(col('gld1.companyCode')==(col('rept1.companyCode')))),how='inner')\
                  .join(knw_LK_ISOLanguageKey.alias('lngkey')\
                                  ,(col('lngkey.targetSystemValue')==(col('rept1.languageCode'))),how='inner')\
                  .join(erp_T004T.alias('T004T')\
                       ,((col('T004T.MANDT')==(col('gld1.clientCode')))\
                          &(col('T004T.KTOPL')==(col('gld1.chartOfAccountID')))\
                          &(col('T004T.SPRAS')==(col('lngkey.sourceSystemValue')))),how='inner')\
                 .select(col('gld1.clientCode')\
                        ,col('gld1.companyCode')\
                        ,col('gld1.accountNumber')\
                        ,col('gld1.chartOfAccountID')\
                        ,col('gld1.accountType')\
                        ,col('gld1.isOnlyAutomatedPostingAllowed')\
                        ,col('gld1.companyID')\
                        ,col('gld1.accountName')\
                        ,col('gld1.languageCode')\
                        ,col('T004T.KTPLT').alias('chartOfAccountText'))


      fin_L1_TMP_GLAccountDetail = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                   .join(update_3.alias('upd3')\
                                   ,((col('gld1.clientCode').eqNullSafe(col('upd3.clientCode')))\
                                    &(col('gld1.companyCode').eqNullSafe(col('upd3.companyCode')))\
                                    &(col('gld1.accountNumber').eqNullSafe(col('upd3.accountNumber')))),how='left')\
                       .select(col('gld1.clientCode')\
                        ,col('gld1.companyCode')\
                        ,col('gld1.accountNumber')\
                        ,col('gld1.chartOfAccountID')\
                        ,col('gld1.accountType')\
                        ,col('gld1.isOnlyAutomatedPostingAllowed')\
                        ,col('gld1.companyID')\
                        ,col('gld1.accountName')\
                        ,col('gld1.languageCode')\
                        ,col('upd3.chartOfAccountText'))
#update4					  
      update_4 = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                  .join(knw_clientDataSystemReportingLanguage.alias('rept1')\
                                 ,((col('gld1.clientCode')==(col('rept1.clientCode')))\
                                  &(col('gld1.companyCode')==(col('rept1.companyCode')))),how='inner')\
                  .join(knw_LK_ISOLanguageKey.alias('lngkey')\
                                    ,(col('lngkey.sourceSystemValue')==(col('rept1.languageCode'))),how='inner')\
                  .join(erp_T004T.alias('T004T')\
                       ,((col('T004T.MANDT').eqNullSafe(col('gld1.clientCode')))\
                          &(col('T004T.KTOPL').eqNullSafe(col('gld1.chartOfAccountID')))\
                           &(col('T004T.SPRAS').eqNullSafe(col('lngkey.sourceSystemValue')))),how='left')\
                   .filter(col('gld1.chartOfAccountText').isNull())\
                  .select(col('gld1.clientCode')\
                         ,col('gld1.companyCode')\
                         ,col('gld1.accountNumber')\
                         ,col('gld1.chartOfAccountID')\
                         ,col('gld1.accountType')\
                         ,col('gld1.isOnlyAutomatedPostingAllowed')\
                         ,col('gld1.companyID')\
                         ,col('gld1.accountName')\
                         ,col('gld1.languageCode')\
                         ,when(col('T004T.KTPLT').isNull(),'#NA#').otherwise(col('T004T.KTPLT')).alias('chartOfAccountText'))


      fin_L1_TMP_GLAccountDetail = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                   .join(update_4.alias('upd4')\
                                   ,((col('gld1.clientCode').eqNullSafe(col('upd4.clientCode')))\
                                    &(col('gld1.companyCode').eqNullSafe(col('upd4.companyCode')))\
                                    &(col('gld1.accountNumber').eqNullSafe(col('upd4.accountNumber')))),how='left')\
                         .select(col('gld1.clientCode')\
                          ,col('gld1.companyCode')\
                          ,col('gld1.accountNumber')\
                          ,col('gld1.chartOfAccountID')\
                          ,col('gld1.accountType')\
                          ,col('gld1.isOnlyAutomatedPostingAllowed')\
                          ,col('gld1.companyID')\
                          ,col('gld1.accountName')\
                          ,col('gld1.languageCode')\
                          ,when(col('gld1.chartOfAccountText').isNull(),col('upd4.chartOfAccountText'))\
                                 .otherwise(col('gld1.chartOfAccountText')).alias('chartOfAccountText'))
 #update 5

      reconcilationAccountType_case = when(col('SKB1.MITKZ')== 'A', lit(1))\
                              .when(col('SKB1.MITKZ')== 'D', lit(2))\
                              .when(col('SKB1.MITKZ')== 'K', lit(3))\
                              .when(col('SKB1.MITKZ')== 'V', lit(4))\
                              .otherwise(None)
                             

      fin_L1_TMP_GLAccountDetail = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                  .join(erp_SKB1.alias('SKB1')\
                                 ,((col('SKB1.SAKNR')==(col('gld1.accountNumber')))\
                                  &(col('gld1.companyCode')==(col('SKB1.BUKRS')))),how='leftouter')\
                     .select(col('gld1.clientCode')\
                        ,col('gld1.companyCode')\
                        ,col('gld1.accountNumber')\
                        ,col('gld1.chartOfAccountID')\
                        ,col('gld1.accountType')\
                        ,col('gld1.isOnlyAutomatedPostingAllowed')\
                        ,col('gld1.companyID')\
                        ,col('gld1.accountName')\
                        ,col('gld1.languageCode')\
                        ,col('gld1.chartOfAccountText')\
                        ,lit(reconcilationAccountType_case).alias('reconcilationAccountType'))

#final output table
      fin_L1_MD_GLAccount = fin_L1_TMP_GLAccountDetail.alias('gld1')\
                      .select(col('gld1.companyCode').alias('companyCode')\
                              ,col('gld1.accountNumber').alias('accountNumber')\
                              ,col('gld1.accountName').alias('accountName')\
                              ,lit('').alias('accountDescription')\
                              ,col('gld1.languageCode').alias('languageCode')\
                              ,col('gld1.chartOfAccountID').alias('chartOfAccountID')\
                              ,col('gld1.chartOfAccountText').alias('chartOfAccountText')\
                              ,col('gld1.accountType').alias('accountType')\
                              ,col('gld1.isOnlyAutomatedPostingAllowed').alias('isOnlyAutomatedPostingAllowed')\
                              ,col('reconcilationAccountType').alias('reconcilationAccountType')\
                              ,col('companyID').alias('companyID')\
                             )\
                     .distinct()
      
      fin_L1_MD_GLAccount =  objDataTransformation.gen_convertToCDMandCache \
         (fin_L1_MD_GLAccount,'fin','L1_MD_GLAccount',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "L1_MD_GLAccount populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


     
