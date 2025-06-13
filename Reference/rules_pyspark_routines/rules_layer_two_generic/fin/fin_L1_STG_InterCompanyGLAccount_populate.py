# Databricks notebook source
import sys
import traceback


def fin_L1_STG_InterCompanyGLAccount_populate():
	""" Populate SQL table fin_L1_STG_InterCompanyGLAccount"""
	try:
		logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
		objGenHelper = gen_genericHelper()
		objDataTransformation = gen_dataTransformation()
		
		global fin_L1_STG_InterCompanyGLAccount 

		fin_L1_STG_InterCompanyGLAccount = None
		#spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_STG_InterCompanyGLAccount")
		#spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_InterCompanyGLAccount")

		#FromAcMapping    ==>L1_STG_GLAccountMapping
		fin_L1_STG_GLAccountMapping_tmp=fin_L1_STG_GLAccountMapping.alias('Gla')\
            .select('Gla.accountNumber','Gla.isIntercompany','Gla.is3rdParty')\
            .filter((col('Gla.isIntercompany')==1) | (col('Gla.is3rdParty')==1))\
            .distinct()

		fin_L1_TMP_InterCompanyGLAccount_STG_GLAAcM=fin_L1_STG_GLAccountMapping_tmp.alias('Gla')\
            .crossJoin(knw_LK_CD_ReportingSetup.alias('rpt'))\
            .select(lit(1).alias('interCompanyAttribute')\
                              ,col('rpt.clientCode').alias('clientCode')\
                              ,col('rpt.companyCode').alias('companyCode')\
                              ,col('Gla.accountNumber').alias('accountNumber')\
                              ,(when(col('isIntercompany')==1 ,lit(1)).otherwise(lit(2))).alias('businessPartnerTypeAttributeFlag'))\
            .distinct()
        #GLA--->L1_MD_GLAccount
		fin_L1_TMP_GLADistinct=fin_L1_MD_GLAccount.alias('gla')\
                       .join(knw_LK_CD_ReportingSetup.alias('rpt'),\
                          (col('gla.companyCode')==col('rpt.companyCode')),how='inner')\
                       .select(col('rpt.clientCode').alias('clientCode')\
                              ,col('gla.companyCode').alias('companyCode')\
                              ,col('gla.accountNumber').alias('accountNumber')\
                              ,when((col('gla.companyID').isNull()),'')\
                                  .otherwise(col('gla.companyID')).alias('companyID_New')\
                              ,col('gla.reconcilationAccountType').alias('reconcilationAccountType')\
                              ,col("gla.isInterCoFlag").alias('isInterCoFlag')\
                              ,col("gla.companyID").alias('companyID')\
                              ,when((col('gla.chartOfAccountID').isNull()),'')\
                                  .otherwise(col('gla.chartOfAccountID')).alias('chartOfAccount_GLA')\
                               ,when((col('rpt.chartOfAccount').isNull()),'')\
                                  .otherwise(col('rpt.chartOfAccount')).alias('chartOfAccount_RPT')\
                              )\
                        .distinct()
		fin_L1_TMP_GLADistinct.persist()

		fin_L1_TMP_InterCompanyGLAccount_GLA=fin_L1_TMP_GLADistinct.alias('GLA')\
                      .select( lit(2).alias('interCompanyAttribute')\
                              ,col('GLA.clientCode').alias('clientCode')\
                              ,col('GLA.companyCode').alias('companyCode')\
                              ,col('GLA.accountNumber').alias('accountNumber')\
                              ,when(col('companyID_New')!='',lit(1))\
                                .otherwise(lit(2)).alias('businessPartnerTypeAttributeFlag')\
                             )\
                        .filter((col('companyID_New') !='') | (col("reconcilationAccountType").isin(2,3)))\
                        .distinct()
		if (fin_L1_TMP_InterCompanyGLAccount_GLA.rdd.isEmpty()):
		  fin_L1_TMP_InterCompanyGLAccount_GLA=fin_L1_TMP_GLADistinct.alias('GLA')\
                .select( lit(2).alias('interCompanyAttribute')\
                        ,col('GLA.clientCode').alias('clientCode')\
                        ,col('GLA.companyCode').alias('companyCode')\
                        ,col('GLA.accountNumber').alias('accountNumber')\
                        ,when(col('isInterCoFlag')==1,lit(1))\
                        .when((col('isInterCoFlag') == 0) | (col('isInterCoFlag')== 2),lit(2))\
                        .otherwise(None).alias('businessPartnerTypeAttributeFlag')\
                       )\
                  .distinct()
        #---JET 
		fin_L1_TMP_JETDistinct=fin_L1_TD_Journal.alias('Jet')\
                                .select('jet.companyCode'\
                                 ,'jet.accountNumber','jet.isInterCoFlag')\
                                .distinct()

		fin_L1_TMP_JETDistinctclientCode= fin_L1_TMP_JETDistinct.alias('Jet')\
                       .join(knw_LK_CD_ReportingSetup.alias('rpt'),\
                          (col('Jet.companyCode')==col('rpt.companyCode')),how='inner')\
                       .select(col('rpt.clientCode').alias('clientCode')\
                              ,col('Jet.companyCode').alias('companyCode')\
                               ,col('Jet.accountNumber').alias('accountNumber')\
                               ,col('Jet.isInterCoFlag').alias('isInterCoFlag')\
                              )
        
        #---JET -->1
        #--Inter company from Transaction - Scenario 1,isInterCoFlag values in the JET file with 0 and with 1, 
        #we want to treat this account-company code as 1, ie “I/C From Transaction
        #--isInterCoFlag values in the JET file with only 1 values, it should be “I/C From Transaction”

		fin_L1_TMP_InterCompanyGLAccount_JET=fin_L1_TMP_JETDistinctclientCode.alias('Jet')\
                       .select(lit(3).alias('interCompanyAttribute')\
                              ,col('Jet.clientCode').alias('clientCode')\
                              ,col('Jet.companyCode').alias('companyCode')\
                              ,col('Jet.accountNumber').alias('accountNumber')\
                              ,lit(1).alias('businessPartnerTypeAttributeFlag')\
                              )\
                         .filter((col('jet.isInterCoFlag')==1))\
                         .distinct()
        #---JET -->2
		fin_L1_TMP_GLAaccountExceptJet=fin_L1_TMP_GLADistinct.alias('gla')\
                              .join(fin_L1_TMP_InterCompanyGLAccount_JET.alias('jet'),\
                                     ((col('gla.clientCode')==col('jet.clientCode'))\
                                     & (col('gla.companyCode')==col('jet.companyCode'))\
                                     & (col('gla.accountNumber')==col('jet.accountNumber'))\
                                     ),how='ANTI' \
                                   )\
                               .select(col('gla.clientCode').alias('clientCode')\
                                      ,col('gla.companyCode').alias('companyCode')\
                                      ,col('gla.accountNumber').alias('accountNumber')\
                                      ,col('gla.reconcilationAccountType').alias('reconcilationAccountType')\
                                      ,col("gla.isInterCoFlag").alias('isInterCoFlag')\
                                      ,col("gla.companyID").alias('companyID')\
                                      )

		fin_L1_TMP_InterCompanyGLAccount_JET_2=fin_L1_TMP_GLAaccountExceptJet.alias('gla')\
                      .select( lit(3).alias('interCompanyAttribute')\
                              ,col('gla.clientCode').alias('clientCode')\
                              ,col('gla.companyCode').alias('companyCode')\
                              ,col('gla.accountNumber').alias('accountNumber')\
                              ,lit(2).alias('businessPartnerTypeAttributeFlag')\
                             )\
                        .filter((col('reconcilationAccountType').isin(2,3)) & (col('gla.companyID')=='') )\
                        .distinct()

		fin_L1_TMP_InterCompanyGLAccount_JET=fin_L1_TMP_InterCompanyGLAccount_JET.union(fin_L1_TMP_InterCompanyGLAccount_JET_2)
        #---JET -->3
        #fin_L1_TMP_GLADistinct.display()
		fin_L1_TMP_GLAaccountExceptJet3=fin_L1_TMP_GLADistinct.alias('gla')\
                         .join(fin_L1_TMP_InterCompanyGLAccount_JET.alias('jet'),\
                                ((col('gla.clientCode')==col('jet.clientCode'))\
                                & (col('gla.companyCode')==col('jet.companyCode'))\
                                & (col('gla.accountNumber')==col('jet.accountNumber'))\
                                ),how='ANTI' \
                              )\
                          .select(col('gla.clientCode').alias('clientCode')\
                                 ,col('gla.companyCode').alias('companyCode')\
                                 ,col('gla.accountNumber').alias('accountNumber')\
                                 ,col('gla.reconcilationAccountType').alias('reconcilationAccountType')\
                                 ,col("gla.isInterCoFlag").alias('isInterCoFlag')\
                                 ,col("gla.companyID").alias('companyID')\
                                 )\
                            .filter((col('gla.companyID').isNull()) & (col('reconcilationAccountType').isNull()))
		fin_L1_TMP_InterCompanyGLAccount_JET_3=fin_L1_TMP_GLAaccountExceptJet3.alias('gla')\
                          .join(fin_L1_TMP_JETDistinctclientCode.alias('jet'),\
                                 ((col('gla.companyCode')==col('jet.companyCode'))\
                                 & (col('gla.accountNumber')==col('jet.accountNumber'))\
                                 & (col('jet.isInterCoFlag')==0)\
                                 ),how='inner' \
                               )\
                           .select(lit(3).alias('interCompanyAttribute')\
                                  ,col('gla.clientCode').alias('clientCode')\
                                  ,col('gla.companyCode').alias('companyCode')\
                                  ,col('gla.accountNumber').alias('accountNumber')\
                                  ,lit(2).alias('businessPartnerTypeAttributeFlag')\
                                  )
		fin_L1_TMP_InterCompanyGLAccount_JET=fin_L1_TMP_InterCompanyGLAccount_JET.union(fin_L1_TMP_InterCompanyGLAccount_JET_3)
        #---L1_STG_InterCompanyGLAccount
		fin_L1_STG_InterCompanyGLAccount =fin_L1_TMP_GLADistinct.alias('gla')\
              .join(fin_L1_TMP_InterCompanyGLAccount_STG_GLAAcM.alias('icFromAcMapping'),\
                                                  ((col('gla.clientCode').eqNullSafe(col('icFromAcMapping.clientCode')))\
                                                    & (col('gla.companyCode').eqNullSafe(col('icFromAcMapping.companyCode')))\
                                                    & (col('gla.accountNumber').eqNullSafe(col('icFromAcMapping.accountNumber')))\
                                                  ),how='left' \
                                                )\
              .join(fin_L1_TMP_InterCompanyGLAccount_GLA.alias('icFromMaster'),\
                                                  ((col('gla.clientCode').eqNullSafe(col('icFromMaster.clientCode')))\
                                                    & (col('gla.companyCode').eqNullSafe(col('icFromMaster.companyCode')))\
                                                    & (col('gla.accountNumber').eqNullSafe(col('icFromMaster.accountNumber')))\
                                                  ),how='left' \
                                                )\
              .join(fin_L1_TMP_InterCompanyGLAccount_JET.alias('icFromTrans'),\
                                                  ((col('gla.clientCode').eqNullSafe(col('icFromTrans.clientCode')))\
                                                    & (col('gla.companyCode').eqNullSafe(col('icFromTrans.companyCode')))\
                                                    & (col('gla.accountNumber').eqNullSafe(col('icFromTrans.accountNumber')))\
                                                  ),how='left' \
                                                )\
              .filter(col('gla.chartOfAccount_GLA') == col('gla.chartOfAccount_RPT'))\
              .select(col('gla.clientCode').alias('clientCode')\
                     ,col('gla.companyCode').alias('companyCode')\
                     ,col('gla.accountNumber').alias('accountNumber')\
                     ,col('icFromAcMapping.businessPartnerTypeAttributeFlag').alias('isInterCoFromAccountMapping')\
                     ,col('icFromMaster.businessPartnerTypeAttributeFlag').alias('isInterCoFromMasterData')\
                     ,col('icFromTrans.businessPartnerTypeAttributeFlag').alias('isInterCoFromTransactionData')\
                     ).withColumn("isInterCoFlag", when((col('isInterCoFromAccountMapping')==1)\
                                                  | (col('isInterCoFromMasterData')==1) \
                                                  | (col('isInterCoFromTransactionData')==1) \
                                                  ,lit(1))\
                                             .when((col('isInterCoFromAccountMapping')==2)\
                                                  | (col('isInterCoFromMasterData')==2) \
                                                  | (col('isInterCoFromTransactionData')==2) \
                                                  ,lit(2))\
                                             .when((col('isInterCoFromAccountMapping')==3)\
                                                  & (col('isInterCoFromMasterData')==3) \
                                                  & (col('isInterCoFromTransactionData')==3) \
                                                  ,lit(3))\
                                              .otherwise(lit(3))\
                            )
		
		fin_L1_STG_InterCompanyGLAccount  = objDataTransformation.gen_convertToCDMandCache \
			(fin_L1_STG_InterCompanyGLAccount,'fin','L1_STG_InterCompanyGLAccount',False)
		
		executionStatus = "fin_L1_STG_InterCompanyGLAccount populated sucessfully"
		executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
		return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

		
	except Exception as e:
		executionStatus = objGenHelper.gen_exceptionDetails_log()       
		executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
		return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

	finally:
		if (fin_L1_TMP_GLADistinct is not None) :
		  fin_L1_TMP_GLADistinct.unpersist()

	

	

# COMMAND ----------


