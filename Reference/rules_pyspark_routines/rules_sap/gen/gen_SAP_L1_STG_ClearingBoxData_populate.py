# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when
def gen_SAP_L1_STG_ClearingBoxData_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_STG_ClearingBoxData
    
    erpSAPSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')
    pGENSystemID =  objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    
    pTargetLanguageCode	= knw_LK_GD_BusinessDatatypeValueMapping.alias('bdm')\
    	  .crossJoin(knw_KPMGDataReportingLanguage.alias('rlg'))\
    	  .groupby("bdm.targetLanguageCode","rlg.languageCode")\
    	  .agg(max(when((col('bdm.targetLanguageCode')==(col('rlg.languageCode'))),col('bdm.targetLanguageCode'))\
    		               .otherwise(lit(''))).alias('c1')\
    		,max(when((col('bdm.targetLanguageCode')==lit('EN')),col('bdm.targetLanguageCode'))\
    			        .otherwise(lit(''))).alias('c2')\
    		,max(when((col('bdm.targetLanguageCode')==lit('DE')),col('bdm.targetLanguageCode'))\
    			        .otherwise(lit(''))).alias('c3')\
    		,max(col('bdm.targetLanguageCode')).alias('c4'))\
          .select(coalesce(col('c1'),col('c2'),col('c3'),col('c4')).alias('pTargetLanguageCode')).collect()[0][0]
    
    pTmpTargetLanguageCode = erp_T074T.alias('T074T_erp')\
    	 .agg(max(when((col('T074T_erp.SPRAS')==(lit(pTargetLanguageCode))),col('T074T_erp.SPRAS'))\
    	                .otherwise(lit(''))).alias('c1')\
    	     ,max(when((col('T074T_erp.SPRAS')==lit('EN')),col('T074T_erp.SPRAS'))\
    			        .otherwise(lit(''))).alias('c2')\
    		 ,max(when((col('T074T_erp.SPRAS')==lit('DE')),col('T074T_erp.SPRAS'))\
    			        .otherwise(lit(''))).alias('c3')\
    		 ,max(col('T074T_erp.SPRAS')).alias('c4'))\
          .select(coalesce(col('c1'),col('c2'),col('c3'),col('c4')).alias('pTmpTargetLanguageCode'))\
          .collect()[0][0]
    
    v_trim = concat(trim(col('T074T_erp.KOART')),lit('/'),trim(col('T074T_erp.SHBKZ')))
    
    L1_STG_ClearingBoxData_FinancialAccountingPoll = erp_T074T.alias('T074T_erp')\
        .filter((col('T074T_erp.SPRAS')==lit(pTmpTargetLanguageCode))&\
               ((when(col('T074T_erp.KOART').isNull(),lit('')).otherwise(col('T074T_erp.KOART')))!=''))\
        .select(lit('GL Special Indicator').alias('businessDatatype')\
               ,lit('GL Special Indicator').alias('businessDatatypeDescription')\
               ,lit(erpSAPSystemID).alias('sourceERPSystemID')\
               ,lit(v_trim).alias('sourceSystemValue')\
               ,col('T074T_erp.KTEXT').alias('sourceSystemValueDescription')\
               ,lit(pGENSystemID).alias('targetERPSystemID')\
               ,col('T074T_erp.SPRAS').alias('targetLanguageCode')\
               ,lit('FinancialAccountingPoll').alias('sourceType')\
               )
    
    pTmpTargetLanguageCode = erp_DD07T.alias('DD07T_erp')\
         .groupby(col('DD07T_erp.DOMNAME'))\
    	 .agg(max(when((col('DD07T_erp.DDLANGUAGE')==(lit(pTargetLanguageCode))),col('DD07T_erp.DDLANGUAGE'))\
    	           .otherwise(lit(''))).alias('c1')\
    	     ,max(when((col('DD07T_erp.DDLANGUAGE')==lit('EN')),col('DD07T_erp.DDLANGUAGE'))\
    		       .otherwise(lit(''))).alias('c2')\
    		,max(when((col('DD07T_erp.DDLANGUAGE')==lit('DE')),col('DD07T_erp.DDLANGUAGE'))\
    		       .otherwise(lit(''))).alias('c3')\
    		,max(col('DD07T_erp.DDLANGUAGE')).alias('c4'))\
         .select(coalesce(col('c1'),col('c2'),col('c3'),col('c4')).alias('pTmpTargetLanguageCode'))\
         .filter(col('DD07T_erp.DOMNAME')==lit('VBTYP')).collect()[0][0]
    
    L1_STG_ClearingBoxData_SDDocumentType = erp_DD07T.alias('DD07T_erp')\
        .filter((col('DD07T_erp.DDLANGUAGE')==lit(pTmpTargetLanguageCode))&\
               (col('DD07T_erp.DOMNAME')==lit('VBTYP')))\
        .select(lit('SD Document Type').alias('businessDatatype')\
               ,lit('SD Document Type').alias('businessDatatypeDescription')\
               ,lit(erpSAPSystemID).alias('sourceERPSystemID')\
               ,col('DD07T_erp.DOMVALUE_L').alias('sourceSystemValue')\
               ,col('DD07T_erp.DDTEXT').alias('sourceSystemValueDescription')\
               ,lit(10).alias('targetERPSystemID')\
               ,col('DD07T_erp.DDLANGUAGE').alias('targetLanguageCode')\
               ,lit('DomainFixedValues_SDDocumentType').alias('sourceType'))
    L1_STG_ClearingBoxData_SDDocumentType = L1_STG_ClearingBoxData_FinancialAccountingPoll.union(L1_STG_ClearingBoxData_SDDocumentType)
    
    pTmpTargetLanguageCode = erp_DD07T.alias('DD07T_erp')\
         .groupby(col('DD07T_erp.DOMNAME').alias('DOMNAME'))\
    	 .agg(max(when((col('DD07T_erp.DDLANGUAGE')==(lit(pTargetLanguageCode))),col('DD07T_erp.DDLANGUAGE'))\
    	              .otherwise(lit(''))).alias('c1')\
    		  ,max(when((col('DD07T_erp.DDLANGUAGE')==lit('EN')),col('DD07T_erp.DDLANGUAGE'))\
    		        .otherwise(lit(''))).alias('c2')\
    	     ,max(when((col('DD07T_erp.DDLANGUAGE')==lit('DE')),col('DD07T_erp.DDLANGUAGE'))\
    		        .otherwise(lit(''))).alias('c3')\
    		 ,max(col('DD07T_erp.DDLANGUAGE')).alias('c4'))\
         .select(coalesce(col('c1'),col('c2'),col('c3'),col('c4')).alias('pTmpTargetLanguageCode'))\
         .filter(col('DD07T_erp.DOMNAME')==lit('KOART')).collect()[0][0]
    
    L1_STG_ClearingBoxData_GLAccountType = erp_DD07T.alias('DD07T_erp')\
        .filter(((col('DD07T_erp.DDLANGUAGE')==lit(pTmpTargetLanguageCode))&\
               (col('DD07T_erp.DOMNAME')==lit('KOART'))))\
        .select(lit('GL Account Type').alias('businessDatatype')\
               ,lit('GL Account Type').alias('businessDatatypeDescription')\
               ,lit(erpSAPSystemID).alias('sourceERPSystemID')\
               ,col('DD07T_erp.DOMVALUE_L').alias('sourceSystemValue')\
               ,col('DD07T_erp.DDTEXT').alias('sourceSystemValueDescription')\
               ,lit(pGENSystemID).alias('targetERPSystemID')\
               ,col('DD07T_erp.DDLANGUAGE').alias('targetLanguageCode')\
               ,lit('DomainFixedValues_GLAccountType').alias('sourceType'))
    gen_L1_STG_ClearingBoxData = L1_STG_ClearingBoxData_GLAccountType.unionAll(L1_STG_ClearingBoxData_SDDocumentType)
    

    gen_L1_STG_ClearingBoxData =  objDataTransformation.gen_convertToCDMandCache \
           (gen_L1_STG_ClearingBoxData,'gen','L1_STG_ClearingBoxData',targetPath=gl_CDMLayer1Path)

    executionStatus = "L1_STG_ClearingBoxData populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
