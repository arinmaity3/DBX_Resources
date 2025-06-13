# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import expr,lit,col,when

def gen_SAP_L1_MD_User_populate(): 
    try:

      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
      global gen_L1_MD_User
      
      erpSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','ERP_SYSTEM_ID')
      knw_KPMGReportingLanguage = knw_LK_CD_ReportingSetup.select(col('clientCode')\
                               ,col('KPMGDataReportingLanguage')).distinct()
      timeZone_nullif = expr("nullif(USR02.TZONE,'')")
      
      gen_L1_MD_User = erp_USR02.alias('USR02')\
               .join(knw_KPMGReportingLanguage.alias('krlg')\
				       ,(col('USR02.MANDT')== col('krlg.clientCode')),how="inner")\
			   .join(erp_USER_ADDR.alias('ADDR')\
				       ,((col('USR02.MANDT').eqNullSafe(col('ADDR.MANDT')))\
					   &(col('USR02.BNAME').eqNullSafe(col('ADDR.BNAME')))),how="left")\
			   .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd')\
				       ,((col('lkbd.targetLanguageCode').eqNullSafe(col('krlg.KPMGDataReportingLanguage')))\
					   &(col('lkbd.sourceSystemValue').eqNullSafe(col('USR02.USTYP')))\
					   &(col('lkbd.businessDatatype')== lit('User Type'))\
					   &(col('lkbd.sourceERPSystemID')== lit(erpSystemID))),how="left")\
			   .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbd0')\
				       ,((col('lkbd0.targetLanguageCode').eqNullSafe(col('krlg.KPMGDataReportingLanguage')))\
					   &(col('lkbd0.sourceSystemValue').eqNullSafe(col('USR02.USTYP')))\
					   &(col('lkbd0.businessDatatype')== lit('User Type Determination'))\
					   &(col('lkbd0.sourceERPSystemID')== lit(erpSystemID))),how="left")\
			   .select(when(col('USR02.BNAME').isNull(),'#NA#').otherwise(col('USR02.BNAME'))\
				                .alias('userName')\
                         ,when(col('ADDR.DEPARTMENT').isNull(),'#NA#').otherwise(col('ADDR.DEPARTMENT'))\
						        .alias('userDepartment')\
                         ,when(col('ADDR.NAME_TEXTC').isNull(),'#NA#').otherwise(col('ADDR.NAME_TEXTC'))\
						        .alias('employeeName')\
                         ,col('USR02.CLASS').alias('userGroup')\
                         ,when(col('lkbd.targetSystemValue').isNull(),'').otherwise(col('lkbd.targetSystemValue'))\
						        .alias('userType')\
                         ,when(lit(timeZone_nullif).isNull(),'#NA#').otherwise(lit(timeZone_nullif))\
						        .alias('timeZone')\
                         ,col('USR02.ANAME').alias('createdBy')\
                         ,col('USR02.ERDAT').alias('createdOn')\
                         ,when(col('USR02.UFLAG')=='0',lit(0)).otherwise(lit(1))\
						        .alias('isUserLocked')\
                         ,when(col('lkbd0.targetSystemValue').isNull(),'').otherwise(col('lkbd0.targetSystemValue'))\
						        .alias('userTypeDetermination')\
                         ,when(col('lkbd0.targetSystemValueDescription').isNull(),'#NA#').otherwise(col('lkbd0.targetSystemValueDescription'))\
                                  .alias('userTypeDeterminationDescription')
                         )

      gen_L1_MD_User =  objDataTransformation.gen_convertToCDMandCache \
         (gen_L1_MD_User,'gen','L1_MD_User',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "L1_MD_User populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

       
        
