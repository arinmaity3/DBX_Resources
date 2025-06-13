# Databricks notebook source
import sys
import traceback
def knw_clientReportingLanguage_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None)

        global knw_clientDataReportingLanguage
        global knw_clientDataSystemReportingLanguage
        global knw_KPMGDataReportingLanguage
        global knw_LK_ISOLanguageKey
        global knw_LK_CD_ReportingSetup
    
    
        if('knw_LK_CD_ReportingSetup' not in globals()):
            knw_LK_CD_ReportingSetup = objGenHelper.gen_readFromFile_perform\
                                   (gl_commonParameterPath + "knw_LK_CD_ReportingSetup.delta")
    
        #clientDataReportingLanguage
        df_clientDataReportingLanguage = knw_LK_CD_ReportingSetup.\
                                        select(col('clientCode'),col('companyCode')\
                                        ,col('clientDataReportingLanguage').alias('languageCode'))\
                                        .distinct()       
        knw_clientDataReportingLanguage =  objDataTransformation.gen_convertToCDMandCache \
        (df_clientDataReportingLanguage,'knw','clientDataReportingLanguage',targetPath=gl_layer0Temp_knw)

        #clientDataSystemReportingLanguage
        df_clientDataSystemReportingLanguage = knw_LK_CD_ReportingSetup.\
                                               select(col('clientCode'),col('companyCode')\
                                               ,col('clientDataSystemReportingLanguage').alias('languageCode'))\
                                               .distinct()    
        knw_clientDataSystemReportingLanguage =  objDataTransformation.gen_convertToCDMandCache \
        (df_clientDataSystemReportingLanguage,'knw','clientDataSystemReportingLanguage',targetPath=gl_layer0Temp_knw)

    
        #KPMGDataReportingLanguage
        df_KPMGDataReportingLanguage = knw_LK_CD_ReportingSetup.\
                                       select(col('clientCode'),col('companyCode')\
                                       ,col('KPMGDataReportingLanguage').alias('languageCode'))\
                                       .distinct()    
        knw_KPMGDataReportingLanguage =  objDataTransformation.gen_convertToCDMandCache \
        (df_KPMGDataReportingLanguage,'knw','KPMGDataReportingLanguage',targetPath=gl_layer0Temp_knw)

        #knw_LK_ISOLanguageKey
        erpSystemIDGeneric = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC'))
    
        df_LK_ISOLanguageKey = gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].\
                               select('sourceSystemValue','targetSystemValue').\
                               filter((col('businessDatatype')== lit('Language Code')) \
                               & (col('sourceERPSystemID')== erpSystemIDGeneric))   

        knw_LK_ISOLanguageKey =  objDataTransformation.gen_convertToCDMandCache \
        (df_LK_ISOLanguageKey,'knw','LK_ISOLanguageKey',targetPath=gl_layer0Temp_knw)
        
        executionStatus = "Client Reporting Language populated successfully."
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
