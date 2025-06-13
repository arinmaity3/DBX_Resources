# Databricks notebook source
from pyspark.sql.functions import row_number,expr,trim,col,lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback


def fin_L2_DIM_BifurcationDataType_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L2_DIM_BifurcationDataType
        
        w= Window.orderBy(lit('glBifurcationDataTypeSurrogateKey'))

        dfBifurcationDataType = spark.sql("select distinct CAST(1 AS smallInt) AS versionID,\
                                    lkbd5.sourceSystemValue AS glBifurcationDataType\
                                    ,lkbd5.targetLanguageCode AS languageCode\
                                    from knw_LK_CD_ReportingSetup krsp\
                                    left join knw_LK_GD_BusinessDatatypeValueMapping lkbd5 \
                                    on (lkbd5.businessDatatype = 'Bifurcation DataType'\
				                    and lkbd5.targetERPSystemID = 10\
				                    and lkbd5.targetLanguageCode = KPMGDataReportingLanguage)\
                             ").withColumn("glBifurcationDataTypeSurrogateKey",F.row_number().over(w))

        fin_L2_DIM_BifurcationDataType  = objDataTransformation.gen_convertToCDMandCache\
                                          (dfBifurcationDataType,'fin',\
                                          'L2_DIM_BifurcationDataType',False)


        dwh_vw_DIM_BifurcationDataType = objDataTransformation.gen_convertToCDMStructure_generate\
                                        (fin_L2_DIM_BifurcationDataType,'dwh',\
                                        'vw_DIM_BifurcationDataType',isIncludeAnalysisID = True)[0]

        objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_BifurcationDataType,\
                                        gl_CDMLayer2Path + "fin_L2_DIM_BifurcationDataType.parquet" )
        
        executionStatus = "fin_L2_DIM_BifurcationDataType populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

