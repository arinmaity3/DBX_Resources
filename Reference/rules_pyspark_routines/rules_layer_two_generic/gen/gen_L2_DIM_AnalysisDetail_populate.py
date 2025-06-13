# Databricks notebook source
from pyspark.sql.functions import row_number,expr,trim,max
from datetime import datetime,timedelta
from pyspark.sql.window import Window
import sys
import traceback

def gen_L2_DIM_AnalysisDetail_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

        global gl_processStartTime

        L2_DIM_AnalysisDetail_Schema = StructType([                                    
                                  StructField("analysisServer",StringType(),True),
                                  StructField("extractionDate",StringType(),True),
                                  StructField("extractionDateFormatted",StringType(),True),
                                  StructField("lastProcessedDateTime",StringType(),True),
                                  StructField("lastProcessedDateTimeFormatted",StringType(),True),
                                  StructField("clientName",StringType(),True),
                                  StructField("periodStartDate",StringType(),True),
                                  StructField("periodStartDateFormatted",StringType(),True),
                                  StructField("periodEndDate",StringType(),True),
                                  StructField("periodEndDateFormatted",StringType(),True),
                                  StructField("periodEndDateAdditinalDay",StringType(),True),
                                  StructField("largeAmount",StringType(),True),
                                  StructField("fiscalYear",StringType(),True),
                                  StructField("PeriodStart",StringType(),True),
                                  StructField("PeriodEnd",StringType(),True)
                                  ])

        lstOfAnalysisDetail = list()
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSIS_SERVER'))
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'EXTRACTION_DATE'))
        lstOfAnalysisDetail.append(parse(objGenHelper.gen_lk_cd_parameter_get(\
                                           'GLOBAL', 'EXTRACTION_DATE')).date().strftime("%d-%b-%Y"))
        lstOfAnalysisDetail.append(gl_processStartTime.strftime("%m/%d/%Y, %H:%M"))
        lstOfAnalysisDetail.append(gl_processStartTime.strftime("%m %b %Y, %H:%M:%S"))
        if(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'CLIENT_NAME') is None):
            lstOfAnalysisDetail.append(knw_LK_CD_ReportingSetup.agg(max('clientName')).collect()[0][0])
        else:
            lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'CLIENT_NAME'))
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE'))
        lstOfAnalysisDetail.append(parse(objGenHelper.gen_lk_cd_parameter_get(\
                                                'GLOBAL', 'START_DATE')).date().strftime("%d-%b-%Y"))
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE'))
        lstOfAnalysisDetail.append(parse(objGenHelper.gen_lk_cd_parameter_get(\
                                                 'GLOBAL', 'END_DATE')).date().strftime("%d-%b-%Y"))
        periodEndDateAdditinalDay = parse(objGenHelper.gen_lk_cd_parameter_get(\
                                                    'GLOBAL', 'END_DATE')).date()+ timedelta(days=1)
        lstOfAnalysisDetail.append(periodEndDateAdditinalDay.strftime("%d-%m-%Y"))
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'LARGE AMOUNT'))
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR'))
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START'))
        lstOfAnalysisDetail.append(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END'))


        dwh_vw_AnalysisDetail = spark.createDataFrame(data = [lstOfAnalysisDetail], schema = L2_DIM_AnalysisDetail_Schema)
        dwh_vw_AnalysisDetail = objDataTransformation.gen_convertToCDMStructure_generate\
                                  (dwh_vw_AnalysisDetail, 'dwh','vw_AnalysisDetail',True)[0]
        objGenHelper.gen_writeToFile_perfom(dwh_vw_AnalysisDetail,gl_CDMLayer2Path + "gen_L2_DIM_AnalysisDetail.parquet" )
        
        executionStatus = "gen_L2_DIM_AnalysisDetail populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


