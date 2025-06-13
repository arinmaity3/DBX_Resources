# Databricks notebook source
from functools import reduce
from pyspark.sql import DataFrame
import sys
import traceback
import uuid

def fin_JEMinimumNumberOfPostingPerPeriod_validate(fileType):
    try:
        objGenHelper = gen_genericHelper()   
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.DATA_ALIGN_IN_ANALYSIS_PERIOD)

        dfResult_Detail = None
        
        startDate = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date())
        endDate = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date())
        fiscalYear = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR'))
        
        fin_L1_TMP_JEperiodwiseTransactionsCount = spark.sql("select  ROW_NUMBER() OVER (ORDER BY documentNumber,companyCode) AS groupSlno,'"\
                                                              +fileType+"'   validationObject,'"\
                                                              +fileType+"'   fileType "\
                                                              ",fiscalYear\
                                                              ,financialPeriod\
                                                              ,companyCode\
                                                              ,documentNumber\
                                                              ,lineItem\
                                                              ,postingDate\
                                                               from fin_L1_TD_Journal where   (\
                                                               CAST(postingDate AS DATE)  < '"+startDate+"' OR CAST(postingDate AS DATE)> '"+endDate+"'\
                                                               OR fiscalYear <> '"+fiscalYear+"' ) LIMIT "+str(gl_maximumNumberOfValidationDetails))
        
        dfResult_Detail= objGenHelper.gen_dynamicPivot(
                                      fileType =fileType,
                                      validationID =VALIDATION_ID.DATA_ALIGN_IN_ANALYSIS_PERIOD.value,
                                      pivotColumn1='companyCode',
                                      pivotColumn2='documentNumber',
                                      pivotColumn3='lineItem',
                                      pivotColumn4='postingDate',
                                      dfToPivot=fin_L1_TMP_JEperiodwiseTransactionsCount
                                      )

        dfresult = dfResult_Detail.count()

        if dfresult >0:
            executionStatus = "JE minimum number of posting per period validation failed"
            executionStatusID = LOG_EXECUTION_STATUS.WARNING
        else:
            executionStatus = "JE minimum number of posting per period validation succeeded"
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS                        
          
    except Exception as err:
       executionStatus =  objGenHelper.gen_exceptionDetails_log()
       executionStatusID = LOG_EXECUTION_STATUS.FAILED
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail)
        return [executionStatusID,executionStatus,dfResult_Detail]

      
