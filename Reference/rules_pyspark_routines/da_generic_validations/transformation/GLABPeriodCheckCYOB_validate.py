# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import *
from pyspark.sql.functions import col, asc, lit, when,regexp_extract,asc,row_number
from pyspark.sql import Row
from functools import reduce
from pyspark.sql import DataFrame
import sys
import traceback
import uuid

def fin_GLABPeriodCheckCYOB_validate(fileType):
    try:
        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.CY_OB)

        dfResult_Detail = None
        
        L1_TMP_GLABperiodAccountCheckCYOB = spark.sql("select distinct bal.companyCode"
                                        ",bal.accountNumber"
                                        ",bal.CYOB"
                                        ",bal.accountType"
                                        ",glac1.accountName"
                                        " from fin_L1_STG_PYCYBalance bal"
                                        " left join fin_L1_MD_GLAccount glac1 "
                                        " on if(glac1.accountNumber IS NULL,'#NA#',glac1.accountNumber)   = bal.accountNumber "
                                        " AND if( glac1.companyCode IS NULL,'#NA#',glac1.companyCode)	 = if(bal.companyCode IS NULL,'#NA#',bal.companyCode) "
                                        " WHERE ISNULL(bal.CYOB) = 1 AND bal.isTransactionAccount = 1 ").limit(gl_maximumNumberOfValidationDetails)

        L1_TMP_GLABperiodAccountCheckCYOB.persist()
        dfresult = L1_TMP_GLABperiodAccountCheckCYOB.count()
        
        if dfresult >0:
            executionStatus = 'Current year opening balance verification check failed'
            executionStatusID = LOG_EXECUTION_STATUS.WARNING
            dfResult_Detail= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.CY_OB.value,
                                      orderBycolumnName1='accountNumber',
                                      pivotColumn1='accountNumber',
                                      pivotColumn2='accountName',
                                      pivotColumn3='companyCode',
                                      pivotColumn4='accountType',
                                      dfToPivot=L1_TMP_GLABperiodAccountCheckCYOB)
        
        else:
            executionStatus = 'Current year opening balance verification check sucessfully completed.'
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
            
    except Exception as e:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED 

    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail)
        if L1_TMP_GLABperiodAccountCheckCYOB is not None:
            L1_TMP_GLABperiodAccountCheckCYOB.unpersist()
        return [executionStatusID,executionStatus,dfResult_Detail]
                       
  




