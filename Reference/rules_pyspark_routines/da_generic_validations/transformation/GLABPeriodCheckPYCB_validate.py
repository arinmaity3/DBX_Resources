# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import *
from pyspark.sql.functions import col, asc, lit, when,regexp_extract,asc,row_number
from pyspark.sql import Row
from functools import reduce
from pyspark.sql import DataFrame
import uuid

def fin_GLABPeriodCheckPYCB_validate(fileType):
    try:
        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.PY_CB)

        dfResult_Detail = None
        
        L1_TMP_GLABPeriodAccountCheckPYCB = spark.sql(" SELECT bal.companyCode "
                                                    ", bal.accountNumber "
                                                    ",bal.accountType"
                                                    ", glac1.accountName as accountName from fin_L1_STG_PYCYBalance bal LEFT JOIN "
                                                    " fin_L1_MD_GLAccount glac1 on if(glac1.accountNumber IS NULL,'#NA#',glac1.accountNumber) = bal.accountNumber "
                                                    " AND if(glac1.companyCode IS NULL,'#NA#',glac1.companyCode) = if(bal.companyCode IS NULL,'#NA#',bal.companyCode)"
                                                    " WHERE ISNULL(bal.PYCB) = 1 AND bal.isTransactionAccount = 1 ").limit(gl_maximumNumberOfValidationDetails)
        
        L1_TMP_GLABPeriodAccountCheckPYCB.persist()
        dfresult = L1_TMP_GLABPeriodAccountCheckPYCB.count()
        
        if dfresult >0:
            executionStatus = 'Prior year closing balance verification check failed'
            executionStatusID = LOG_EXECUTION_STATUS.WARNING
            
            dfResult_Detail= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.PY_CB.value,
                                      orderBycolumnName1='accountNumber',
                                      pivotColumn1='accountNumber',
                                      pivotColumn2='accountName',
                                      pivotColumn3='companyCode',
                                      pivotColumn4='accountType',
                                      dfToPivot=L1_TMP_GLABPeriodAccountCheckPYCB)
        else:
            executionStatus = 'Prior year closing balance verification check succeeded'
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        
    except Exception as err:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED

    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail) 
        if L1_TMP_GLABPeriodAccountCheckPYCB is not None:
           L1_TMP_GLABPeriodAccountCheckPYCB.unpersist()
        return [executionStatusID,executionStatus,dfResult_Detail]

