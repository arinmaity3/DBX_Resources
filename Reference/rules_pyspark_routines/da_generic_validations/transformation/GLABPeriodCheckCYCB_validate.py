# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import *
from pyspark.sql.functions import col, asc, lit, when,regexp_extract,asc,row_number
from pyspark.sql import Row
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
import sys
import traceback
import uuid


def fin_GLABPeriodCheckCYCB_validate(fileType):
    try:
        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.CY_CB)

        dfResult_Detail = None
        
        dfFin_L1_TMP_GLBalanceDC1 = spark.sql("select distinct bal.companyCode"
                                        ",bal.accountNumber"
                                        ",bal.CYCB"
                                        ",bal.accountType"
                                        ",glac1.accountName"
                                        " from fin_L1_STG_PYCYBalance bal"
                                        " left join fin_L1_MD_GLAccount glac1 "
                                        " on if(glac1.accountNumber IS NULL,'#NA#',glac1.accountNumber)   = bal.accountNumber "
                                        " AND if( glac1.companyCode IS NULL,'#NA#',glac1.companyCode)	 = if(bal.companyCode IS NULL,'#NA#',bal.companyCode) "
                                        " WHERE ISNULL(bal.CYCB) = 1 AND bal.isTransactionAccount = 1 ").limit(gl_maximumNumberOfValidationDetails)
        
        dfFin_L1_TMP_GLBalanceDC1.persist()
        dfresult = dfFin_L1_TMP_GLBalanceDC1.count()
        
        if dfresult >0:
            executionStatus = "Current year closing balance check (CYCB) failed"
            executionStatusID = LOG_EXECUTION_STATUS.WARNING
            
            dfResult_Detail = objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.CY_CB.value,
                                      orderBycolumnName1='accountNumber',
                                      pivotColumn1='accountNumber',
                                      pivotColumn2='accountName',
                                      pivotColumn3='companyCode',
                                      pivotColumn4='accountType',
                                      dfToPivot=dfFin_L1_TMP_GLBalanceDC1)
        else:
            executionStatus = "Current year closing balance check (CYCB) succeeded"
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
            
    except Exception as err:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED 
        
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail)
        if dfFin_L1_TMP_GLBalanceDC1 is not None:
            dfFin_L1_TMP_GLBalanceDC1.unpersist()
        return [executionStatusID,executionStatus,dfResult_Detail]
      

