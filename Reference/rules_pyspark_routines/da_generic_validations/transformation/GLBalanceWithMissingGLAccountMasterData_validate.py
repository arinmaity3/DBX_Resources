# Databricks notebook source
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType
from datetime import datetime
import uuid

def fin_GLBalanceWithMissingGLAccountMasterData_validate(fileType):
    try:
        fileName = "L1_TD_GLBalance.csv"
        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.ACCOUNT_NO_MATCH_GLA_GLAB)

        dfResult_Detail = None

        df1 = spark.sql("SELECT DISTINCT glab.companyCode, glab.accountNumber FROM fin_L1_TD_GLBalance AS glab "\
            "LEFT JOIN fin_L1_MD_GLAccount AS gla ON glab.companyCode = gla.companyCode AND glab.accountNumber = gla.accountNumber "\
            "WHERE gla.accountNumber IS NULL ")
        w = Window().orderBy(lit('groupSlno'))
        df1 = df1.withColumn("groupSlno", row_number().over(w))
        df1.limit(gl_maximumNumberOfValidationDetails)
        errorCount = df1.count()
        
        if errorCount == 0:
            executionStatus = "Account number in GLA and GLAB are matched."
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        else:
            executionStatus = "Account number in GLA and GLAB files are not matching."
            executionStatusID = LOG_EXECUTION_STATUS.WARNING
            
            dfResult_Detail= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.ACCOUNT_NO_MATCH_GLA_GLAB.value,
                                      pivotColumn1='CompanyCode',
                                      pivotColumn2='AccountNumber',
                                      dfToPivot=df1)
            remarks = "Account number in GLA and GLAB files does not match"
            dfResult_Detail = dfResult_Detail.withColumn("remarks", lit(remarks))
            
    except Exception as e:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail)
        return [executionStatusID,executionStatus,dfResult_Detail]               
            



