# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from functools import reduce
import numpy as na
import sys
import traceback
import uuid

def _requiredField_validate(tableName,fileType,lstOfScopedAnalytics):
    try:
        objGenHelper = gen_genericHelper()   
        fileName          = tableName 
        
        dfValidationResultsDetails = None
        validationObject  = tableName[4:len(tableName)]
        companyCodes = 'companyCodes_' + fileName

        if((fileType == 'GLTB') and (validationObject != 'L1_MD_GLAccount')):
            periodStart = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START')
            periodEnd   = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END')
            dfInputTable = spark.sql('select * from ' + tableName +' where financialPeriod between '+periodStart+' and '+periodEnd )    
        else:
            dfInputTable = spark.sql('select * from ' + tableName )

        if (dfInputTable.count() == 0):          
          data = [{'validationObject': fileType,'companyCode1' : 'All','columnName' : 'All','numberOfBlankRows' : 'All'}]
          dfReqFields = spark.createDataFrame(data)
        else:            
            if((lstOfScopedAnalytics is None) | (len(lstOfScopedAnalytics) == 0)):                  
              dfRequiredFields = gl_metadataDictionary["dic_ddic_auditProcedureColumnMapping"].\
                      select(col("columnName")).filter((col("businessPriorityID") == 2) &\
                      (col("tableName") == validationObject.strip())).distinct()
            else:                  
              dfRequiredFields = gl_metadataDictionary["dic_ddic_auditProcedureColumnMapping"].\
                      select(col("columnName")).filter((col("businessPriorityID") == 2) &\
                      (col("tableName") == validationObject.strip()) &\
                      (col("auditProcedureName").isin(lstOfScopedAnalytics))).distinct()

            colCount = dfRequiredFields.count()
            strColumnNames = ''

            if colCount > 0:
                for r in dfRequiredFields.rdd.collect():
                    strColumnNames = strColumnNames +  ",'" + str(r["columnName"]) + "' ," + str(r["columnName"])
                requiredFieldlist = dfRequiredFields.select("columnName").rdd.flatMap(list).collect()
                dfInputTable.select("CompanyCode").createOrReplaceTempView(companyCodes)
                dfCompanycodes = spark.sql("select DISTINCT CompanyCode  from "+companyCodes+"")
                strExpr = "stack(" + str(colCount) + strColumnNames + ") as (columnName, numberOfBlankRows)"
                dfs = []
                for r in dfCompanycodes.rdd.collect():
                    strCompanyCode = str(r[0])
                    dfRequiredFields = dfInputTable.select(requiredFieldlist).filter(dfInputTable.companyCode == strCompanyCode)
                    dfNull = dfRequiredFields.\
                    select([count(when(((col(c).isNull()) | (trim(col(c)) == '') ), c)).alias(c) for c in dfRequiredFields.columns])
                
                    dfNullWithCompanycode = dfNull.withColumn('validationObject',lit(validationObject))
                    dfNullWithCompanycode = dfNullWithCompanycode.withColumn('companyCode1',lit(strCompanyCode))
                    dfRequiredFields = dfNullWithCompanycode.selectExpr("validationObject","companyCode1",strExpr ).where("numberOfBlankRows >0")
                    dfs.append(dfRequiredFields)
                dfReqFields = reduce(DataFrame.union, dfs)
            else:
                dfReqFieldSchema = StructType([
                                StructField("validationObject",StringType(),True),                                               
                                StructField("companyCode1", StringType(),True),
                                StructField("columnName", StringType(),True),
                                StructField("numberOfBlankRows", StringType(),True)                                               
                               ])
                dfReqFields = spark.createDataFrame(spark.sparkContext.emptyRDD(),dfReqFieldSchema)
            
        if (dfReqFields.count() > 0):
          executionStatusID = LOG_EXECUTION_STATUS.FAILED
          executionStatus = "Required field validation failed for the table " + validationObject
        else:
          executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
          executionStatus = "Required field validation succeeded for the table " + validationObject

        return [executionStatusID,executionStatus,dfReqFields]
                  
    except Exception as e:
        print(e)
        raise

def app_requiredField_validate(tableName,fileType,lstOfScopedAnalytics):
    try:
        objGenHelper = gen_genericHelper()   
        fileName          = tableName

        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,\
                                  fileType = fileType,\
                                  validationID = VALIDATION_ID.BLANK_DATA)

        dfrequiredField = None

        if((objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'BALANCE_TYPE')=='GLTB') and (fileType == 'GLTB')):
            lstOfStatus = _requiredField_validate(tableName,fileType,lstOfScopedAnalytics)
            lstOfStatusGla = _requiredField_validate(tableName = 'fin_L1_MD_GLAccount',fileType = fileType,\
                lstOfScopedAnalytics = lstOfScopedAnalytics)
            dfrequiredField = lstOfStatus[2].union(lstOfStatusGla[2])
        else:
            lstOfStatus =_requiredField_validate(tableName,fileType,lstOfScopedAnalytics)
            dfrequiredField = lstOfStatus[2]

        dfReqFields = dfrequiredField.withColumn("groupSlno",row_number().over(Window.orderBy('validationObject')))        
        dfrequiredFieldDtls = dfReqFields.select(col("groupSlno"),\
            col("validationObject").alias("tableName"),\
            col("companyCode1").alias("companyCode"),\
            col("columnName"),\
            col("numberOfBlankRows").cast("string"))
        
        dfValidationResultsDetails = objGenHelper.gen_dynamicPivot(
                                  fileType =fileType,
                                  validationID = VALIDATION_ID.BLANK_DATA.value,
                                  pivotColumn1 = 'companyCode',
                                  pivotColumn2 = 'tableName',
                                  pivotColumn3 = 'columnName',
                                  pivotColumn4 = 'numberOfBlankRows',
                                  dfToPivot = dfrequiredFieldDtls)
        dfValidationResultsDetails = dfValidationResultsDetails.withColumn("remarks", lit(""))
        dfValidationResultsDetails = dfValidationResultsDetails.sort("groupSlno")
            
        if (dfValidationResultsDetails.count() > 0):
          executionStatusID = LOG_EXECUTION_STATUS.FAILED
          executionStatus = "Required field validation failed for the table " + tableName
        else:
          executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
          executionStatus = "Required field validation succeeded for the table " + tableName
                  
    except Exception as e:                
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfValidationResultsDetails)
        return [executionStatusID,executionStatus]    
  
