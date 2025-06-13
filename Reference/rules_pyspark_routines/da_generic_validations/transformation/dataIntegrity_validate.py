# Databricks notebook source
from pyspark.sql.functions import row_number,expr
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType
from datetime import datetime
import uuid
import sys
import traceback

def app_dataIntegrity_validate(tableName,fileType):
    try:
        objGenHelper = gen_genericHelper()  

        global gl_scoped_analytics 
        validationObject  = tableName[4:len(tableName)]
        fileName          = tableName

        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.DUPLICATE_RECORD)

        dfValidationDtl = None
        if((objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'BALANCE_TYPE')=='GLTB') and (fileType == 'GLTB')):
            executionStatus = _duplicateRecordValidation(tableName,validationObject,fileName,fileType,VALIDATION_ID.DUPLICATE_RECORD.value)
            executionStatusGla = _duplicateRecordValidation('fin_L1_MD_GLAccount','L1_MD_GLAccount','fin_L1_MD_GLAccount',fileType,VALIDATION_ID.DUPLICATE_RECORD.value)
            dfValidationDtl = executionStatus[2].union(executionStatusGla[2])
        else:
            executionStatus = _duplicateRecordValidation(tableName,validationObject,fileName,fileType,VALIDATION_ID.DUPLICATE_RECORD.value)
            dfValidationDtl = executionStatus[2]


        if (dfValidationDtl.count() > 0):
          executionStatusID = LOG_EXECUTION_STATUS.FAILED
          executionStatus = "Duplicate record validation for the table " + tableName
        else:
          executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
          executionStatus = "Duplicate record validation succeeded for the table " + tableName

        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfValidationDtl)                
        return [executionStatusID,executionStatus]
    
    except Exception as e:        
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED             
        executionLog.add(executionStatusID,executionStatus,dfDetail = None)
        return [executionStatusID,executionStatus]
        
def _duplicateRecordValidation(InputTable,validationObject,fileName,fileType,validationID):
    try:
        duplicateRecords = 'duplicateRecords_' + InputTable
        objGenHelper = gen_genericHelper()
        validationId = VALIDATION_ID.DUPLICATE_RECORD.value
        
        df4 = None
            
        df1 = gl_metadataDictionary["dic_ddic_column"].select("fileType","tableName","columnName", "isKey")
        df1 = df1.filter((df1.tableName == validationObject) & (df1.isKey == "X"))
        str1 = ""
         
        for r in df1.rdd.collect():
            str1 =  str1 +  "cast("+str(r["columnName"]) +" AS string)" +","
               
        str1 = str1.rstrip(',')
        if((fileType == 'GLTB') and (validationObject != 'L1_MD_GLAccount')):
          endMonth   = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END')
        
          str1 = ('select '+str1+' , cast(count(*) AS string) AS duplicated from '+InputTable+' \
                  where financialPeriod <= '+endMonth+'\
                  group by '+str1+' having count(*) > 1')
        else:
          str1 = ('select '+str1+' ,cast(count(*) AS string) AS duplicated from '+InputTable+' \
                 group by '+str1+' having count(*) > 1')
                
        df3 = spark.sql(str1)
        w = Window().orderBy(lit('groupSlno'))
        df3 = df3.withColumn("groupSlno", row_number().over(w))
        df3 = df3.filter(df3.groupSlno <= gl_maximumNumberOfValidationDetails)
        df3.createOrReplaceTempView(duplicateRecords)
        df4 = spark.sql('select * , '+str(validationId)+' AS validationID , "'+fileType+'" AS validationObject,\
              "'+fileType+'" AS fileType , "" AS remarks from '+ duplicateRecords +' order by groupSlno')
            
        expression = ""
        cnt = 0
        
        for column in df4.columns:
            if ((column != 'validationID') & (column != 'groupSlno') & (column != 'validationObject') \
                & (column != 'fileType') & (column != 'remarks')):
                cnt += 1
                expression += f"'{column}' , {column},"
                
        expression = f"stack({cnt}, {expression[:-1]}) as (resultKey,resultValue)"
        df4 = df4.select( "groupSlno","validationID", "validationObject", "fileType", expr(expression), "remarks")           
                       
        errorCount = 0
        errorCount = df3.count() 
            
         
        if errorCount == 0:
            executionStatus = "Duplicate record validation succeeded for table "+validationObject+"."
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        else:
            executionStatus = "Duplicate record validation failed for table "+validationObject+"."
            executionStatusID = LOG_EXECUTION_STATUS.FAILED 

        return [executionStatusID,executionStatus,df4]
    except Exception as e:
        raise    
                              
