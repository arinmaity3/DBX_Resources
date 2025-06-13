# Databricks notebook source
from dateutil.parser import parse
from pyspark.sql.functions import row_number,lit ,abs,col, asc
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType
from datetime import datetime
import pathlib
import sys
import traceback
import re

def app_dateFormat_validate(dfSource, fileName,fileID,fileType,executionID,dateFormat):
    try:
        
        lstOfValidationResult=None
        dfErrorDetail = None
        cleanedColumnList = []
            
        objGenHelper = gen_genericHelper()  
        logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.DATE_FORMAT,executionID = executionID) 

        df1_table = 'df1_table_' + fileID.replace('-','_').lower()
        columnMapping_table = 'columnMapping_table_' + fileID.replace('-','_').lower()
        dic_ddic_column_table = 'dic_ddic_column_table_' + fileID.replace('-','_').lower()

        if dateFormat is None:
            dateFormat = 'YYYY-MM-DD'
        w = Window().orderBy(lit('rowNumber'))
        dfSource = dfSource.withColumn("rowNumber", row_number().over(w))
        dfSource.createOrReplaceTempView(df1_table)
                     
        gl_parameterDictionary["SourceTargetColumnMapping"].filter((col("fileID")) == fileID).createOrReplaceTempView(columnMapping_table)
        gl_metadataDictionary["dic_ddic_column"].filter(((col("fileType")) == fileType) & ((col("dataType")) == "date")).\
                                        createOrReplaceTempView(dic_ddic_column_table)
        dfDateColumnsList = spark.sql('select '
                                 'C.sourceColumn '
                                 'from '+ columnMapping_table +' AS C '
                                 'inner join '+ dic_ddic_column_table +' AS D '
                                 '    on  D.columnName = C.targetColumn')

        rowCount = dfDateColumnsList.count()
        dateColumnCount = rowCount
        cleanedColumnList = dfDateColumnsList.select(col("sourceColumn")).rdd.flatMap(lambda x: x).collect()   

        dfError = spark.sql('select rowNumber,"False" as validateresult,"" as Column,"" as Value from '+ df1_table +' where 1 = 2')
            
        date_format_sql = dateFormat.replace("YYYY","yyyy").replace("DD","d").replace("MM","M")
        while rowCount > 0:
            dateColumnName = dfDateColumnsList.select('sourceColumn').collect()[rowCount - 1][0]
            df6 = spark.sql('select cast(rowNumber as string) ,\
                                    case when TO_DATE(`'+dateColumnName+'`,"'+date_format_sql+'") \
                                    is null then "False" else "True" end as validateresult ,\
                                    "'+dateColumnName+'" As ColumnName , `'+dateColumnName+'` as ColumnValue \
                                    from '+ df1_table +' where nullif(ltrim(rtrim(`'+dateColumnName+'`)),"''") is not null ')
            dfError = dfError.union(df6.filter(df6.validateresult == 'False'))
            rowCount = rowCount - 1
            df6.first()

        w = Window().orderBy(lit('groupSlno'))
        dfError = dfError.withColumn("groupSlno", row_number().over(w))
        dfError = dfError.filter(dfError.groupSlno <= gl_maximumNumberOfValidationDetails)
        errorCount = dfError.count()
            
        if errorCount == 0:
            executionStatus = "Date Format validation succeeded."
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        else:
            executionStatus = "Date Format validation failed."
            executionStatusID = LOG_EXECUTION_STATUS.FAILED
            dfErrorDetail= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.DATE_FORMAT.value,
                                      pivotColumn1='Column',
                                      pivotColumn2='Value',
                                      pivotColumn3='rowNumber',
                                      dfToPivot=dfError)
            remarks = "Invalid date format"
            dfErrorDetail = dfErrorDetail.withColumn("remarks", lit(remarks))
           
    except Exception as e:
      lsdatefrmt=re.findall(r"Fail to parse '.*' in the new parser",str(e))
      if lsdatefrmt:
        invalidDate= lsdatefrmt[0].replace("Fail to parse '",'').replace("' in the new parser",'')
        dfError = spark.sql('select 1 as rowNumber,1 as groupSlno,"False" as validateresult,\
                            "'+dateColumnName+'" as Column,"'+invalidDate+'" as Value ')

        executionStatus = "Date Format validation failed."
        executionStatusID = LOG_EXECUTION_STATUS.FAILED

        dfErrorDetail = objGenHelper.gen_dynamicPivot(fileType =fileType,
                              validationID =VALIDATION_ID.DATE_FORMAT.value,
                              pivotColumn1='Column',
                              pivotColumn2='Value',
                              pivotColumn3='rowNumber',
                              dfToPivot=dfError)
        
        remarks = "Invalid date format"
        dfErrorDetail = dfErrorDetail.withColumn("remarks",lit(remarks))
        
      else:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail  = dfErrorDetail)
        return [executionStatusID,executionStatus,dfErrorDetail,cleanedColumnList,VALIDATION_ID.DATE_FORMAT.value]                   
