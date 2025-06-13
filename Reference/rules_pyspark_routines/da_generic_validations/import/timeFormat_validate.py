# Databricks notebook source
from dateutil.parser import parse
from pyspark.sql.functions import row_number,lit ,abs,col, asc
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType
from datetime import datetime
import pathlib
import sys
import traceback

def app_timeFormat_validate(dfSource, fileName, fileID, fileType,executionID):
    try:
        lstOfValidationResult=None
        dfTimeErrorDetail = None
        cleanColumnList = []
            
        objGenHelper = gen_genericHelper()  
        logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.TIME_FORMAT,executionID = executionID) 

        Input_df_table = 'Input_df_table_' + fileID.replace('-','_').lower()
        source_columnMapping_table = 'source_columnMapping_table_' + fileID.replace('-','_').lower()
        ddic_column_table = 'ddic_column_table_' + fileID.replace('-','_').lower()

        w = Window().orderBy(lit('rowNumber'))
        dfSource = dfSource.withColumn("rowNumber", row_number().over(w))
        dfSource.createOrReplaceTempView(Input_df_table)
                     
        gl_parameterDictionary["SourceTargetColumnMapping"].filter((col("fileID")) == fileID) \
                .createOrReplaceTempView(source_columnMapping_table)
        gl_metadataDictionary["dic_ddic_column"].filter(((col("fileType")) == fileType) & \
                ((col("dataType")) == "time")).createOrReplaceTempView(ddic_column_table)

        dfTimeColumnsList = spark.sql('select '
                                 'C.sourceColumn '
                                 'from '+ source_columnMapping_table +' AS C '
                                 'inner join '+ ddic_column_table +' AS D '
                                 '    on  D.columnName = C.targetColumn')

        rowCount = dfTimeColumnsList.count()
        timeColumnCount = rowCount
        cleanColumnList = dfTimeColumnsList.select(col("sourceColumn")).rdd.flatMap(lambda x: x).collect()   

        dfTimeError = spark.sql('select rowNumber,"False" as validateresult,"" as Column,"" as Value from '+ Input_df_table +' where 1 = 2')
            
        while rowCount > 0:
            timeColumnName = dfTimeColumnsList.select('sourceColumn').collect()[rowCount - 1][0]
            dftime = spark.sql("select "\
                                        "cast(rowNumber as string) , "\
                                        "case " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^([1-9]|0[1-9]|1[0-2]):([0-5][0-9]):([0-5][0-9]) ([AP]M|[ap]m)',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^([0-9]|0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^([0-9]|0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]).([0-5][0-9])',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^([0-9]|0[0-9]|1[0-9]|2[0-3]).([0-5][0-9]).([0-5][0-9])',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^([0-9]|0[0-9]|1[0-9]|2[0-3]).([0-5][0-9])',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^([0-9]|0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^(0[1-9]|1[0-2])([0-5][0-9])([0-5][0-9]) ([AP]M|[ap]m)',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^(0[0-9]|1[0-9]|2[0-3])([0-5][0-9])([0-5][0-9])',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "when regexp_extract(trim(`"+timeColumnName+"`),'^([0-9]|0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])([.][0-9]*)',0) == trim(`"+timeColumnName+"`) " \
                                            "then 'True' " \
                                            "else 'False' " \
                                        "end as validateresult , "\
                                        " '"+timeColumnName+"' As ColumnName , "\
                                        "`"+timeColumnName+"` as ColumnValue "\
                                    "from "+ Input_df_table +" "\
                                    "where nullif(ltrim(rtrim(`"+timeColumnName+"`)),'') is not null ")

            dfTimeError = dfTimeError.union(dftime.filter(dftime.validateresult == 'False'))
            rowCount = rowCount - 1
                
        w = Window().orderBy(lit('groupSlno'))
        dfTimeError = dfTimeError.withColumn("groupSlno", row_number().over(w))
        dfTimeError = dfTimeError.filter(dfTimeError.groupSlno <= gl_maximumNumberOfValidationDetails)
        errorCount = dfTimeError.count()
            
        if errorCount == 0:
            executionStatus = "Time Format validation succeeded."
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        else:
            executionStatus = "Time Format validation failed."
            executionStatusID = LOG_EXECUTION_STATUS.FAILED
            dfTimeErrorDetail= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.TIME_FORMAT.value,
                                      pivotColumn1='Column',
                                      pivotColumn2='Value',
                                      pivotColumn3='rowNumber',
                                      dfToPivot=dfTimeError)
            remarks = "Invalid time format"
            dfTimeErrorDetail = dfTimeErrorDetail.withColumn("remarks", lit(remarks))
            
    except Exception as e:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail  = dfTimeErrorDetail) 
        return [executionStatusID,executionStatus,dfTimeErrorDetail,cleanColumnList,VALIDATION_ID.TIME_FORMAT.value]



