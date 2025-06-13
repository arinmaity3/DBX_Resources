# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import when, col,lit, length,rpad,regexp_replace,date_format,to_timestamp,lpad,trim
import sys
import traceback

class app_transformDataToStandardFormat():
    
    def app_convertDataToStandardFormat(self,dfSource,fileID,fileType,ls_OfNumColumns,ls_OfDateColumns,ls_OfTimeColumns,decimalSeperator,thousandSeperator,dateFormat,fileName):
        try:
            global gl_lstOfImportValidationSummary
            global gl_lstOfImportValidationDetails
            lstOfValidationResult = list()

            objGenHelper = gen_genericHelper()
  
            logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.PACKAGE_FAILURE_IMPORT,executionID = executionID)

            inputFileType = pathlib.Path(fileName).suffix.replace(".","").strip().lower()
            if (inputFileType == 'xlsx'):
                dfSource = objGenHelper.gen_convertToStandardDateFormatExcel_perform(dfSource,fileName,fileID,fileType,dateFormat)
            else:
                dfSource = objGenHelper.gen_convertToStandardNumberFormat_perform(dfSource,ls_OfNumColumns,decimalSeperator,thousandSeperator)
                dfSource = objGenHelper.gen_convertToStandardDateFormat_perform(dfSource,ls_OfDateColumns,dateFormat)
                dfSource = objGenHelper.gen_convertToStandardTimeFormat_perform(dfSource,ls_OfTimeColumns)

            executionStatus = "Successfully converted date and number fields to standard format"
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
            print("Data convertion to standard time and date completed..")

        except Exception as err:
            executionStatus =  objGenHelper.gen_exceptionDetails_log()
            executionStatusID = LOG_EXECUTION_STATUS.FAILED
            print("Data convertion to standard time and date failed..")
        finally:
            executionLog.add(executionStatusID,logID,executionStatus)
            return [executionStatusID,executionStatus,dfSource]  
            
