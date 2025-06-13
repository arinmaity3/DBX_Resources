# Databricks notebook source
import codecs
import pathlib
from pathlib import Path
from pyspark.sql import Row
import io
import os

def gen_usf_rawFileEncoding_convert(inputFileType,fileID,fileType,fileName,executionID,fileEncoding):
    try:
          global gl_lstOfImportValidationSummary

          executionStatus=""
          logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.PACKAGE_FAILURE_IMPORT,executionID = executionID) 
          detectedEncoding = fileEncoding
          if detectedEncoding is not None and detectedEncoding != 'UTF8' and detectedEncoding.upper() != 'ASCII' and inputFileType not in ['xls','xlsx'] :
                print("Encoding before conversion: "+detectedEncoding)
                BLOCKSIZE = 1048576 # or some other, desired size in bytes
                global gl_rawFilesPath
                source = 'cp ' + '"/dbfs' + gl_rawFilesPath + fileName + '"'
                os.system(source + ' /tmp')
                targetFile = "/tmp/" + fileName
                with io.open(file = targetFile, mode="r", encoding =detectedEncoding) as sourceFile:
                  gl_rawFilesPath = gl_convertedRawFilesPath
                  with io.open(file = "/dbfs/"+gl_rawFilesPath +"Converted_"+ fileName, mode="w", encoding = "utf-8") as targetFile:
                     while True:
                        contents = sourceFile.read(BLOCKSIZE)
                        if not contents:
                          break
                        targetFile.write(contents)
                newFileName = '"'+ fileName + '"'
                os.system('rm /tmp/' + newFileName)
                fileName = 'Converted_'+fileName
                fileEncoding = 'UTF8'
                print("Encoding after conversion: "+fileEncoding)
                executionStatus =  "File conversion from "+detectedEncoding+" to UTF8 succeeded."
          else:
              print("Conversion was not performed. gl_fileEncoding value:",fileEncoding)
              print("Fileextension:", inputFileType)
              executionStatus =  "No conversion required."
          executionStatusID = LOG_EXECUTION_STATUS.SUCCESS                 
    except Exception as e:
          executionStatus =  objGenHelper.gen_exceptionDetails_log()
          executionStatusID = LOG_EXECUTION_STATUS.FAILED       
    finally:
        executionLog.add(executionStatusID,logID,executionStatus)
        return [executionStatusID,executionStatus,fileName,fileEncoding]
