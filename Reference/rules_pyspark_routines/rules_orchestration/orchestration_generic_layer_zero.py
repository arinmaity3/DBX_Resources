# Databricks notebook source
# MAGIC %md #### ERP layer zero to layer one transfromation, validations

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

pip install azure-identity

# COMMAND ----------

pip install chardet

# COMMAND ----------

# MAGIC %md ##### Variables &Initialization

# COMMAND ----------

dbutils.widgets.text("fileID", "")
dbutils.widgets.text("fileName", "")
dbutils.widgets.text("fileType", "")
dbutils.widgets.text("columnDelimiter", "")
dbutils.widgets.text("thousandSeperator", "")
dbutils.widgets.text("decimalSeperator", "")
dbutils.widgets.text("dateFormat", "")
dbutils.widgets.text("executionID", "")
dbutils.widgets.text("textQualifier", "")

dbutils.widgets.text("gl_MountPoint", "")
dbutils.widgets.text("gl_analysisID", "")
dbutils.widgets.text("gl_analysisPhaseID", "")
dbutils.widgets.text("gl_executionGroupID", "")
dbutils.widgets.text("storageAccountName", "")

# COMMAND ----------

# MAGIC %md #### install pre requisite libraries 

# COMMAND ----------

import pathlib
from pathlib import Path
fileName = dbutils.widgets.get("fileName")
inputFileType = pathlib.Path(fileName).suffix.replace(".","").strip().lower()
if (inputFileType == 'xlsx'):
    %pip install openpyxl

# COMMAND ----------

import pathlib
from pathlib import Path
import uuid
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

# COMMAND ----------

fileID = dbutils.widgets.get("fileID")
fileName = dbutils.widgets.get("fileName")
fileType = dbutils.widgets.get("fileType")
columnDelimiter = dbutils.widgets.get("columnDelimiter")
thousandSeperator = dbutils.widgets.get("thousandSeperator")
decimalSeperator = dbutils.widgets.get("decimalSeperator")
dateFormat = dbutils.widgets.get("dateFormat")
executionID = dbutils.widgets.get("executionID")
textQualifier = dbutils.widgets.get("textQualifier")

gl_MountPoint = dbutils.widgets.get("gl_MountPoint")
gl_analysisID = dbutils.widgets.get("gl_analysisID")
gl_analysisPhaseID = dbutils.widgets.get("gl_analysisPhaseID")
gl_executionGroupID = dbutils.widgets.get("gl_executionGroupID")
storageAccountName = dbutils.widgets.get("storageAccountName")
gl_ERPPackageID = ""

if(gl_analysisPhaseID.find("/")== -1):
  gl_customFunctionFolder = (gl_analysisPhaseID[2:])
else:
  gl_customFunctionFolder =  gl_analysisPhaseID

# COMMAND ----------

# MAGIC %md ##### Compile common functions

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_common_functions

# COMMAND ----------

# MAGIC %md ##### Compile all the standard modules

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_import_validations

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_layer_zero

# COMMAND ----------

# MAGIC %md ##### Initilaize workspace, logs

# COMMAND ----------

global gl_enableTransformaionLog 
global gl_ExecutionMode
objGenHelper = gen_genericHelper() 
objDataTransformation = gen_dataTransformation()
if (gl_ExecutionMode == EXECUTION_MODE.DEBUGGING):
    gl_enableTransformaionLog = False
else:
    gl_enableTransformaionLog = True

try:
    transformationStatusLog.initializeLogs(storageAccountName)           
    transformationStatusLog.createLogFile(PROCESS_ID.IMPORT_VALIDATION,executionID)
    
    ERPOrchestration.allPreRequisiteMetadata_load()
    gl_ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL','ERP_SYSTEM_ID'))
    inputFileType = pathlib.Path(fileName).suffix.replace(".","").strip().lower()

    filePath = ''
    filePath = gl_rawFilesPath + fileName
    fileEncoding = ''
except Exception as e:
  raise e

# COMMAND ----------

# MAGIC %md ##### Compile custom functions

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_importCustomFunctions

# COMMAND ----------

try:
  gen_customFunctions.customFunctions_load(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
                                                 gl_customFunctionFolder)
except Exception as err:
    raise err

# COMMAND ----------

# MAGIC %md ##### Prepare import validations

# COMMAND ----------

try:

  dfImportValidation = ERPOrchestration.prepareFilesForImportValidation(processID = PROCESS_ID.IMPORT_VALIDATION,
                                                  fileID = fileID,
                                                  fileType = fileType,
                                                  fileName = fileName,
                                                  validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                                  executionID = executionID)
except Exception as err:
    layerZeroProcessingStatus.updateExecutionStatus()
    raise err
 

# COMMAND ----------

try:
  if not(dfImportValidation.filter(col("validationID") == VALIDATION_ID.FILE_ENCODING.value).first() is None):
    fileEncoding = app_fileEncoding_validate(fileName, fileID, fileType, executionID, filePath)[2]
    
    #executionStatus = gen_usf_rawFileEncoding_convert(inputFileType,fileID,fileType,fileName,executionID,fileEncoding)
    try:
      logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,\
                                fileID,fileName,fileType,\
                                validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                phaseName = 'Rawfile convert to UTF-8',\
                                executionID = executionID)
      
      if (fileEncoding is not None and fileEncoding != 'UTF8' 
               and fileEncoding.upper() != 'ASCII' and inputFileType not in ['xlsx']):

        executionStatus_Convert = objDataTransformation.gen_UTF8_convert(fileName = fileName,
                                  fileEncoding = fileEncoding,
                                  filePath = filePath,
                                  convertedRawFilesPath = gl_convertedRawFilesPath,
                                  inputFileType=inputFileType)

        if(executionStatus_Convert[0] == LOG_EXECUTION_STATUS.SUCCESS):
          fileName = executionStatus_Convert[2]
          filePath = executionStatus_Convert[3]
          fileEncoding = executionStatus_Convert[4]
          gl_rawFilesPath = gl_convertedRawFilesPath
          executionStatusDesc='Rawfile converted to UTF-8 successfully'
          executionStatusID=LOG_EXECUTION_STATUS.SUCCESS

      else:
         print("Conversion was not performed. gl_fileEncoding value:",fileEncoding)
         print("Fileextension:", inputFileType)
         executionStatusDesc= "No conversion required."
         executionStatusID = LOG_EXECUTION_STATUS.SUCCESS  
    except:
          executionStatusDesc = objGenHelper.gen_exceptionDetails_log()
          executionStatusID=LOG_EXECUTION_STATUS.FAILED
    finally:
          executionLog.add(executionStatusID,logID,executionStatusDesc)
          executionStatus=[executionStatusID,executionStatusDesc,fileName,fileEncoding]  
      
    if(executionStatus[0] == LOG_EXECUTION_STATUS.FAILED):
      layerZeroProcessingStatus.updateExecutionStatus()
      raise Exception (executionStatus[1])    
except Exception as err:
  layerZeroProcessingStatus.updateExecutionStatus()
  raise err

# COMMAND ----------

#excute the import vaidations [13,17,32,19]
try:
  lstOfValidations = [VALIDATION_ID.COLUMN_DELIMITER.value
                     ,VALIDATION_ID.ZERO_KB.value
                     ,VALIDATION_ID.DATA_PERSISTENCE.value
                     ,VALIDATION_ID.ZERO_ROW_COUNT.value]

  lstOfImportStatus = ERPOrchestration.submitImportValidation(dfImportValidation, lstOfValidations)
except Exception as err:
    layerZeroProcessingStatus.updateExecutionStatus()
    raise err

# COMMAND ----------

# MAGIC %md ##### import raw file

# COMMAND ----------

try:
  obj_gen_rawFiles_import = gen_rawFiles_import()
  executionStatus = obj_gen_rawFiles_import.importRawFiles(fileName,fileID,fileType,columnDelimiter,executionID,textQualifier)
  if(executionStatus[0] == LOG_EXECUTION_STATUS.FAILED):
    layerZeroProcessingStatus.updateExecutionStatus()
    raise Exception (executionStatus[1])
  dfSource = executionStatus[2]
except Exception as err:
  layerZeroProcessingStatus.layerZeroProcessingStatus.updateExecutionStatus()
  raise

# COMMAND ----------

#excute the import vaidations [2,3,31,67]
try:  
  lstOfValidations = list()
  lstOfValidations = [VALIDATION_ID.NUMBER_FORMAT.value
                       ,VALIDATION_ID.DATE_FORMAT.value
                       ,VALIDATION_ID.UPLOAD_VS_IMPORT.value
                       ,VALIDATION_ID.TIME_FORMAT.value]

  if (inputFileType == 'xlsx'):
      ls_OfNumColumns = []
      ls_OfDateColumns = []
      ls_OfTimeColumns = []

  lstOfImportStatus = ERPOrchestration.submitImportValidation(dfImportValidation, lstOfValidations)
  for ls_result in lstOfImportStatus:
    if ls_result[4] == VALIDATION_ID.NUMBER_FORMAT.value:
      ls_OfNumColumns = ls_result[3]
    elif ls_result[4] == VALIDATION_ID.DATE_FORMAT.value:
      ls_OfDateColumns = ls_result[3]
    elif ls_result[4] == VALIDATION_ID.TIME_FORMAT.value:
      ls_OfTimeColumns = ls_result[3]
  
  if not (dfImportValidation == None):
    dfImportValidation.unpersist()
    
except Exception as err:
    layerZeroProcessingStatus.updateExecutionStatus()
    raise err

# COMMAND ----------

# MAGIC %md ##### Convert data to standard format 

# COMMAND ----------

try:
  objDataConversion = app_transformDataToStandardFormat()
  executionStatus = objDataConversion.app_convertDataToStandardFormat\
                (dfSource,fileID,fileType,ls_OfNumColumns,\
                ls_OfDateColumns,ls_OfTimeColumns,decimalSeperator,\
                thousandSeperator,dateFormat,fileName)
  if(executionStatus[0] == LOG_EXECUTION_STATUS.FAILED):
    layerZeroProcessingStatus.updateExecutionStatus()
    raise Exception (executionStatus[1])
  dfSource = executionStatus[2]
except Exception as err:
  layerZeroProcessingStatus.updateExecutionStatus()
  raise

# COMMAND ----------

# MAGIC %md ##### Layer zero post-import

# COMMAND ----------

objPostProcessing =  app_layerZeroPostProcessing_perform()
objPostProcessing.collectAllRawFiles(dfSource,fileID,fileType,fileName,executionID)
