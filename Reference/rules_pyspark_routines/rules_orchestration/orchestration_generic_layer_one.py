# Databricks notebook source
# MAGIC %md ### Layer one transfroamtion and validations

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

pip install azure-identity

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import*
from datetime import datetime,timedelta
from multiprocessing import cpu_count
import re
import itertools

# COMMAND ----------

dbutils.widgets.text("ContainerName", "")
dbutils.widgets.text("SubDirectory", "")
dbutils.widgets.text("ExecutionGroupID", "")
dbutils.widgets.text("StorageAccount", "")
dbutils.widgets.text("processStage", "")

# COMMAND ----------

gl_processStartTime = datetime.now()
print("Execution start time: " +  str(gl_processStartTime))

# COMMAND ----------

gl_analysisID = dbutils.widgets.get("ContainerName")
gl_analysisPhaseID = dbutils.widgets.get("SubDirectory")
gl_executionGroupID = dbutils.widgets.get("ExecutionGroupID")
storageAccountName = dbutils.widgets.get("StorageAccount")
gl_ERPPackageID = ""

if(gl_analysisPhaseID.find("/")== -1):
  gl_customFunctionFolder = (gl_analysisPhaseID[2:])
else:
  gl_customFunctionFolder =  gl_analysisPhaseID

# COMMAND ----------

gl_MountPoint = '/mnt/' + storageAccountName + '-' + gl_analysisID

# COMMAND ----------

# MAGIC %md ##### Compile common functions

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_common_functions

# COMMAND ----------

# MAGIC %md ##### Initilaize workspace, logs

# COMMAND ----------

global gl_enableTransformaionLog
global gl_ExecutionMode
global gl_isCustomTransform 
global gl_lstOfFilesToBeImported
global gl_fileTypes

gl_fileTypes["Input"]=set()  
gl_fileTypes["Failed"]=set() 
gl_fileTypes["Succeeded"]=set() 
      
gl_isCustomTransform = False
gl_lstOfFilesToBeImported = list()
objGenHelper = gen_genericHelper() 
objDataTransformation = gen_dataTransformation()
if (gl_ExecutionMode == EXECUTION_MODE.DEBUGGING):
    gl_enableTransformaionLog = False
else:
    gl_enableTransformaionLog = True
gl_processStage = list()

try:
    if (gl_enableTransformaionLog):
        transformationStatusLog.initializeLogs(storageAccountName)
        transformationStatusLog.createLogFile(PROCESS_ID.IMPORT)    
    
    runDirectory = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    objGenHelper.miscellaneousLogWrite(gl_executionLogResultPath, "Execution directory: " + runDirectory)

    for stage in dbutils.widgets.get("processStage").split(","):
        if(stage == '42'):
            gl_processStage = [PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION]

    if (len(gl_processStage) == 0):
        gl_processStage = [PROCESS_STAGE.IMPORT,
                           PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION]
    
    if(gl_enableTransformaionLog):
        objGenHelper.createPackageExecutionValidationResultFiles()
    
    ERPOrchestration.allPreRequisiteMetadata_load()
    GenOrchestration.paramaterFile_update(VALIDATION_ID.PACKAGE_FAILURE_IMPORT)
    
    if(PROCESS_STAGE.IMPORT in gl_processStage):      
      GenOrchestration.intializeAllOutputDirectories(VALIDATION_ID.PACKAGE_FAILURE_IMPORT)
      
    gl_ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL','ERP_SYSTEM_ID'))
    
    if (objGenHelper.gen_lk_cd_parameter_get('GLOBAL','SCOPED_ANALYTICS')) is None:
        gl_lstOfScopedAnalytics = list()
    elif ((objGenHelper.gen_lk_cd_parameter_get('GLOBAL','SCOPED_ANALYTICS')).strip() ==''):
        gl_lstOfScopedAnalytics = list()
    else:
        gl_lstOfScopedAnalytics = (objGenHelper.gen_lk_cd_parameter_get('GLOBAL','SCOPED_ANALYTICS')).split(",")
  
    gl_lstOfFilesToBeImported = gl_parameterDictionary["InputParams"].\
                                filter(col("IsCustomTransform")==gl_isCustomTransform).\
                                select(col("fileType")).distinct().rdd.flatMap(lambda x: x).collect()
    
except Exception as err:
    raise err

# COMMAND ----------

# MAGIC %md ##### Compile all the standard modules

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_transformation

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_postTransformation

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_layer_one

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

# MAGIC %md ##### Orchestration of data import, import validations

# COMMAND ----------

if((PROCESS_STAGE.IMPORT in gl_processStage) & (len(gl_lstOfFilesToBeImported)>0)):
    df_input_files_info = gl_parameterDictionary["InputParams"].filter(col("IsCustomTransform")==gl_isCustomTransform)
    df_input_files_info = df_input_files_info.withColumn("textQualifier1",when(col("textQualifier").isNull(),'"')
                                   .when(trim(col("textQualifier")) == "",'"')
                                   .otherwise(col("textQualifier"))).drop("textQualifier")
    df_input_files_info = df_input_files_info.withColumnRenamed("textQualifier1","textQualifier")  
    collect_input_files_info = df_input_files_info.collect()

# COMMAND ----------

class NotebookData:
  def __init__(self, path, timeout, parameters=None, retry=0):
     self.path = path
     self.timeout = timeout
     self.parameters = parameters
     self.retry = retry

# COMMAND ----------

def submitNotebook(notebook):
   print("Running notebook %s" % notebook.path + "\n")
   try:
     if (notebook.parameters):
       return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
     else:
       return dbutils.notebook.run(notebook.path, notebook.timeout)
   except Exception:
     if notebook.retry < 1:
       raise
     print("Retrying notebook %s" % notebook.path + "\n")
     notebook.retry = notebook.retry - 1
     submitNotebook(notebook)

# COMMAND ----------

def parallelNotebooks(notebooks, numInParallel):
   with ThreadPoolExecutor(max_workers=numInParallel) as ec:
     return [ec.submit(submitNotebook, notebook) for notebook in notebooks]

# COMMAND ----------

if((PROCESS_STAGE.IMPORT in gl_processStage) & (len(gl_lstOfFilesToBeImported)>0)):
    run_notebooks = []
    for row in collect_input_files_info:
      run_notebooks.append(NotebookData("orchestration_generic_layer_zero", 0, 
                                        {"fileID": row['fileID'], 
                                          "fileName": row['fileName'],
                                          "fileType": row['fileType'],
                                          "columnDelimiter": row['columnDelimiter'],
                                          "thousandSeperator": row['thousandSeperator'],
                                          "decimalSeperator": row['decimalSeperator'],
                                          "dateFormat": row['dateFormat'],
                                          "executionID": row['executionID'],
                                          "textQualifier": row['textQualifier'],
                                          
                                         "gl_MountPoint": gl_MountPoint,
                                         "gl_analysisID": gl_analysisID,
                                         "gl_analysisPhaseID": gl_analysisPhaseID,
                                         "gl_executionGroupID": gl_executionGroupID,
                                         "storageAccountName" : storageAccountName
                                        }))

# COMMAND ----------

if((PROCESS_STAGE.IMPORT in gl_processStage) & (len(gl_lstOfFilesToBeImported) > 0)):
    res = parallelNotebooks(run_notebooks, len(run_notebooks))

# COMMAND ----------

# MAGIC %md ###### Post-import validation

# COMMAND ----------

objL1PreProcessing =  app_layerOnePreProcessing_perfom()
importStatus = list()
if((PROCESS_STAGE.IMPORT in gl_processStage) & (len(gl_lstOfFilesToBeImported) > 0)):
    importStatus = objL1PreProcessing.layerOnePreProcessingValidation_perform()

if(len(gl_processStage) == 1 and PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION in gl_processStage):
    importStatus = objL1PreProcessing.layerOnePreProcessingValidationForDataFlow_perform()

if(len(gl_lstOfFilesToBeImported) == 0 ):
  print('No files selected for import...')
elif importStatus[0]== LOG_EXECUTION_STATUS.FAILED:
  raise Exception(importStatus[1])
elif importStatus[0]== LOG_EXECUTION_STATUS.WARNING:
  warnings.warn(importStatus[1])
else:
  print(importStatus[1])     
  


# COMMAND ----------

# MAGIC %md ###### CDM conversion

# COMMAND ----------

try:  
    status = ERPOrchestration.loadAllSourceFiles_load(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                               processStage = PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION)
    if(status[0] == LOG_EXECUTION_STATUS.FAILED):
      executionStatus = status[1]                   
      print('Loading pre-requisites for transformaiton validation failed')    
      raise Exception (objPrepareValidation[1])    
except Exception as err:
    raise err

# COMMAND ----------

objLayerOneTransformation = app_layerOneTransformation_perform()
CDMTransformationStatus = objLayerOneTransformation.GLALayerOneTransformation_perform()
if (bool(CDMTransformationStatus[0]) != True):
    raise Exception(CDMTransformationStatus[1])

# COMMAND ----------

# MAGIC %md ###### Transformation validations

# COMMAND ----------

try:
  objPrepareValidation = ERPOrchestration.prepareFilesForPostTransformation()
  if(objPrepareValidation[0] == LOG_EXECUTION_STATUS.FAILED):
    print('Loading pre-requisites for transformaiton validation failed')
    raise Exception (objPrepareValidation[1]) 
except Exception as err:
  raise err

# COMMAND ----------

try:
  lstOfMethods =  [["fin_L1_STG_GLBalanceAccumulated_populate",""],
                  ["fin_L1_STG_PYCYBalance_populate",""],
                  ["knw_LK_CD_reportingSetup_populate",""],
                  ["knw_LK_GD_DAFlagFiles_populate","{'processID':PROCESS_ID.TRANSFORMATION_VALIDATION}"]]

  lstOfValidationStatus = ERPOrchestration.submitTransformation(lstOfMethods,MAXPARALLELISM.DEFAULT.value)
except Exception as err:
  raise err

# COMMAND ----------

try:  
    lstOfValidationStatus = ERPOrchestration.submitPostTransformationValidation(objPrepareValidation[2])
    executionStatus = list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.FAILED,lstOfValidationStatus))
    if(len(executionStatus) != 0):
        print(executionStatus)
        dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                           gl_executionLogResultPath + "ExecutionLogDetails.csv")
        warnings.warn("Transformation validations failed, please check the validation logs for more details.")       
    else:
        print("Transformaiton validation completed successfully.")    
except Exception as err:
  raise err


# COMMAND ----------

# MAGIC %md ###### Generate execution log file

# COMMAND ----------

try:
    dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,
                                                            gl_logSchema,\
                                                            gl_executionLogResultPath + "ExecutionLogDetails.csv")
    transformationValidationStatus = True
    if (dfExecutionLog.filter(col("statusID") == 0).count() > 0):
        transformationValidationStatus = False   
        statusDescription = "Transformation failed, please check validation details."

    if (bool(transformationValidationStatus) != True):
        raise Exception(statusDescription)   

except Exception as err:
    raise err

# COMMAND ----------

gl_processEndTime = datetime.now()
print("Execution end time: " +  str(gl_processEndTime))
print("Execution time: " + str((gl_processEndTime - gl_processStartTime)))

# COMMAND ----------

dbutils.notebook.exit("L0 and L1 completed successfully")
