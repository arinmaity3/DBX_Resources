# Databricks notebook source
# MAGIC %md #### ERP layer zero to layer one orchestration

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

pip install azure-identity

# COMMAND ----------

pip install chardet

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")
from datetime import datetime,timedelta
from delta import *
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import itertools
from pyspark.sql.functions import concat, col

# COMMAND ----------

gl_processStartTime = datetime.now()
print("Execution start time: " +  str(gl_processStartTime))

# COMMAND ----------

dbutils.widgets.text("ContainerName", "")
dbutils.widgets.text("SubDirectory", "")
dbutils.widgets.text("ExecutionGroupID", "")
dbutils.widgets.text("StorageAccount", "")
dbutils.widgets.text("ERPPackageID", "")
dbutils.widgets.text("processStage", "")

# COMMAND ----------

gl_analysisID = dbutils.widgets.get("ContainerName")
gl_analysisPhaseID = dbutils.widgets.get("SubDirectory")
gl_executionGroupID = dbutils.widgets.get("ExecutionGroupID")
storageAccountName = dbutils.widgets.get("StorageAccount")
gl_ERPPackageID = dbutils.widgets.get("ERPPackageID")
gl_processStage = list()
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
objGenHelper = gen_genericHelper() 
objDataTransformation = gen_dataTransformation()
if (gl_ExecutionMode == EXECUTION_MODE.DEBUGGING):
    gl_enableTransformaionLog = False
else:
    gl_enableTransformaionLog = True

try:
    
  gl_processStage.clear()
  for stage in dbutils.widgets.get("processStage").split(","):
    gl_processStage.append(PROCESS_STAGE(int(stage)))
  dbutils.fs.rm(gl_executionLogResultPath + 'ExecutionLogDetails.csv')    
  transformationStatusLog.initializeLogs(storageAccountName)
  transformationStatusLog.phasewiseExecutionStatusInit()      
  objGenHelper.createPackageExecutionValidationResultFiles()

  ERPOrchestration.allPreRequisiteMetadata_load()
  gl_ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL','ERP_SYSTEM_ID'))
  gl_lstOfScopedAnalytics = (objGenHelper.gen_lk_cd_parameter_get('GLOBAL','SCOPED_ANALYTICS')).split(",")

  runDirectory = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  objGenHelper.miscellaneousLogWrite(gl_executionLogResultPath, "Execution directory: " + runDirectory)
  
except Exception as err:
    transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
    raise err

# COMMAND ----------

# MAGIC %md ##### Compile all the standard modules

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_import_validations

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_cleansing

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_preTransformation

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_transformation

# COMMAND ----------

# MAGIC %run ../kpmg_rules_sap/compile/transformationSAP_compile

# COMMAND ----------

# MAGIC %run ../kpmg_rules_oracle/compile/transformationORA_compile

# COMMAND ----------

# MAGIC %run ../kpmg_rules_compile_all/compile_postTransformation

# COMMAND ----------

# MAGIC %md ##### Compile custom functions

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_importCustomFunctions

# COMMAND ----------

try:
  gen_customFunctions.customFunctions_load(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
                                                 gl_customFunctionFolder)
except Exception as err:
    transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
    raise err

# COMMAND ----------

# MAGIC %md ##### Data import,validtion, CDM conversion

# COMMAND ----------

if(PROCESS_STAGE.IMPORT in gl_processStage):
    try: 
        transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.UPLOAD)
        transformationStatusLog.createLogFile(PROCESS_ID.IMPORT)   
        #ERPOrchestration.enableAccountMapping(VALIDATION_ID.PACKAGE_FAILURE_IMPORT)
        logFileUpdateStatus =  ERPOrchestration.loadLogFile()
        if(logFileUpdateStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):        
          importPrepareStatus = ERPOrchestration.prepareFilesForImport_prepare()
          if(importPrepareStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):
            ERPOrchestration.generateCDMStructureForERPFiles() 
            print("Initialization completed")
          else:
            print(importPrepareStatus[1])
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
            raise Exception ("Unable to prepare the list of files to be imported")
        else:
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
          print(logFileUpdateStatus[1])
          raise Exception ("Unable to load log file")
    except Exception as err:
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
      raise err

# COMMAND ----------

if(PROCESS_STAGE.IMPORT in gl_processStage):
    try:  
      statusImport = True
      lstOfDataImportStatus = ERPOrchestration.submitDataImportAndValidation()      
      dataImportStatus =  list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.FAILED, lstOfDataImportStatus))
      if(len(dataImportStatus)>0):
        statusImport = False
        executionStatus = "Data import and validations got failed, please check the import status for more details." + dataImportStatus[0][1]          
        raise Exception (executionStatus)  
      else:
          print("Data import and validation completed successfully.")          
    except Exception as err:
        statusImport = False                        
        raise Exception (err)  
    finally:
        if(statusImport == False):
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)

        executionStatus = ERPOrchestration.dataImportPostProcessing_perform()
        ERPOrchestration.setStagingRecordCount()
        ERPOrchestration.enableAccountMapping(VALIDATION_ID.PACKAGE_FAILURE_IMPORT)

        if(executionStatus[0] == LOG_EXECUTION_STATUS.FAILED):
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
            print('Staging file record count update got failed.')

        dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                               gl_executionLogResultPath + "ExecutionLogDetails.csv")
        
        if (dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).count() != 0):
            print(dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).select(col("comments")).first()[0])
            raise Exception ("Data import and validation got failed, please check the log details for more details.")

# COMMAND ----------

# MAGIC %md ##### Data cleansing - Remove duplicate

# COMMAND ----------

if(PROCESS_STAGE.CLEANSING in gl_processStage):
    try:
        transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.IMPORT)
        transformationStatusLog.createLogFile(PROCESS_ID.CLEANSING)  
        df_RD_KeyColumns = None
        status = ERPOrchestration.prepareFilesForCleansing_prepare()
        df_RD_KeyColumns = status[2]
        if (status[0] == LOG_EXECUTION_STATUS.FAILED):
            print(status[1])
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.CLEANSING)
            raise Exception ("Unable to load all pre-requisities files for cleansing")
    except Exception as err:
        transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.CLEANSING)
        raise err

# COMMAND ----------

if(PROCESS_STAGE.CLEANSING in gl_processStage):
    try:
      statusCleansing = True
      executionStatus = ""  
      lstOfDuplicateRemovalStatus = ERPOrchestration.submitDataCleansing()   
      cleansingStatus =  list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.FAILED, lstOfDuplicateRemovalStatus))  
      if(len(cleansingStatus) == 0):        
        executionStatus = "Remove duplicate completed successfully."  
        print(executionStatus)
      else:
          statusCleansing = False          
          executionStatus = "DuplicateRemoval failed, please check the Cleansing status for more details."
          raise Exception (executionStatus)
    except Exception as err:
      statusCleansing = False
      executionStatus ='Data cleansing - Remove duplicate failed.'      
      raise err
    finally:
      if df_RD_KeyColumns is not None:
        df_RD_KeyColumns.unpersist()

      if(statusCleansing == False):
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.CLEANSING)

      ERPOrchestration.updateStagingRecordStatus(lstOfDuplicateRemovalStatus)
      dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                               gl_executionLogResultPath + "ExecutionLogDetails.csv")
      if (dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).count() != 0):
          print(dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).select(col("comments")).first()[0])
          raise Exception ("Data cleansing failed, please check validation status file for more details.")

# COMMAND ----------

# MAGIC %md ##### Data cleansing - Decimal point shifting

# COMMAND ----------

if(PROCESS_STAGE.DPS in gl_processStage):
    if(gl_ERPSystemID != 12):  
      objGenHelper = gen_genericHelper()
      gl_enableTransformaionLog = True
      try:
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.CLEANSING)
          transformationStatusLog.createLogFile(PROCESS_ID.DPS)  
          status = ERPOrchestration.prepareFilesForDPS()
          if (status[0] == LOG_EXECUTION_STATUS.FAILED):
              print(status[1])
              transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.DPS)
              raise Exception ("Unable to load all pre-requisities files for cleansing")
      except Exception as err:
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.DPS)
          raise

# COMMAND ----------

if(PROCESS_STAGE.DPS in gl_processStage):
    if(gl_ERPSystemID != 12):  
      try:
        executionStatus = ""  
        lstOfDPSStatus = ERPOrchestration.submitDPS() 
        dpsStatus =  list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.FAILED, lstOfDPSStatus))
        if(len(dpsStatus) == 0):
          executionStatus = "DecimalPointShifting completed successfully."  
        else:
          executionStatus = "DecimalpointShifting failed, please check the Cleansing status for more details.",dpsStatus[0][1]  
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.DPS)
          raise Exception (executionStatus)

      except Exception as err:
        transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.DPS)
        executionStatus ='Data cleansing - DecimalpointShifting failed.'  
        raise err
      finally:
        dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                               gl_executionLogResultPath + "ExecutionLogDetails.csv")    
        if (dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).count() != 0):
            print(dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).select(col("comments")).first()[0])
            raise Exception ("DPS failed, please check validation status file for more details.")

# COMMAND ----------

# MAGIC %md ##### Pre transformation and validations

# COMMAND ----------

if(PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION in gl_processStage):
    try:
        transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.DPS)
        status = ERPOrchestration.prepareFilesForPreTransformationValidation()
        if (status[0] == LOG_EXECUTION_STATUS.FAILED):
            print(status[1])
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION)
            raise Exception ("Unable to load all pre-requisities files for pre-transformation")
        else:
            dfPreTransRoutines = status[2]
    except Exception as err:
        raise err

# COMMAND ----------

if(PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION in gl_processStage):
    try:  
      lstOfPreTransStatus = ERPOrchestration.submitPreTransformationValidation(dfPreTransRoutines)  
      lstOfStatus = list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.FAILED, lstOfPreTransStatus))
      if(len(lstOfStatus) !=0):
        lstErrorMsg = list()
        [lstErrorMsg.append(s[1]) for s in lstOfPreTransStatus if s[0] == LOG_EXECUTION_STATUS.FAILED]
        print(lstErrorMsg)
        transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION)
        raise Exception ("Pre transformation validation failed, please check the validation results for more details.")
      else:
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION)
          print("Pre transformation validation completed")
    except Exception as err:
      print(err)
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION)
      raise 
    finally:
      dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                               gl_executionLogResultPath + "ExecutionLogDetails.csv")
      if (dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).count() != 0):
          print(dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).select(col("comments")).first()[0])
          raise Exception ("Pre transformationstep failed, please check validation status file for more details.")

# COMMAND ----------

# MAGIC %md ##### Transformation

# COMMAND ----------

if(PROCESS_STAGE.L1_TRANSFORMATION in gl_processStage):
    try:  
      objGenHelper = gen_genericHelper()
      gl_enableTransformaionLog = True  
      transformationStatusLog.createLogFile(PROCESS_ID.L1_TRANSFORMATION)
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,\
                                                PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION)  
      lstOfPrepareStatus = ERPOrchestration.prepareFilesForTransformation()
      lstOfStatus = list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.FAILED, lstOfPrepareStatus))
      if(len(lstOfStatus) == 0):    
        gl_dfExecutionOrder = lstOfPrepareStatus[0][2]
        maxParallelism = MAXPARALLELISM.CPU_COUNT_PER_WORKER.value        
      else:    
          print(lstOfStatus)
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.L1_TRANSFORMATION)
          raise Exception ("L1 orchestration failed, please check the log details." )
        
    except Exception as err:
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.L1_TRANSFORMATION)
      raise err

# COMMAND ----------

if(PROCESS_STAGE.L1_TRANSFORMATION in gl_processStage):
    try:
       lstOfTransformationProcs = list()  
       [lstOfTransformationProcs.append([h.objectName,h.hierarchy,h.parameters]) \
       for h in gl_dfExecutionOrder.select(col("objectName"),\
                  col("hierarchy"),col("parameters")).\
                  filter((col("hierarchy")>0)).\
                  orderBy(col("hierarchy")).rdd.collect()]
    
       executionStartTime = datetime.now()
       transformationProcs = itertools.groupby(lstOfTransformationProcs, lambda x : x[1])
    
       for hierarchy, procs in transformationProcs:  
          print("Executing routines at hierarchy {0} on {1}".format(str(hierarchy), str(datetime.now())))
          lstOfClassesAndMethods = list()
          [lstOfClassesAndMethods.append([proc[0],proc[2]]) \
           for proc in list(itertools.filterfalse(lambda routine : routine[1] != hierarchy, lstOfTransformationProcs))]  
    
          lstOfTransformationStatus = ERPOrchestration.submitTransformation(lstOfClassesAndMethods,maxParallelism)
          executionStatus = list(itertools.filterfalse(lambda status : status[0] == LOG_EXECUTION_STATUS.SUCCESS,lstOfTransformationStatus))
          if(len(executionStatus) != 0):
            print(executionStatus)        
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.L1_TRANSFORMATION)
            raise Exception ("Transformation failed.")
       print("Execution completed on {0}".format(str(datetime.now())))
       print("Execution time: " + str(datetime.now() - executionStartTime))
      
    except Exception as err:
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.L1_TRANSFORMATION)
      print(err)
      raise  
    finally:  
      dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                               gl_executionLogResultPath + "ExecutionLogDetails.csv")

      if (dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).count() != 0):
          print(dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).select(col("comments")).first()[0])
          raise Exception ("Transformation step failed, please check status file for more details.")

# COMMAND ----------

# MAGIC %md ##### Transformation validations

# COMMAND ----------

if(PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION in gl_processStage):
    try:  
      gl_maxParallelism = MAXPARALLELISM.DEFAULT.value 
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.L1_TRANSFORMATION)  
      objPrepareValidation = ERPOrchestration.prepareFilesForPostTransformation() 
      if(objPrepareValidation[0] == LOG_EXECUTION_STATUS.FAILED):
          print(objPrepareValidation[1])
          raise Exception ("Post-transformation validation preparation failed.")
    except Exception as err:
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION)
      raise err

# COMMAND ----------

if(PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION in gl_processStage):
    try:    
        lstOfValidationStatus = ERPOrchestration.submitPostTransformationValidation(objPrepareValidation[2])
        executionStatus = list(itertools.filterfalse(lambda status : status[0] != LOG_EXECUTION_STATUS.FAILED,lstOfValidationStatus))
        if(len(executionStatus) != 0):
            print(executionStatus)
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION)
            raise Exception ("Transformation validations failed, please check the validation logs for more details.")
        else:
            transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION)
            print("Transformaiton validation completed successfully.")    
    except Exception as err:
      transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.SUCCESS,PROCESS_STAGE.L1_TRANSFORMATION)  
      raise err
    finally:
        dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                               gl_executionLogResultPath + "ExecutionLogDetails.csv")
        
        if (dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).count() != 0):
          print(dfExecutionLog.filter(col("status") == LOG_EXECUTION_STATUS.FAILED.value).select(col("comments")).first()[0])
          raise Exception ("Transformation step failed, please check status file for more details.")

# COMMAND ----------

transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.COMPLETED,None)

# COMMAND ----------

gl_processEndTime = datetime.now()
print("Execution end time: " +  str(gl_processEndTime))
print("Execution time: " + str((gl_processEndTime - gl_processStartTime)))

# COMMAND ----------

dbutils.notebook.exit("Execution completed successfully")

