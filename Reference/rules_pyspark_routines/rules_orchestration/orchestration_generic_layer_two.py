# Databricks notebook source
# MAGIC %md #### Layer two transformation

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

pip install azure-identity

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")
from datetime import datetime,timedelta
from delta import *
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import itertools

# COMMAND ----------

gl_processStartTime = datetime.now()
print("Execution start time: " +  str(gl_processStartTime))

# COMMAND ----------

dbutils.widgets.text("ContainerName", "")
dbutils.widgets.text("SubDirectory", "")
dbutils.widgets.text("ExecutionGroupID", "")
dbutils.widgets.text("StorageAccount", "")

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
objGenHelper = gen_genericHelper() 
objDataTransformation = gen_dataTransformation()
if (gl_ExecutionMode == EXECUTION_MODE.DEBUGGING):
    gl_enableTransformaionLog = False
else:
    gl_enableTransformaionLog = True

try:
    
  dbutils.fs.rm(gl_executionLogResultPathL2 + 'ExecutionLogDetails.csv')    
  transformationStatusLog.initializeLogs(storageAccountName)       
  transformationStatusLog.createLogFile(PROCESS_ID.L2_TRANSFORMATION)
  
  runDirectory = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  objGenHelper.miscellaneousLogWrite(gl_executionLogResultPathL2, "Execution directory: " + runDirectory)
  
  ERPOrchestration.allPreRequisiteMetadata_load(PROCESS_ID.L2_TRANSFORMATION)
  gl_ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL','ERP_SYSTEM_ID'))
  gl_lstOfScopedAnalytics = (objGenHelper.gen_lk_cd_parameter_get('GLOBAL','SCOPED_ANALYTICS')).split(",")
  GenOrchestration.prepareFilesForL2Transformation()    
  if 'fin_L1_TD_Journal' in globals():
      gl_countJET=fin_L1_TD_Journal.count()
except Exception as err:    
    raise err

# COMMAND ----------

# MAGIC %md ##### Compile all routines

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_two_generic/compile/fin_compile

# COMMAND ----------

# MAGIC  %run ../kpmg_rules_layer_two_generic/compile/gen_compile

# COMMAND ----------

# MAGIC  %run ../kpmg_rules_layer_two_generic/compile/knw_compile

# COMMAND ----------

# MAGIC  %run ../kpmg_rules_layer_two_generic/compile/ptp_compile

# COMMAND ----------

# MAGIC  %run ../kpmg_rules_layer_two_generic/compile/otc_compile

# COMMAND ----------

# MAGIC  %run ../kpmg_rules_layer_two_generic/compile/JEBF_compile

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

# MAGIC %md ##### Layer 2 transformation

# COMMAND ----------

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
      
      lstOfTransformationStatus = ERPOrchestration.submitTransformation(lstOfClassesAndMethods,gl_maxParallelism)
      executionStatus = list(itertools.filterfalse(lambda status : status[0] == LOG_EXECUTION_STATUS.SUCCESS,lstOfTransformationStatus))
      if(len(executionStatus) != 0):
        print(executionStatus)              
        raise Exception ("Transformation failed.")

    print("Execution completed on {0}".format(str(datetime.now())))
    print("Execution time: " + str(datetime.now() - executionStartTime))
except Exception as err:
    print(err)
    raise
finally:
    dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                               gl_executionLogResultPathL2 + "ExecutionLogDetails.csv")

# COMMAND ----------

for key,value in gl_executionLog.items():
  if(value["status"] != LOG_EXECUTION_STATUS.SUCCESS.name):    
    raise Exception ("Initialization/workspace preparation/L2 transformation got failed, please check the log for more details")
    break

# COMMAND ----------

gl_processEndTime = datetime.now()
print("Execution end time: " +  str(gl_processEndTime))
print("Execution time: " + str((gl_processEndTime - gl_processStartTime)))

# COMMAND ----------

dbutils.notebook.exit("L2 transformation completed")
