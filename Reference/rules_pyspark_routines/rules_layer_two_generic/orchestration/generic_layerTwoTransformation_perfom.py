# Databricks notebook source
# MAGIC %md ##### Layer two transfroamtion

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

# MAGIC %md ##### Get variables from Azure Data Factory

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
gl_isReProcessing = False

# COMMAND ----------

gl_MountPoint = '/mnt/' + storageAccountName + '-' + gl_analysisID

# COMMAND ----------

# MAGIC %run ../helper/gen_mountADLSContainer

# COMMAND ----------

gen_mountADLSContainer.gen_mountContainerToWorkspace_perform(gl_analysisID,storageAccountName,gl_MountPoint)

# COMMAND ----------

# MAGIC %md ##### Initialize common parameters

# COMMAND ----------

# MAGIC %run ../orchestration/generic_commonParmeters_Initialize

# COMMAND ----------

# MAGIC %run ../orchestration/generic_Initialize

# COMMAND ----------

# MAGIC %run ../helper/genericHelper

# COMMAND ----------

# MAGIC %run ../helper/gen_dataTransformation

# COMMAND ----------

# MAGIC %run ../helper/gen_transformationExecutionOrder

# COMMAND ----------

# MAGIC %run ../parameters/gen_constants

# COMMAND ----------

# MAGIC %run ../parameters/gen_globalVariables

# COMMAND ----------

# MAGIC %run ../log/LOG_EXECUTION_STATUS

# COMMAND ----------

# MAGIC %run ../log/log_init

# COMMAND ----------

# MAGIC %run ../log/executionLog

# COMMAND ----------

# MAGIC %run ../log/transformationStatusLog 

# COMMAND ----------

# MAGIC %md ##### initialization

# COMMAND ----------

try:
  gl_enableTransformaionLog = True  
  objGenHelper = gen_genericHelper()  
  transformationStatusLog.initializeLogging(gl_executionLogBlobFilePath,storageAccountName,gl_processID)
  logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION,phaseName = "Initialize logging")

  executionStatus = "Initialize logging completed."
  executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
except Exception as err:
  raise err

# COMMAND ----------

objInit = generic_Initialize()
objInit.getModulesAndPrepareOrchestration()

# COMMAND ----------

# MAGIC %md ##### Load all the input files

# COMMAND ----------

# MAGIC %run ../orchestration/generic_layerTwoPrerequisites_load

# COMMAND ----------

loadPreRquisites = generic_layerTwoPrerequisites_load()

# COMMAND ----------

for key,value in gl_executionLog.items():
  if(value["status"] != LOG_EXECUTION_STATUS.SUCCESS.name):
    raise Exception ("Initialization/workspace preparation got failed, please check the log for more details")
    break

# COMMAND ----------

# MAGIC %run ../gen/gen_attributeCollectionSchema_initialize

# COMMAND ----------

# MAGIC %run ../gen/gen_placeHolderFiles_G0001_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_JEDocumentClassification_01_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_JEDocumentClassification_02_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_JEDocumentClassification_03_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_JEDocumentClassification_04_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_STG_GLAccountCategoryMappingHierarchy_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_STG_GLBalancesByKnowledgeAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_STG_InterCompanyGLAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_BenfordAnalysis_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_FinancialPeriod_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_GLAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEAdditionalAttribute_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEDocumentClassification_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEPatternFootprint_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEWithCriticalComment_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_GLBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_Journal_01_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_Journal_02_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_Journal_03_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_Journal_04_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_Journal_05_prepare

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_Journal_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLAnnualizedBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLPeriodAccountBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLPPYCumulativeBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEDocumentClassification_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEDocumentDetail_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEDocumentDetailExtended_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_JEBifurcationBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_GLBifurcation_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_GLAccountCombination_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_KnowledgeAccountCombinationExpectation_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_BifurcationDataType_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailDocumentBase_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_JournalExtended_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailCommentLink_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_JournalExtendedLink_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JournalRaw_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesGLBalance_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesJournal_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesPlaceHolderValues_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesKnowledge_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_Bracket_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_BusinessTransaction_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_CalendarDate_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_Currency_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_DocType_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_Organization_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_Parameter_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_PostingKey_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_Product_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_ReferenceTransaction_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_Time_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_Transaction_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_User_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_UserTimeZone_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L3_STG_Product_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_AnalysisDetail_populate

# COMMAND ----------

# MAGIC %run ../knw/knw_LK_GD_DAFlagKnowledgeAccountMappingDetail_populate

# COMMAND ----------

# MAGIC %run ../knw/knw_L2_DIM_BifurcationRuleType_populate

# COMMAND ----------

# MAGIC %run ../knw/knw_L2_DIM_BifurcationPatternSource_populate 

# COMMAND ----------

# MAGIC %run ../knw/knw_LK_GD_MaterialityKnowledge_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_L1_STG_InterCompanyCustomer_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_L2_DIM_Customer_populate

# COMMAND ----------

# MAGIC %run ../ptp/ptp_L1_STG_InterCompanyVendor_populate

# COMMAND ----------

# MAGIC %run ../ptp/ptp_L2_DIM_Vendor_populate 

# COMMAND ----------

# MAGIC %run ../JEBF/JEBF_functions

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_AllDimAttributes_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_STG_BifurcationOffset_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_Base_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_LineItem_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_Link_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEBifurcation_Raw_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEBifurcation_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailComplete_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailCommentComplete_populate

# COMMAND ----------

 

def submitTransformation(lstOfClassesAndMethods):      
  try:            
    objectName  = lstOfClassesAndMethods[0]   
    parameters  = lstOfClassesAndMethods[1]
    
    if(parameters !=  ""):
      parameters =  eval(parameters)      
    
    #if it is class
    if('.' in objectName):          
      objClass = globals()[objectName.split('.')[0]]()
      objMethod = objectName.split('.')[1]
      if (parameters != ""):
        result = getattr(objClass,objMethod)(**parameters)
      else:
        result = getattr(objClass,objMethod)()
    else:      
      if (parameters!= ""):
        result = globals()[objectName](**parameters)    
      else:
        result = globals()[objectName]()
    return result
  except Exception as e:
    print(e)
    raise 

# COMMAND ----------

def prepareTransformation(lstOfClassesAndMethods):
  with ThreadPoolExecutor(max_workers = gl_maxParallelism) as executor:
    lstOfTransformationStatus = list(executor.map(submitTransformation,lstOfClassesAndMethods))
    return lstOfTransformationStatus

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import itertools


lstOfTransformationProcs = list()
[lstOfTransformationProcs.append([h.objectName,h.hierarchy,h.parameters]) \
 for h in gl_dfExecutionOrder.select(col("objectName"),\
            col("hierarchy"),col("parameters")).\
            filter(col("hierarchy")>0).\
            orderBy(col("hierarchy")).rdd.collect()]

executionStartTime = datetime.now()
transformationProcs = itertools.groupby(lstOfTransformationProcs, lambda x : x[1])

for hierarchy, procs in transformationProcs:  
  print("Executing routines at hierarchy {0} on {1}".format(str(hierarchy), str(datetime.now())))
  lstOfClassesAndMethods = list()
  [lstOfClassesAndMethods.append([proc[0],proc[2]]) \
   for proc in list(itertools.filterfalse(lambda routine : routine[1] != hierarchy, lstOfTransformationProcs))]  
  lstOfTransformationStatus = prepareTransformation(lstOfClassesAndMethods)   
  executionStatus = list(itertools.filterfalse(lambda status : status[0] == LOG_EXECUTION_STATUS.SUCCESS,lstOfTransformationStatus))
  if(len(executionStatus) != 0):
    print(executionStatus)
    break

print("Execution completed on {0}".format(str(datetime.now())))
print("Execution time: " + str(datetime.now() - executionStartTime))

# COMMAND ----------

for key,value in gl_executionLog.items():
  if(value["status"] != LOG_EXECUTION_STATUS.SUCCESS.name):
    objGenHelper.gen_executionLog_write(logFilePath = gl_executionLogResultPath + "ExecutionLogDetails-" + gl_executionGroupID + ".csv")
    raise Exception ("Initialization/workspace preparation/L2 transformation got failed, please check the log for more details")
    break

# COMMAND ----------

objGenHelper.gen_executionLog_write(logFilePath = gl_executionLogResultPath + "ExecutionLogDetails-" + gl_executionGroupID + ".csv")

# COMMAND ----------


print("Execution end time: " + str(datetime.now()))

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

dbutils.notebook.exit("L2 transformation completed successfully")
