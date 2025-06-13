# Databricks notebook source
# MAGIC %run ../kpmg_rules_common_functions/parameters/gen_globalVariables
# MAGIC           

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/parameters/gen_constants

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_commonFunctions

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_transformationExecutionOrder

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_mountADLSContainerToWorkspace

# COMMAND ----------

# MAGIC  
# MAGIC %run ../kpmg_rules_common_functions/helper/gen_commonDataModel

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/log/LOG_EXECUTION_STATUS

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/log/log_init

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/log/executionLog

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/log/transformationStatusLog 

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/metadata/ddic

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/metadata/knowledge

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/parameters/gen_inputParams

# COMMAND ----------

# MAGIC %run ../kpmg_rules_orchestration/erp_orchestration_helper

# COMMAND ----------

# MAGIC %run ../kpmg_rules_orchestration/gen_orchestration_helper

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_importCustomFunctions

# COMMAND ----------

gen_mountADLSContainer.gen_mountContainerToWorkspace_perform(gl_analysisID, storageAccountName, gl_MountPoint)

dbutils.fs.mkdirs(gl_convertedRawFilesPath)
dbutils.fs.mkdirs(gl_CDMLayer2Path)
dbutils.fs.mkdirs(gl_CDMLayer1Path)
dbutils.fs.mkdirs(gl_convertedRawFilesPathERP)

