# Databricks notebook source
# MAGIC %run ../kpmg_rules_layer_one_generic/app/app_layerOnePreProcessing_perfom

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/app/app_layerOneTransformation_perform

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/knw/knw_LK_CD_reportingSetup_populate

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/gen/gen_CDMLayer_prepare

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/app/app_layerOnePostProcessing_perform

# COMMAND ----------


