# Databricks notebook source
# MAGIC %run ../kpmg_rules_layer_one_generic/gen/gen_rawFiles_import

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/gen/gen_usf_rawFileEncoding_convert

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/app/app_layerZeroPostProcessing_perform

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/app/app_transformDataToStandardFormat 

# COMMAND ----------

# MAGIC %run ../kpmg_rules_layer_one_generic/app/app_layerZeroProcessingStatus
