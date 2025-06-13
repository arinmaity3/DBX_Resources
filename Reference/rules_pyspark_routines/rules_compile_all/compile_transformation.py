# Databricks notebook source
# MAGIC %run ../kpmg_da_generic_validations/transformation/L1_STG_PYCYBalance_populate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/L1_STG_GLBalanceAccumulated_populate

# COMMAND ----------

# MAGIC %run ../kpmg_rules_generic_methods/knw/knw_clientReportingLanguage_populate

# COMMAND ----------

# MAGIC %run ../kpmg_rules_generic_methods/knw/knw_LK_GD_DAFlagFiles_populate

# COMMAND ----------

# MAGIC %run ../kpmg_rules_generic_methods/gen/gen_currencyConversion_perform

# COMMAND ----------

# MAGIC %run ../kpmg_rules_generic_methods/gen/gen_journalExtendedKeys_prepare
