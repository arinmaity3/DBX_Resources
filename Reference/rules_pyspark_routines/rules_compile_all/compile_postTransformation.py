# Databricks notebook source
# MAGIC %run ../kpmg_da_generic_validations/transformation/dataIntegrity_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/GLABPeriodCheckCYCB_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/GLABPeriodCheckCYOB_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/GLABPeriodCheckPYCB_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/GLBalanceWithMissingGLAccountMasterData_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/GLJEAccountReconciliation_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/GLPeriodwiseBalanceReconcile_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/JEMinimumNumberOfPostingPerPeriod_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/JEPeriodwiseBalanceReconcile_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/JETransactionWithMissingGLAccountMasterData_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/requiredField_validate

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/accountBalanceAndPeriodTransactionReport

# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/transformation/trialBalanceReport

# COMMAND ----------



# COMMAND ----------

# MAGIC %run ../kpmg_da_generic_validations/import/maxDataLength_validate

# COMMAND ----------


