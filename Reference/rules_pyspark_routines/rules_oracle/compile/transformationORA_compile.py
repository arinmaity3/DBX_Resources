# Databricks notebook source
# MAGIC %run ../fin/fin_ORA_L0_STG_ClientBusinessStructure_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_ORA_L1_MD_GLAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_ORA_L1_TD_GLBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_ORA_L1_TD_Journal_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_ORA_ClientCCIDAccount_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_ORA_L1_MD_DocType_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_ORA_L1_MD_Organization_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_ORA_L1_MD_Period_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_ORA_L1_MD_Transaction_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_ORA_L1_MD_User_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_ORA_L1_MD_UserTimeZone_populate
# MAGIC  
