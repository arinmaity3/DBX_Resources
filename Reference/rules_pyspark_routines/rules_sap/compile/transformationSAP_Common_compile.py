# Databricks notebook source
# MAGIC %run ../fin/fin_SAP_L1_MD_GLAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_SAP_L1_TD_GLBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_SAP_L1_TD_Journal_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_SAP_L0_STG_reversalDocument_determine

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_BusinessTransaction_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_DocType_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_Organization_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_Period_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_PostingKey_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_Product_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_ReferenceTransaction_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_Transaction_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_User_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_MD_UserTimezone_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_MD_Customer_populate

# COMMAND ----------

# MAGIC %run ../ptp/ptp_SAP_L1_MD_Vendor_populate
