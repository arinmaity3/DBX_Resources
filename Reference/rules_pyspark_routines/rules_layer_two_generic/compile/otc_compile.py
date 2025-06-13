# Databricks notebook source
# MAGIC %run ../otc/otc_L1_STG_InterCompanyCustomer_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_L2_DIM_Customer_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_L2_DIM_ClearingTypeChain_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_L2_STG_RevenueSLTreeToClearing_populate
# MAGIC
# MAGIC
# MAGIC
