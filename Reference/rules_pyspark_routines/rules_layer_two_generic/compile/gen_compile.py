# Databricks notebook source
# MAGIC %run ../gen/gen_attributeCollectionSchema_initialize

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesGLBalance_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesJournal_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesKnowledge_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_dimensionAttributesPlaceHolderValues_prepare

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_AnalysisDetail_populate

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

# MAGIC %run ../gen/gen_dimensionAttributesKnowledge_prepare

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

# MAGIC %run ../gen/gen_placeHolderFiles_G0001_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L2_DIM_DocumentType_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L1_STG_ClearingBox_07_InterpretPreparation_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L1_STG_ClearingBox_10_InterpretFinalization_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_L1_STG_ClearingBox_12_InitialRevenue_populate
