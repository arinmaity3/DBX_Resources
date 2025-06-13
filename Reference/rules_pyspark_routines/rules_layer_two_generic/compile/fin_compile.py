# Databricks notebook source
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

# MAGIC %run ../fin/fin_L1_STG_Journal_00_Initialize_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_BenfordAnalysis_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_BifurcationDataType_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailCommentComplete_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailCommentLink_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailComplete_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_DocumentDetailDocumentBase_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_FinancialPeriod_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_GLAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_GLAccountCombination_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEAdditionalAttribute_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_AllDimAttributes_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_Base_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_LineItem_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_Link_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEDocumentClassification_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEPatternFootprint_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEWithCriticalComment_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_KnowledgeAccountCombinationExpectation_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_GLBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_GLBifurcation_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_JEBifurcationBalance_populate

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

# MAGIC %run ../fin/fin_L2_FACT_JournalExtended_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_JournalExtendedLink_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_STG_BifurcationOffset_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLAccount_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLAnnualizedBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLPeriodAccountBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_GLPPYCumulativeBalance_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEBifurcation_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEBifurcation_Raw_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEDocumentClassification_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEDocumentDetail_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEDocumentDetailExtended_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JournalRaw_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_FACT_RevenueClearing_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L2_DIM_JEBifurcation_Transaction_Base_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L3_STG_JEBifurcation_Agg_populate
