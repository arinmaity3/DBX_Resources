# Databricks notebook source
# MAGIC %run ../fin/fin_SAP_L1_MD_PaymentTerm_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_SAP_L1_STG_AccountConfiguration_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L0_STG_MinimumYear_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_MD_InternationalCommercialterm_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_MD_SalesOrganization_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_STG_MessageStatus_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L0_STG_ProductHierarchy_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_TD_ChangeOrder_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L0_STG_DocumentTypeBackfilling_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_TD_InternationalCommercialtermChangeLog_populate

# COMMAND ----------

# MAGIC %run ../ptp/ptp_SAP_L1_MD_PurchaseOrderSubType_populate

# COMMAND ----------

# MAGIC %run ../ptp/ptp_SAP_L1_TD_PurchaseOrder_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_TD_SalesShipment_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_TD_Billing_populate

# COMMAND ----------

# MAGIC %run ../otc/otc_SAP_L1_TD_SalesOrder_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_SAP_L1_TD_ElectronicBankStatement_populate

# COMMAND ----------

# MAGIC %run ../gen/gen_SAP_L1_STG_ClearingBoxData_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_SAP_L1_TD_AccountReceivable_populate

# COMMAND ----------

# MAGIC %run ../fin/fin_L1_STG_PaymentTerm_populate
