# Databricks notebook source
# MAGIC %run ../JEBF/helper/bif_init

# COMMAND ----------

# MAGIC %run ../JEBF/prebifurcation/JEBF_PrebifurcationInput_prepare

# COMMAND ----------

# MAGIC %run ../JEBF/prebifurcation/JEBF_BlockBasedPatterntag_perform

# COMMAND ----------

# MAGIC %run ../JEBF/prebifurcation/JEBF_initialize

# COMMAND ----------

# MAGIC %run ../JEBF/prebifurcation/JEBF_ShiftedBlockBuilding_perform

# COMMAND ----------

# MAGIC %run ../JEBF/prebifurcation/JEBF_BlockBuilding_perform

# COMMAND ----------

# MAGIC %run ../JEBF/helper/updateInputDeltaFile

# COMMAND ----------

# MAGIC %run ../JEBF/helper/aggregateInputAndUpdateLink

# COMMAND ----------

# MAGIC %run ../JEBF/helper/lsShiftedBlock_get

# COMMAND ----------

# MAGIC %run ../JEBF/helper/ApplySBB

# COMMAND ----------

# MAGIC %run ../JEBF/helper/lsBlockID_get

# COMMAND ----------

# MAGIC %run ../JEBF/helper/JEBF_BifurcationInput_prepare

# COMMAND ----------

# MAGIC %run ../JEBF/helper/JEBF_LargeTransaction_prepare

# COMMAND ----------

# MAGIC %run ../JEBF/helper/fin_L1_STG_JEBifurcation_01_Combined_prepare

# COMMAND ----------

# MAGIC %run ../JEBF/helper/LoopingStrategy

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_31_Zero_Amount_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_01_O_O_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_02_O_M_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_03_M_O_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_32_AdjMatch_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_NonAdjacentExactMatches_04_MM_O_O_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_AggNonAdjacentExactMatches_11_Agg_M_M_O_O_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_Aggr_08_O_O_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_Aggr_09_O_M_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_Aggr_10_M_O_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_AggUnambiguousResidual_12_Agg_M_M_O_M_13_Agg_M_M_M_OO_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_LoopBased_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_Aggr_LoopBased_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/bifurcation/JEBF_UnambiguousResidual_05_MM_O_M_06_MM_M_O_bifurcate

# COMMAND ----------

# MAGIC %run ../JEBF/helper/addUnbifurcatedOutputToLink

# COMMAND ----------

# MAGIC %run ../JEBF/JEBF_bifurcation

# COMMAND ----------

# MAGIC %run ../JEBF/JEBF_prebifurcation

# COMMAND ----------

print("All bifurcation functions compiled successfully")
