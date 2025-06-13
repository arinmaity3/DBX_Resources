# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
def JEBF_bifurcation():  
    try:

      objGenHelper = gen_genericHelper()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      
      JEBF_BifurcationInput_prepare()
      JEBF_31_Zero_Amount_bifurcate()
      updateInputDeltaFile()
      JEBF_01_O_O_bifurcate()
      JEBF_02_O_M_bifurcate()
      JEBF_03_M_O_bifurcate()
      JEBF_32_AdjMatch_bifurcate(ruleSequence=1,lsPatterns=[0,10,11,14,15,16,4,5,6,7,8,9])
      JEBF_UnambiguousResidual_05_MM_O_M_06_MM_M_O_bifurcate(ruleSequence=1,lsPatterns=[12,13])
      updateInputDeltaFile()
      JEBF_NonAdjacentExactMatches_04_MM_O_O_bifurcate(ruleSequence=2,lsPatterns=[0,10,11,14,15,16,4,5,6,7,8,9])
      updateInputDeltaFile()
      JEBF_LargeTransaction_prepare(maxLineCount=gl_maxLineCount)
      JEBF_LoopBased_bifurcate(ruleSequence=5,lsPatterns=[0,10])
      updateInputDeltaFile()
      JEBF_UnambiguousResidual_05_MM_O_M_06_MM_M_O_bifurcate(ruleSequence=6,lsPatterns=[0,10])
      updateInputDeltaFile()
      aggregateInputAndUpdateLink()
      JEBF_Aggr_08_O_O_bifurcate([0,10])
      updateInputDeltaFile(aggr=1)
      JEBF_Aggr_09_O_M_bifurcate([0,10])
      updateInputDeltaFile(aggr=1)
      JEBF_Aggr_10_M_O_bifurcate([0,10])
      updateInputDeltaFile(aggr=1)
      JEBF_Aggr_LoopBased_bifurcate(ruleSequence=10,lsPatterns=[0,10])
      updateInputDeltaFile(aggr=1)
      JEBF_AggNonAdjacentExactMatches_11_Agg_M_M_O_O_bifurcate(ruleSequence=11,lsPatterns=[0,10])
      updateInputDeltaFile(aggr=1)
      JEBF_AggUnambiguousResidual_12_Agg_M_M_O_M_13_Agg_M_M_M_OO_bifurcate(ruleSequence=12,lsPatterns=[0,10])
      updateInputDeltaFile(aggr=1)
      executionStatus = "JEBF bifurcation executed successfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

