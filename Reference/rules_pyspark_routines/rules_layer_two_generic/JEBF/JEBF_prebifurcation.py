# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
def JEBF_prebifurcation():  
    try:

      objGenHelper = gen_genericHelper()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      JEBF_PrebifurcationInput_prepare()
      JEBF_BlockBasedPatterntag_perform(BlockType=0)
      JEBF_BlockBuilding_perform()
      JEBF_BlockBasedPatterntag_perform(BlockType=1)
      JEBF_ShiftedBlockBuilding_perform(shiftedBlockCall=1)
      JEBF_BlockBasedPatterntag_perform(BlockType=2)
      JEBF_ShiftedBlockBuilding_perform(shiftedBlockCall=2)
      JEBF_BlockBasedPatterntag_perform(BlockType=3)
      
      
      executionStatus = "JEBF prebifurcation executed successfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
