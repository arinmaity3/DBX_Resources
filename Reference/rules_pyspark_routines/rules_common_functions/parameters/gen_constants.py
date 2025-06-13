# Databricks notebook source
from enum import Enum
import os
import builtins as p
global gl_analysisID
global gl_analysisPhaseID
global gl_executionGroupID



class PROCESS_ID(Enum):   
    IMPORT = 12
    IMPORT_VALIDATION = 4
    CLEANSING = 9
    DPS = 6
    PRE_TRANSFORMATION = 14
    PRE_TRANSFORMATION_VALIDATION = 7
    TRANSFORMATION_VALIDATION = 5
    L1_TRANSFORMATION = 10
    L2_TRANSFORMATION = 11


class FILES(Enum):    
    KPMG_ERP_STAGING_RECORD_COUNT = "KPMG_ERP_STAGING_RECORD_COUNT.delta"
    TRANSFORMATION_ROUTINE_LIST_FILE = "transformationRoutines.csv"
    KPMG_PROCESS_STAGE_STATUS_FILE = "ProcessStageStatus.delta"
    APP_PROCESS_STAGE_STATUS_FILE = "currentProcessingStage.csv"
    EXECUTION_STATUS_SUMMARY_FILE = "ExecutionSummary.csv"    
    ACCOUNT_MAPPING_ENABLE_STATUS_FILE = "app_LA_EnableAccountMapping.csv"    
    ERP_IMPORT_INPUT_PARAM_FILE = "ERPImportInputParams.csv"
    
    
class GLOBAL_PARAM(Enum):
    ANALYSIS_ID = gl_analysisID
    ANALYSIS_PHASE_ID = gl_analysisPhaseID
    EXECUTIONGROUP_ID = gl_executionGroupID
    STORAGE_ACCOUNT_NAME = storageAccountName

class EXECUTION_MODE(Enum):
    STANDARD = 1
    DEBUGGING = 2

class PROCESS_STAGE(Enum):
    UPLOAD = 17
    IMPORT = 18
    CLEANSING = 19
    L1_TRANSFORMATION = 20
    POST_TRANSFORMATION_VALIDATION = 21
    DPS = 24
    PRE_TRANSFORMATION_VALIDATION = 25
    L2_TRANSFORMATION = 26

class MAXPARALLELISM(Enum):
    DEFAULT              = sc.defaultParallelism
    CPU_COUNT_PER_WORKER = p.min(32, os.cpu_count())
    WORKER_COUNT         = len([executor.host() for executor in spark._jsc.sc().statusTracker().getExecutorInfos() ]) -1
   
 
class PROCESSING_SCENARIO(Enum):
  FULL_STAGE = 1
  COMPACT = 2
  @classmethod
  def get(cls):
    pStage1 = list()
    pStage2 = list()
    [pStage1.append(i.value) for i in gl_processStage]
    [pStage2.append(i.value) for i in PROCESS_STAGE]
    if(set(pStage1) == set(pStage2)):      
      return PROCESSING_SCENARIO.FULL_STAGE
    else:
      return PROCESSING_SCENARIO.COMPACT

class DML_FLAG(Enum):
    INSERT = 1
    UPDATE = 2
    DELETE = 3
    MERGE = 4

class ORCHESTRATION_PHASE(Enum):
  WB_LAYER_ZERO_LAYER_ONE_ERP = 1
  DM_LAYER_ZERO_LAYER_ONE = 2


class FILES_PATH(Enum):
    IMPORT_VALIDATION_RESULTS_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/import/"
    IMPORT_STATUS_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/import/"
    CLEANSING_STATUS_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/cleansing/"
    DPS_STATUS_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/decimalpointshifting/"

    PRE_TRANS_STATUS_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/pretransformation/"
    PRE_TRANS_VALIDATION_STATUS_BLOB_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/pretransformationvalidation/"
    PRE_TRANS_VALIDATION_STATUS_PATH = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/pretransformationvalidation/"

    L1_TRANSFORMATION_STATUS_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/transformation/"
    L1_TRANSFORMATION_ROUTINE_LIST_PATH = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/transformation/"
    TRANSFORMATION_VALIDATION_STATUS_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/posttransformationvalidation/"
    TRANSFORMATION_VALIDATION_ROUTINE_LIST_PATH = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/validationresults/posttransformationvalidation/"
    REPORTS_PATH = gl_MountPoint + "/"+ gl_analysisPhaseID + "/cdm-l1/reports/"

    L2_TRANSFORMATION_STATUS_BLOB_PATH = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l1-l2/" + gl_executionGroupID + "/"

gl_processStagewiseOrder ={1: PROCESS_STAGE.UPLOAD,
                    2: PROCESS_STAGE.IMPORT,
                    3: PROCESS_STAGE.CLEANSING,
                    4: PROCESS_STAGE.DPS,
                    5: PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION,
                    6: PROCESS_STAGE.L1_TRANSFORMATION,
                    7: PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION}

class VALIDATION_ID(Enum):
  FILE_ENCODING = 1
  NUMBER_FORMAT = 2
  DATE_FORMAT = 3
  BLANK_DATA = 4
  ACCOUNT_NO_MATCH_GLA_JET = 5
  ACCOUNT_NO_MATCH_GLA_GLAB = 6
  PERIOD_BALANCE_EQUAL_ZERO = 7
  DATA_ALIGN_IN_ANALYSIS_PERIOD = 8
  DUPLICATE_RECORD = 9
  CALC_VS_IMPORTED_BALANCES = 10
  PACKAGE_FAILURE_IMPORT = 11
  PACKAGE_FAILURE_TRANSFORMATION = 12
  COLUMN_DELIMITER = 13
  CY_CB = 14
  CY_OB = 15
  PY_CB = 16
  ZERO_KB =17
  ZERO_ROW_COUNT = 19
  UPLOAD_VS_IMPORT = 31
  DATA_PERSISTENCE = 32
  REMOVE_DUPLICATE = 34
  DPS = 35
  ORGANIZATION_SETUP = 37
  MANDATORY_PARAMS = 38
  SCOPING_LEDGER = 39
  SCOPING_LEDGER_CURRENCY = 40
  SCOPING_LEDGER_CALENDAR = 41
  GL_SOURCES = 43
  GL_CATEGORY = 44
  SCOPING_LEDGER_LEBS = 46
  SCOPING_LEDGER_COA = 47
  ORA_ORGANIZATION_SETUP = 49
  FINANCIAL_PERIOD_PARAMETER = 50
  MANDATORY_TABLE = 51
  GL_MULTIPLE_CHARTOFACCOUNT = 52
  BLANK_DATA_ERP = 54
  MANDATORY_FIELD = 55
  CLASSIC_OR_NEW_GL = 56
  SCOPING_LEDGER_PERIOD_SET_NAME = 60
  TIME_FORMAT = 67
  MAX_DATA_LENGTH = 68
  TRIAL_BALANCE_REPORT = 1001
  PTR_AND_ACCB_REPORT = 1002
  MISSING_CURRENCY = 61
