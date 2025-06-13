# Databricks notebook source
import sys
import traceback
from pyspark.sql.window import Window
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F
from delta.tables import *

class app_layerOnePostProcessing_perform():
  def app_importResults_write(self):
    try:
      global fin_L1_MD_GLAccount, fin_L1_TD_Journal,fin_L1_TD_GLBalance,knw_LK_CD_Parameter      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      dfResult = None
     
      print('Writing CDM layer 1 files...')
      logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION)
     
      knw_LK_CD_Parameter = gl_parameterDictionary['knw_LK_CD_Parameter'] 
      InputfileTypes = gl_parameterDictionary["InputParams"].\
                       select(upper(col("fileType"))).\
                       filter(col("isCustomTransform")==gl_isCustomTransform).\
                       distinct().rdd.flatMap(lambda x: x).collect()
               

      if ((fin_L1_TD_GLBalance.first() is None) and (('GLAB' not in InputfileTypes) or ('GLTB' not in InputfileTypes))):
          try:
              
              fin_L1_TD_GLBalance = objGenHelper.\
                                    gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_TD_GLBalance.delta")    
          except Exception as err:
              pass
      elif(('GLAB' in InputfileTypes) or ('GLTB' in InputfileTypes)):
          fileName1 = gl_CDMLayer1Path +"fin_L1_TD_GLBalance.csv"                
          objGenHelper.gen_writeToFile_perfom(fin_L1_TD_GLBalance,fileName1)  
                    
          fin_L1_TD_GLBalance = objDataTransformation.gen_convertToCDMandCache(fin_L1_TD_GLBalance
                                                                  ,schemaName = 'fin'
                                                                  ,tableName = 'L1_TD_GLBalance'
                                                                  ,targetPath = gl_CDMLayer1Path
                                                                  ,isOverWriteSchema = True
                                                                  )  
              
      if ((fin_L1_TD_Journal.first() is None) and ('JET' not in InputfileTypes)):
          try:              
              fin_L1_TD_Journal = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_TD_Journal.delta")    
          except Exception as err:
              pass
      elif('JET' in InputfileTypes):        
        fileName1 = gl_CDMLayer1Path +"fin_L1_TD_Journal.csv"                               
        dfCDM = objDataTransformation.gen_CDMStructure_get('fin','L1_TD_Journal')
        objGenHelper.gen_writeToFile_perfom(dfCDM,fileName1)
        
        keyColumns = fin_L1_TD_Journal.select("fiscalYear","documentNumber","financialPeriod")\
            .filter((col('fiscalYear').isNull())|(col('documentNumber').isNull())|(col('financialPeriod').isNull())).first()
        
        ## If any of the key column "fiscalYear","documentNumber","financialPeriod" is null in journal, then after this 
        ## gen_journalExtendedKeys_prepare those row is getting removed, so if any keycolumn is null, this function is not called
        ## and so required feild validation will fail
        if keyColumns is None:
            fin_L1_TD_Journal = gen_journalExtendedKeys_prepare() ## under kpmg_rules_generic_methods.gen

        fin_L1_TD_Journal = objDataTransformation.gen_convertToCDMandCache(fin_L1_TD_Journal
                                                                  ,schemaName = 'fin'
                                                                  ,tableName = 'L1_TD_Journal'
                                                                  ,targetPath = gl_CDMLayer1Path
                                                                  ,isOverWriteSchema = True
                                                                  )                
        
      if ((fin_L1_MD_GLAccount.first() is None) and (('GLA' not in InputfileTypes) or ('GLTB' not in InputfileTypes))):
          try:
              fin_L1_MD_GLAccount = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_MD_GLAccount.delta")
          except Exception as err:
              pass
      elif(('GLA' in InputfileTypes) or ('GLTB' in InputfileTypes)):
          fileName1 = gl_CDMLayer1Path + "fin_L1_MD_GLAccount.csv"             
          objGenHelper.gen_writeToFile_perfom(fin_L1_MD_GLAccount,fileName1) 
          fin_L1_MD_GLAccount = objDataTransformation.gen_convertToCDMandCache(fin_L1_MD_GLAccount
                                                                  ,schemaName = 'fin'
                                                                  ,tableName = 'L1_MD_GLAccount'
                                                                  ,targetPath = gl_CDMLayer1Path
                                                                  ,isOverWriteSchema = True
                                                                  ) 
      print('CDM Layer 1 files writing completed...')
      executionStatus = "Layer one post processing completed."
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    except Exception as e: 
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
        executionLog.add(executionStatusID,logID,executionStatus)
        raise
    finally:
        print(executionStatus)
