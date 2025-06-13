# Databricks notebook source
from pyspark.sql.functions import concat
import sys
import traceback

def fin_ORA_L0_STG_ClientBusinessStructure_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    global fin_L0_STG_ClientBusinessStructure

    bal_seg = when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT1'),col('GCC.SEGMENT1'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT2'),col('GCC.SEGMENT2'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT3'),col('GCC.SEGMENT3'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT4'),col('GCC.SEGMENT4'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT5'),col('GCC.SEGMENT5'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT6'),col('GCC.SEGMENT6'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT7'),col('GCC.SEGMENT7'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT8'),col('GCC.SEGMENT8'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT9'),col('GCC.SEGMENT9'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT10'),col('GCC.SEGMENT10'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT11'),col('GCC.SEGMENT11'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT12'),col('GCC.SEGMENT12'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT13'),col('GCC.SEGMENT13'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT14'),col('GCC.SEGMENT14'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT15'),col('GCC.SEGMENT15'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT16'),col('GCC.SEGMENT16'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT17'),col('GCC.SEGMENT17'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT18'),col('GCC.SEGMENT18'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT19'),col('GCC.SEGMENT19'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT20'),col('GCC.SEGMENT20'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT21'),col('GCC.SEGMENT21'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT22'),col('GCC.SEGMENT22'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT23'),col('GCC.SEGMENT23'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT24'),col('GCC.SEGMENT24'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT25'),col('GCC.SEGMENT25'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT26'),col('GCC.SEGMENT26'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT27'),col('GCC.SEGMENT27'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT28'),col('GCC.SEGMENT28'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT29'),col('GCC.SEGMENT29'))\
                    .when(col('gl.BAL_SEG_COLUMN_NAME') == lit('SEGMENT30'),col('GCC.SEGMENT30'))\
                    .otherwise(None)
              


    CTE = erp_GL_LEDGERS.alias('gl')\
                          .join(erp_GL_CODE_COMBINATIONS.alias('gcc'),(col("gl.CHART_OF_ACCOUNTS_ID") == col("gcc.CHART_OF_ACCOUNTS_ID"))\
                                                                    & (col("gcc.SUMMARY_FLAG") == lit("N")),how="inner")\
                          .join(knw_LK_CD_ReportingSetup.alias('lcr'),(col("gl.ledger_Id") == col("lcr.ledgerid")),how="inner")\
                          .select(col('gl.LEDGER_ID'),col('gl.NAME'),col('gl.CHART_OF_ACCOUNTS_ID'),col('gl.CURRENCY_CODE'),\
                          lit(bal_seg).alias('bal_seg'),col('gl.BAL_SEG_VALUE_SET_ID'))


    df_fin_L0_STG_ClientBusinessStructure = CTE.alias('t')\
                                        .join(erp_GL_LEDGER_NORM_SEG_VALS.alias('glnsv'),(col("t.LEDGER_ID") == col("glnsv.LEDGER_ID"))\
                                                                & (col("t.bal_seg") == col('glnsv.SEGMENT_VALUE')),how="left")\
                                        .join(erp_XLE_ENTITY_PROFILES.alias('he'),(col("glnsv.LEGAL_ENTITY_ID") == col("he.LEGAL_ENTITY_ID")),how="left")\
                                        .join(erp_FND_FLEX_VALUES_VL.alias('ffv'),(col("t.BAL_SEG_VALUE_SET_ID") == col("ffv.FLEX_VALUE_SET_ID"))\
                                                                & (col("t.bal_seg") == col('ffv.FLEX_VALUE')),how="inner")\
                                        .join(knw_LK_CD_ReportingSetup.alias('lcr'),(col("lcr.ledgerid") == col("t.ledger_id"))\
                                                                & (col("lcr.balancingsegment") == col('t.bal_seg')),how="inner")\
                                        .select(col('t.LEDGER_ID'),col('t.NAME').alias('ledger_Name'),concat(col('t.LEDGER_ID'),lit('-'),col('t.bal_seg')).alias('kaap_Company_Code'),\
                                               when(col('glnsv.LEGAL_ENTITY_ID').isNull(),lit(0))\
                                               .otherwise(col('glnsv.LEGAL_ENTITY_ID')).alias('legal_Entity_Id'),\
                                               col('t.bal_seg').alias('balancing_Segment'),col('ffv.DESCRIPTION').alias('description'),col('t.CHART_OF_ACCOUNTS_ID'),\
                                               when(col('he.NAME').isNull(),lit('#NA#'))\
                                               .otherwise(col('he.NAME')).alias('legal_Entity_Name'),col('T.CURRENCY_CODE').alias('ledger_Currency'),\
                                              lit(None).alias('country'),lit(None).alias('city'),lit('').alias('countrycode')).distinct()

    fin_L0_STG_ClientBusinessStructure = objDataTransformation.gen_convertToCDMandCache \
        (df_fin_L0_STG_ClientBusinessStructure,'fin','L0_STG_ClientBusinessStructure',targetPath=gl_CDMLayer1Path)

    
    
    executionStatus = "fin_L0_STG_ClientBusinessStructure populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]											
