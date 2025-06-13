# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType
import sys
import traceback
def fin_L2_DIM_DocumentDetailCommentComplete_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    
    dwh_vw_DIM_DocumentDetail_Comment_Complete = dwh_vw_DIM_DocumentDetail_CommentLink.alias('CL') \
        .join(dwh_vw_DIM_DocumentDetail_DocumentDetail.alias('DD') \
              ,col('CL.documentDetailID').eqNullSafe(col('DD.documentDetailID')),'left') \
        .join(dwh_vw_DIM_DocumentDetail_DocumentDescription.alias('DoD') \
              ,col('CL.documentDescriptionID').eqNullSafe(col('DoD.documentDescriptionID')),'left') \
        .join(dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription.alias('DLD') \
              ,col('CL.documentLineItemDescriptionID').eqNullSafe(col('DLD.documentLineItemDescriptionID')),'left') \
        .join(dwh_vw_DIM_DocumentDetail_DocumentCustomText.alias('CT') \
              ,col('CL.documentCustomTextID').eqNullSafe(col('CT.documentCustomTextID')),'left') \
        .select(col('CL.customLinkID').alias('customLinkID') \
                ,col('DD.assignmentNumber').alias('assignmentNumber') \
                ,col('DD.clearingDocumentNumber').alias('clearingDocumentNumber') \
                ,col('DD.customDate').alias('customDate') \
                ,when(col('DoD.documentHeaderDescription').isNull(),lit('')) \
                   .otherwise(col('DoD.documentHeaderDescription')).alias('documentHeaderDescription') \
                ,when(col('DLD.documentLineDescription').isNull(),lit('')) \
                   .otherwise(col('DLD.documentLineDescription')).alias('documentLineDescription') \
                ,col('CT.customText').alias('customText') \
                ,col('CT.customText02').alias('customText02') \
                ,col('CT.customText03').alias('customText03') \
                ,col('CT.customText04').alias('customText04') \
                ,col('CT.customText05').alias('customText05') \
                ,col('CT.customText06').alias('customText06') \
                ,col('CT.customText07').alias('customText07') \
                ,col('CT.customText08').alias('customText08') \
                ,col('CT.customText09').alias('customText09') \
                ,col('CT.customText10').alias('customText10') \
                ,col('CT.customText11').alias('customText11') \
                ,col('CT.customText12').alias('customText12') \
                ,col('CT.customText13').alias('customText13') \
                ,col('CT.customText14').alias('customText14') \
                ,col('CT.customText15').alias('customText15') \
                ,col('CT.customText16').alias('customText16') \
                ,col('CT.customText17').alias('customText17') \
                ,col('CT.customText18').alias('customText18') \
                ,col('CT.customText19').alias('customText19') \
                ,col('CT.customText20').alias('customText20') \
                ,col('CT.customText21').alias('customText21') \
                ,col('CT.customText22').alias('customText22') \
                ,col('CT.customText23').alias('customText23') \
                ,col('CT.customText24').alias('customText24') \
                ,col('CT.customText25').alias('customText25') \
                ,col('CT.customText26').alias('customText26') \
                ,col('CT.customText27').alias('customText27') \
                ,col('CT.customText28').alias('customText28') \
                ,col('CT.customText29').alias('customText29') \
                ,col('CT.customText30').alias('customText30') \
                )

    dwh_vw_DIM_DocumentDetail_Comment_Complete = objDataTransformation.gen_convertToCDMStructure_generate \
        (dwh_vw_DIM_DocumentDetail_Comment_Complete,'dwh','vw_DIM_DocumentDetail_Comment_Complete',isIncludeAnalysisID = True, isSqlFormat = True)[0]

    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_Comment_Complete,gl_CDMLayer2Path \
        + "fin_L2_DIM_DocumentDetail_Comment_Complete.parquet" ) 

    executionStatus = "fin_L2_DIM_DocumentDetail_Comment_Complete populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


