# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim
from pyspark.sql.functions import monotonically_increasing_id as mi
from dateutil.parser import parse

def fin_L2_FACT_JournalExtendedLink_populate():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        global fin_L2_FACT_Journal_Extended_Link
        
        fin_L2_FACT_Journal_Extended_Link = fin_L2_FACT_Journal_Extended.alias('je') \
                                      .join(fin_L2_DIM_DocumentDetail_DetailLink.alias('dl'), \
                                      ((col('je.documentBaseID').eqNullSafe(col('dl.documentBaseID'))) \
                                      &(col('je.documentCoreID').eqNullSafe(col('dl.documentCoreID')))),how ='left') \
                                      .join(fin_L2_DIM_DocumentDetail_CommentLink.alias('cl'), \
                                      ((col('je.documentDetailID').eqNullSafe(col('cl.documentDetailID'))) \
                                      &(col('je.documentDescriptionID').eqNullSafe(col('cl.documentDescriptionID'))) \
                                      &(col('je.documentLineItemDescriptionID').eqNullSafe(col('cl.documentLineItemDescriptionID'))) \
                                      &(col('je.documentCustomTextID').eqNullSafe(col('cl.documentCustomTextID')))),how ='left') \
                                      .select(when(col('je.journalSurrogateKey').isNull(),lit(-1)).\
                                      otherwise(col('je.journalSurrogateKey')).alias('journalSurrogateKey'),\
                                      when(col('dl.detailLinkID').isNull(),lit(-1)).\
                                      otherwise(col('dl.detailLinkID')).alias('detailLinkID'),\
                                      when(col('cl.customLinkID').isNull(),lit(-1)).\
                                      otherwise(col('cl.customLinkID')).alias('customLinkID'))
        
        fin_L2_FACT_Journal_Extended_Link  = objDataTransformation.gen_convertToCDMandCache \
            (fin_L2_FACT_Journal_Extended_Link,'fin','L2_FACT_Journal_Extended_Link',False)
        
        executionStatus = "fin_L2_FACT_Journal_Extended_Link populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

