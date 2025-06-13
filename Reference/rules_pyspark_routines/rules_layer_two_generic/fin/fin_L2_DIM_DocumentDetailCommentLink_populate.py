# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim
from dateutil.parser import parse

class fin_L2_DIM_DocumentDetailCommentLink_populate():
    @staticmethod

    def dwhvwDIMDocumentDetailCommentLink_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_CommentLink
            global dwh_vw_DIM_DocumentDetail_CommentLink
            
            fin_L2_DIM_DocumentDetail_CommentLink = fin_L2_FACT_Journal_Extended.alias('jrnl') \
                                      .select(col('jrnl.DocumentDetailID').alias('documentDetailID')\
                                      ,col('jrnl.DocumentDescriptionID').alias('documentDescriptionID')\
                                      ,col('jrnl.DocumentLineItemDescriptionID').alias('documentLineItemDescriptionID')\
                                      ,col('jrnl.DocumentCustomTextID').alias('documentCustomTextID')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_CommentLink = fin_L2_DIM_DocumentDetail_CommentLink\
                                .withColumn( "customLinkID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_CommentLink  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_CommentLink,'fin','L2_DIM_DocumentDetail_CommentLink',False)
            
            dwh_vw_DIM_DocumentDetail_CommentLink = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_CommentLink,'dwh','vw_DIM_DocumentDetail_CommentLink',True)[0]
            
            keysAndValues = {'customLinkID':-1,'documentDetailID':-1,'documentDescriptionID':-1,\
                'documentLineItemDescriptionID':-1,'documentCustomTextID':-1}
            
            dwh_vw_DIM_DocumentDetail_CommentLink = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_CommentLink',keysAndValues,dwh_vw_DIM_DocumentDetail_CommentLink)

            dwh_vw_DIM_DocumentDetail_CommentLink  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_CommentLink,'dwh','vw_DIM_DocumentDetail_CommentLink',False)
            
            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_CommentLink,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_CommentLink.parquet" )

            executionStatus = "fin_L2_DIM_DocumentDetail_CommentLink populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dwhvwDIMDocumentDetailDetailLink_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DetailLink
            global dwh_vw_DIM_DocumentDetail_DetailLink
            
            fin_L2_DIM_DocumentDetail_DetailLink = fin_L2_FACT_Journal_Extended.alias('jrnl') \
                                      .select(col('jrnl.DocumentBaseID').alias('documentBaseID')\
                                      ,col('jrnl.DocumentCoreID').alias('documentCoreID')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DetailLink = fin_L2_DIM_DocumentDetail_DetailLink.withColumn( "detailLinkID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_DetailLink  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DetailLink,'fin','L2_DIM_DocumentDetail_DetailLink',False)
            
            dwh_vw_DIM_DocumentDetail_DetailLink = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DetailLink,'dwh','vw_DIM_DocumentDetail_DetailLink',True)[0]
            keysAndValues = {'detailLinkID':-1,'documentBaseID':-1,'documentCoreID':-1}
            dwh_vw_DIM_DocumentDetail_DetailLink = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DetailLink',keysAndValues,dwh_vw_DIM_DocumentDetail_DetailLink)

            dwh_vw_DIM_DocumentDetail_DetailLink  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_DetailLink,'dwh','vw_DIM_DocumentDetail_DetailLink',False)
            
            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_DetailLink,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DetailLink.parquet" )

            executionStatus = "fin_L2_DIM_DocumentDetail_DetailLink populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

