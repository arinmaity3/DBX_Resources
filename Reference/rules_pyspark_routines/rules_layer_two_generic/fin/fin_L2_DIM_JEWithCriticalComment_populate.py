# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import when, concat_ws, regexp_extract,dense_rank,trim
import sys
import traceback

def fin_L2_DIM_JEWithCriticalComment_populate():
    """Populate L2 Dimension JEWithCriticalComment"""

    try:

        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        
        global fin_L2_DIM_JEWithCriticalComment
        global fin_L1_STG_JEWithCriticalComment

        objGenHelper.gen_readFromFile_perform(\
                          gl_knowledgePath + "knw_LK_GD_JECriticalComment.delta").\
                          createOrReplaceTempView('knw_LK_GD_JECriticalComment')
                                
        # Construct a list of critical comments from knw table.
        ls_knwCriticalComments = spark.sql("SELECT DISTINCT JECriticalComment \
                                 FROM knw_LK_GD_JECriticalComment").rdd.flatMap(lambda x: x).collect()
        analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))

        # Concatenate header and line descriptions and filter only those cases where header or line is nonempty.
        fin_L1_STG_JEWithCriticalComment= spark.sql(\
                "SELECT  \
                      jrnl.journalSurrogateKey \
		             ,jrnl.transactionIDbyJEDocumnetNumber \
		             ,CONCAT(CASE WHEN RTRIM(LTRIM(jrnl.headerDescription)) IS NULL THEN '' \
                                  ELSE RTRIM(LTRIM(jrnl.headerDescription)) END,\
                             CASE WHEN RTRIM(LTRIM(jrnl.lineDescription)) IS NULL THEN '' \
                                  ELSE RTRIM(LTRIM(jrnl.lineDescription)) END) AS headerLineDescription  \
                 FROM	 fin_L1_TD_Journal jrnl \
		         INNER JOIN gen_L2_DIM_Organization dou2	ON \
							               dou2.companyCode		    = jrnl.companyCode	\
							               AND dou2.companyCode		<> '#NA#' \
                 WHERE  CASE WHEN jrnl.headerDescription IS NULL THEN '' ELSE jrnl.headerDescription END <> ''\
                    OR  CASE WHEN jrnl.lineDescription IS NULL THEN '' ELSE jrnl.lineDescription END <> '' ")
         # For each journal-line, collect all the critical comments 
          #from knw table that are present in its headerlinedescription into a |-separated string.
        fin_L1_STG_JEWithCriticalComment = fin_L1_STG_JEWithCriticalComment\
                .withColumn("jeCriticalComment",concat_ws('| ',*([when(regexp_extract \
                 (upper("headerLineDescription"),eachcomment.upper(), 0)!= "",eachcomment).otherwise(None) \
                 for eachcomment in ls_knwCriticalComments])))\
                .filter(trim(col("jeCriticalComment"))!='')\
                .withColumn("jeCriticalCommentSurrogateKey",dense_rank().over(Window().orderBy("jeCriticalComment")))\
                .drop("headerLineDescription")
        
        fin_L1_STG_JEWithCriticalComment = objDataTransformation.gen_convertToCDMandCache \
            (fin_L1_STG_JEWithCriticalComment,'fin','L1_STG_JEWithCriticalComment',False)

        fin_L2_DIM_JEWithCriticalComment = spark.sql("SELECT DISTINCT '"+ analysisid+"' as analysisID\
			                                            ,jeCriticalCommentSurrogateKey\
			                                            ,jeCriticalComment\
			                                          FROM fin_L1_STG_JEWithCriticalComment")
        
        fin_L2_DIM_JEWithCriticalComment = objDataTransformation.gen_convertToCDMandCache \
            (fin_L2_DIM_JEWithCriticalComment,'fin','L2_DIM_JEWithCriticalComment',True)
        
        #Writing to parquet file in dwh schema format
        dwh_vw_DIM_JEWithCriticalComment = objDataTransformation.gen_convertToCDMStructure_generate(fin_L2_DIM_JEWithCriticalComment,\
                                           'dwh','vw_DIM_JEWithCriticalComment',False)[0]            
        objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_JEWithCriticalComment,\
                                            gl_CDMLayer2Path + "fin_L2_DIM_JEWithCriticalComment.parquet" ) 
        
        executionStatus = "fin_L2_DIM_JEWithCriticalComment populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L2_DIM_JEWithCriticalComment")
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_STG_JEWithCriticalComment")
        if fin_L2_DIM_JEWithCriticalComment is not None:
            fin_L2_DIM_JEWithCriticalComment.unpersist()
        if fin_L1_STG_JEWithCriticalComment is not None:
            fin_L1_STG_JEWithCriticalComment.unpersist()

        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]          
