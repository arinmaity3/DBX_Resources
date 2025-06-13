# Databricks notebook source
from pyspark.sql.functions import row_number,expr,dense_rank
from pyspark.sql.window import Window
import sys
import traceback

def fin_L2_DIM_JEAdditionalAttribute_populate():
    try:
        global fin_L2_STG_JEAdditionalAttribute
        global fin_L2_DIM_JEAdditionalAttribute
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

        fin_L2_STG_JEAdditionalAttribute_1 = spark.sql("select \
				jou1.journalSurrogateKey				AS journalSurrogateKey	\
				,dou2.organizationUnitSurrogateKey		AS organizationUnitSurrogateKey	\
				,dou2.companyCode						AS companyCode \
				,dou2.reportingLanguage					AS reportingLanguage	\
				,doc2.documentType						AS documentType	\
				,doc2.documentTypeDescription			AS documentTypeDescription	\
				,pos2.postingKey						AS postingKey	\
				,pos2.postingKeyDescription				AS postingKeyDescription	\
				,pos2.postingKeyKPMG					AS postingKeyKPMG	\
				,tra2.transactionType					AS transactionType	\
				,tra2.transactionTypeDescription		AS transactionTypeDescription	\
				,ref2.referenceTransactionCode			AS referenceTransactionCode	\
				,ref2.referenceTransactionDescription	AS referenceTransactionDescription	\
				,bus2.businessTransactionCode			AS businessTransactionCode	\
				,bus2.businessTransactionDescription	AS businessTransactionDescription	\
			from fin_L1_TD_Journal jou1	\
            join gen_L2_DIM_Organization dou2	\
				on dou2.companyCode	= jou1.companyCode	\
			join gen_L2_DIM_DocType doc2	\
				on doc2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey	\
				and doc2.documentType = case when isnull(nullif(jou1.documentType,'')) then '#NA#' else jou1.documentType end \
			join gen_L2_DIM_PostingKey pos2	\
				on pos2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey	\
				and pos2.postingKey = case when isnull(nullif(jou1.postingKey,'')) then '#NA#' else jou1.postingKey end \
			join gen_L2_DIM_Transaction tra2	\
				on tra2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey	\
				and tra2.transactionType = case when isnull(nullif(jou1.transactionCode,'')) \
                then '#NA#' else jou1.transactionCode end \
			join gen_L2_DIM_ReferenceTransaction ref2	\
				on ref2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey	\
				and ref2.referenceTransactionCode = case when isnull(nullif(jou1.referenceTransaction,'')) \
                then '#NA#' else jou1.referenceTransaction end \
			join gen_L2_DIM_BusinessTransaction bus2 \
				on bus2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey \
				and bus2.businessTransactionCode = case when isnull(nullif(jou1.businessTransactionCode,'')) \
                then '#NA#' else jou1.businessTransactionCode end")
        
        #----Begin
        keyCol=concat(col("companyCode") ,col("reportingLanguage") \
            , col("documentType") , col("documentTypeDescription")\
           ,col("postingKey") ,col("postingKeyDescription") \
           , col("postingKeyKPMG") , col("transactionType")\
           ,col("transactionTypeDescription") ,col("referenceTransactionCode") \
           , col("referenceTransactionDescription")\
           ,col("businessTransactionCode") ,col("businessTransactionDescription") \
           )
        fin_L2_STG_JEAdditionalAttribute_1 = fin_L2_STG_JEAdditionalAttribute_1\
          .withColumn("JEKey",hex(md5(keyCol)))

        w = Window.orderBy(F.lit(''))
        df_JEKey=fin_L2_STG_JEAdditionalAttribute_1.select("JEKey").distinct()\
        .withColumn("JEAdditionalAttributeSurrogateKey", F.row_number().over(w))

        fin_L2_STG_JEAdditionalAttribute =fin_L2_STG_JEAdditionalAttribute_1.alias('j')\
            .join(df_JEKey.alias('j1')\
                  ,["JEKey"],'inner')\
        .select("j.journalSurrogateKey","j.organizationUnitSurrogateKey"\
                 ,"j.companyCode","j.reportingLanguage"\
               ,"j.documentType","j.documentTypeDescription","j.postingKey"\
                ,"j.postingKeyDescription","j.postingKeyKPMG"\
                ,"j.transactionType","j.transactionTypeDescription","j.referenceTransactionCode"\
                ,"j.referenceTransactionDescription"\
                ,"j.businessTransactionCode","j.businessTransactionDescription"\
                ,"j1.JEAdditionalAttributeSurrogateKey"\
               )

        #---END
        fin_L2_STG_JEAdditionalAttribute = objDataTransformation.gen_convertToCDMandCache \
        (fin_L2_STG_JEAdditionalAttribute,'fin','L2_STG_JEAdditionalAttribute',True)

        fin_L2_DIM_JEAdditionalAttribute = spark.sql("select distinct	\
														 JEAdditionalAttributeSurrogateKey	\
														,organizationUnitSurrogateKey	\
														,companyCode	\
														,reportingLanguage	\
														,documentType	\
														,documentTypeDescription	\
														,postingKey	\
														,postingKeyDescription	\
														,postingKeyKPMG	\
														,transactionType	\
														,transactionTypeDescription	\
														,referenceTransactionCode	\
														,referenceTransactionDescription	\
														,businessTransactionCode	\
														,businessTransactionDescription	\
													  from fin_L2_STG_JEAdditionalAttribute")

        fin_L2_DIM_JEAdditionalAttribute  = objDataTransformation.gen_convertToCDMandCache \
			(fin_L2_DIM_JEAdditionalAttribute,'fin','L2_DIM_JEAdditionalAttribute',True)

		#Writing to parquet file in dwh schema format
        dwh_vw_DIM_JEAdditionalAttribute = objDataTransformation.gen_convertToCDMStructure_generate(fin_L2_DIM_JEAdditionalAttribute,\
                                        'dwh','vw_DIM_JEAdditionalAttribute',False)[0]
        
        fileName = gl_CDMLayer2Path + "fin_L2_DIM_JEAdditionalAttribute.parquet"
        objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_JEAdditionalAttribute,fileName)

        executionStatus = "L2_DIM_JEAdditionalAttribute populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


    finally:
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L2_STG_JEAdditionalAttribute")
