# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import col,lit,when

def fin_L2_DIM_KnowledgeAccountCombinationExpectation_populate():

    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L2_DIM_KnowledgeAccountCombinationExpectation

        Expectation = when(col('ME.expectationName').isNotNull(),col('ME.expectationName')) \
                     .when(col('BE.expectationName').isNotNull(),col('BE.expectationName')) \
                     .when(col('KA.expectationID') == 1,lit('expected'))\
                     .when(col('DA.knowledgeAccountID') == col('CA.knowledgeAccountID'),lit('same'))\
                     .when((col('DA.isUnique') == 1)|(col('CA.isUnique') == 1),lit('unique'))\
                     .otherwise(lit('unexpected'))
        fin_L2_DIM_KnowledgeAccountCombinationExpectation = knw_LK_CD_KnowledgeAccountSnapshot.alias('DA')\
                   .crossJoin(knw_LK_CD_KnowledgeAccountSnapshot.alias('CA'))\
                   .join(knw_LK_CD_KnowledgeAccountExpectation.alias('KA')\
                               ,((col('KA.debitAccountID')== col('DA.knowledgeAccountID'))\
                               & (col('KA.creditAccountID')== col('CA.knowledgeAccountID'))),how='leftouter')\
                   .join(knw_LK_GD_Expectation.alias('ME')\
                               ,(col('ME.expectationID')== col('KA.modifiedExpectationID')),how='leftouter')\
		           .join(knw_LK_GD_Expectation.alias('BE')\
                               ,(col('BE.expectationID')== col('KA.expectationID')),how='leftouter')\
                   .select(col('DA.knowledgeAccountID').alias('DebitKnowledgeAccountID')\
                              ,when(col('DA.displayName').isNull(),col('DA.knowledgeAccountName'))\
                                     .otherwise(col('DA.displayName')).alias('DebitknowledgeAccountName')\
                              ,col('CA.knowledgeAccountID').alias('CreditKnowledgeAccountID')\
                              ,when(col('CA.displayName').isNull(),col('CA.knowledgeAccountName'))\
                                     .otherwise(col('CA.displayName')).alias('CreditknowledgeAccountName')\
                              ,lit(Expectation).alias('Expectation'))\
                              .filter((col('DA.knowledgeAccountType') == 'Account') & (col('CA.knowledgeAccountType') == 'Account'))\
                              .distinct()

        fin_L2_DIM_KnowledgeAccountCombinationExpectation = objDataTransformation.gen_convertToCDMandCache\
            (fin_L2_DIM_KnowledgeAccountCombinationExpectation, 'fin','L2_DIM_KnowledgeAccountCombinationExpectation',True)

        objGenHelper.gen_writeSingleCsvFile_perform(df = fin_L2_DIM_KnowledgeAccountCombinationExpectation,\
            targetFile = gl_CDMLayer2Path + "fin_L2_DIM_KnowledgeAccountCombinationExpectation.csv")

        executionStatus = "KnowledgeAccountCombinationExpectation completed successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
       executionStatus = objGenHelper.gen_exceptionDetails_log()
       executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
       return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
