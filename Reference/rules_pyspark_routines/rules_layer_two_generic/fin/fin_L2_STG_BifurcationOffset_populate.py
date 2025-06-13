# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col,sum,max,expr

def fin_L2_STG_BifurcationOffset_populate():
    try:
         objGenHelper = gen_genericHelper()
         objDataTransformation = gen_dataTransformation()
         logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
         global fin_L2_STG_BifurcationOffset

         w = Window.partitionBy("glJETransactionNo","glBifurcationblockID","glJEFixedDrCrIndicator")
         UnbifurcatedAccountwise_DF = fin_L2_FACT_GLBifurcation\
                                     .select(col("glJETransactionNo")\
                                             ,col("glBifurcationblockID")\
                                             ,col("glJEFixedDrCrIndicator")\
                                             ,col("glJEFixedAccountIdSK")\
                                             ,col("glJEBIDRCRAmount")\
                                             ,col("ruleDetails"))\
                                     .filter(col("ruleDetails") == 'unbifurcated')
         fin_L2_TMP_UnbifurcatedAccountwise = UnbifurcatedAccountwise_DF\
                                            .groupBy("glJETransactionNo","glBifurcationblockID"\
                                                     ,"glJEFixedDrCrIndicator","glJEFixedAccountIdSK")\
                                            .agg(sum('glJEBIDRCRAmount').alias("accountwiseSum")\
                                             ,max(sum('glJEBIDRCRAmount')).over(w).alias('maxAccountwiseSum'))

         w2 = Window.partitionBy("glJETransactionNo","glBifurcationblockID"\
                                 ,"glJEFixedDrCrIndicator","accountwiseSum")                                           
         UnbifMaxAmtCntAcctws = fin_L2_TMP_UnbifurcatedAccountwise\
                               .select(col('glJETransactionNo')\
                                       ,col('glBifurcationblockID')\
                                       ,col('glJEFixedDrCrIndicator')\
                                       ,col('glJEFixedAccountIdSK')\
                                       ,col('maxAccountwiseSum')\
                                       ,col('accountwiseSum'))\
                               .filter(col('accountwiseSum')== col('maxAccountwiseSum'))
         fin_L2_TMP_UnbifurcateMaxAmountCountAccountwise = UnbifMaxAmtCntAcctws\
                                                .withColumn('countMaxAmountAccountwise',count('*').over(w2))

         glJEMainOffsetForDrCrIndicator = when(col('glJEFixedDrCrIndicator') == 'C',lit('D'))\
                                .otherwise(lit('C'))
         fin_L2_TMP_UnbifurcatedOffset = fin_L2_TMP_UnbifurcateMaxAmountCountAccountwise\
                                   .select(col('glJETransactionNo')\
	                                    ,col('glBifurcationblockID')\
		 							    ,lit(glJEMainOffsetForDrCrIndicator).alias('glJEMainOffsetForDrCrIndicator')\
		 							    ,col('glJEFixedAccountIdSK').alias('glJEMainOffsetAccountIdSK')\
									    ,col('maxAccountwiseSum').alias('glJEMainOffsetAmount'))\
									   .filter(col('countMaxAmountAccountwise')== 1)
         fin_L2_STG_BifurcationOffset = fin_L2_FACT_GLBifurcation.alias('gbi2')\
                              .join(fin_L2_TMP_UnbifurcatedOffset.alias('boff2')\
                                    ,((col('gbi2.glJETransactionNo')==col('boff2.glJETransactionNo'))\
                                       & (col('gbi2.glBifurcationblockID')==col('boff2.glBifurcationblockID'))\
									   & (col('gbi2.glJEFixedDrCrIndicator')==col('boff2.glJEMainOffsetForDrCrIndicator'))\
									   & (col('gbi2.ruleDetails')== 'unbifurcated')),how='inner')\
                              .select(col('gbi2.glJEBifurcationId').alias('glJEBifurcationId')\
                                       ,col('boff2.glJEMainOffsetAccountIdSK').alias('glJEMainOffsetAccountIdSK')\
                              ,expr(" CASE WHEN gbi2.glJEBIDRCRAmount < boff2.glJEMainOffsetAmount\
							   THEN CASE WHEN gbi2.glJEFixedDrCrIndicator = 'C' THEN  gbi2.glJEBIDRCRAmount\
                                                   ELSE -1*gbi2.glJEBIDRCRAmount END \
							   ELSE CASE WHEN gbi2.glJEFixedDrCrIndicator = 'C' THEN  boff2.glJEMainOffsetAmount \
                                                   ELSE -1*boff2.glJEMainOffsetAmount END \
							   END").alias('glJEMainOffsetAmount'))

         fin_L2_STG_BifurcationOffset  = objDataTransformation.gen_convertToCDMandCache(\
                                                 fin_L2_STG_BifurcationOffset,'fin','L2_STG_BifurcationOffset',False)

         executionStatus = "fin_L2_STG_BifurcationOffset populated sucessfully"
         executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
         return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
