# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType
import sys
import traceback
def fin_L3_STG_JEBifurcation_Raw_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L2_FACT_GLBifurcation_Raw

    dwh_vw_FACT_JEBifurcation_Raw=fin_L3_STG_JEBifurcation.alias('JEB')\
      .join(fin_L2_DIM_JEBifurcation_Link.alias("l2DJEB"),\
                           (col("JEB.glJEBifurcationId")== col("l2DJEB.bifurcationIdDerived")),"inner")\
      .join(fin_L2_FACT_Journal_Extended.alias("JETEx"),\
                           (col("JEB.journalSurrogateKey").eqNullSafe(col("JETEx.journalSurrogateKey"))),"left")\
      .join(fin_L2_FACT_Journal_Extended_Link.alias("JETExL"),\
                           (col("JEB.journalSurrogateKey").eqNullSafe(col("JETExL.journalSurrogateKey"))),"left")\
      .join(fin_L2_DIM_FinancialPeriod.alias("FinPer"),\
                                 (col("JEB.postingDateSurrogateKey").eqNullSafe(col("FinPer.financialPeriodSurrogateKey"))),"left")\
      .select(col('JEB.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
              ,col('JEB.userSurrogateKey').alias('userSurrogateKey')\
              ,col('JEB.postingDateSurrogateKey').alias('postingDateSurrogateKey')\
              ,col('JEB.documentClassificationSurrogateKey').alias('documentClassificationSurrogateKey')\
              ,col('JEB.customerSurrogateKey').alias('customerSurrogateKey')\
              ,col('JEB.vendorSurrogateKey').alias('vendorSurrogateKey')\
              ,col('JEB.lineStatisticsSurrogateKey').alias('lineStatisticsSurrogateKey')\
              ,col('JEB.footPrintSurrogateKey').alias('footPrintSurrogateKey')\
              ,col('JEB.patternSourceSurrogateKey').alias('patternSourceSurrogateKey')\
              ,col('JEB.journalSurrogateKey').alias('journalSurrogateKey')\
              ,col('JEB.glBifurcationRuleTypeSurrogateKey').alias('glBifurcationRuleTypeSurrogateKey')\
              ,col('JEB.glBifurcationDataTypeSurrogateKey').alias('glBifurcationDataTypeSurrogateKey')\
              ,col('JEB.fixedGLAccountSurrogateKey').alias('fixedGLAccountSurrogateKey')\
              ,col('JEB.relatedGLAccountSurrogateKey').alias('relatedGLAccountSurrogateKey')\
              ,col('JEB.glAccountCombinationSurrogateKey').alias('glAccountCombinationSurrogateKey')\
              ,col('JEB.JEAdditionalAttributeSurrogateKey').alias('JEAdditionalAttributeSurrogateKey')\
              ,col('JEB.shiftedDateOfEntrySurrogateKey').alias('shiftedDateOfEntrySurrogateKey')\
              ,col('JEB.mainOffsetGLAccountSurrogateKey').alias('mainOffsetGLAccountSurrogateKey')\
              ,col('JEB.glJEBifurcationId').alias('glJEBifurcationId')\
              ,col('JEB.amountDr').alias('amountDr')\
              ,col('JEB.amountCr').alias('amountCr')\
              ,col('JEB.documentNumberWithCompanyCode').alias('documentNumberWithCompanyCode')\
              ,col('JEB.mainOffsetAmount').alias('mainOffsetAmount')\
              ,col('JEB.aggIntoLineItem').alias('aggIntoLineItem')\
              ,col('JEB.amountDrCr').alias('amountDrCr')\
              ,coalesce(col('l2DJEB.lineItemLinkID'),lit(-1)).alias('lineItemLinkID')\
              ,coalesce(col('l2DJEB.transactionLinkID'),lit(-1)).alias('transactionLinkID')
              ,coalesce(col('l2DJEB.exceptLineItemLinkID'),lit(-1)).alias('exceptLineItemLinkID')\
              ,coalesce(col('JETExL.DetailLinkID'),lit(-1)).alias('detailLinkID')\
              ,coalesce(col('JETExL.CustomLinkID'),lit(-1)).alias('customLinkID')\
              ,coalesce(col('JETEx.DocumentLineItemID'),lit(-1)).alias('documentLineItemID')\
              ,col('FinPer.financialPeriod')
             )
    
    
    fin_L2_FACT_GLBifurcation_Raw = objDataTransformation.gen_convertToCDMStructure_generate(dwh_vw_FACT_JEBifurcation_Raw,\
				'dwh','vw_FACT_JEBifurcation_Raw',True)[0]
    fin_L2_FACT_GLBifurcation_Raw.createOrReplaceTempView('fin_L2_FACT_GLBifurcation_Raw')
        
    objGenHelper.gen_writeToFile_perfom(fin_L2_FACT_GLBifurcation_Raw,gl_CDMLayer2Path + "fin_L2_FACT_GLBifurcation_Raw.parquet" \
                                       ,isCreatePartitions = True,partitionColumns = "financialPeriod")
    
    executionStatus = "fin_L2_FACT_GLBifurcation_Raw populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

