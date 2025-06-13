# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import expr,col,lit,when


def fin_L3_STG_JEBifurcation_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L3_STG_JEBifurcation

        DF1 = fin_L2_FACT_GLBifurcation.alias('glb2')\
               .join(fin_L2_DIM_JEDocumentClassification.alias('docCl2')\
                    ,col('glb2.glJEDocumentClassificationSurrogateKey')==col('docCl2.JEDocumentClassificationSurrogateKey'),\
                    how='inner')\
               .join(fin_L2_STG_BifurcationOffset.alias('bos2')\
                     ,col('bos2.glJEBifurcationId')==col('glb2.glJEBifurcationId'),how='leftouter')\
               .select(col('glb2.glJEBifurcationId').alias('bifurcationIdDerived')\
                      ,col('glb2.glJEBifurcationId').alias('glJEBifurcationId')\
                      ,col('glb2.glJETransactionNo').alias('transactionNo')\
                      ,col('glb2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                      ,col('glb2.glJEUserSurrogateKey').alias('userSurrogateKey')\
                      ,col('glb2.glJEPostingDateSurrogateKey').alias('postingDateSurrogateKey')\
                      ,col('glb2.glJEDocumentClassificationSurrogateKey').alias('documentClassificationSurrogateKey')\
                      ,col('glb2.glJECustomerSurrogateKey').alias('customerSurrogateKey')\
                      ,col('glb2.glJEVendorSurrogateKey').alias('vendorSurrogateKey')\
                      ,col('glb2.glJEProductSurrogateKey').alias('productSurrogateKey')\
                      ,col('glb2.glJELineStatisticsSurrogateKey').alias('lineStatisticsSurrogateKey')\
                      ,col('glb2.glJEFootPrintSurrogateKey').alias('footPrintSurrogateKey')\
                      ,col('glb2.patternSourceSurrogateKey').alias('patternSourceSurrogateKey')\
                      ,col('glb2.journalSurrogateKey').alias('journalSurrogateKey')\
                      ,col('glb2.glBifurcationRuleTypeSurrogateKey').alias('glBifurcationRuleTypeSurrogateKey')\
                      ,col('glb2.glBifurcationDataTypeSurrogateKey').alias('glBifurcationDataTypeSurrogateKey')\
                      ,col('glb2.glJEFixedAccountIdSK').alias('fixedGLAccountSurrogateKey')\
                      ,col('glb2.glJERelatedAccountIdSK').alias('relatedGLAccountSurrogateKey')\
                      ,col('glb2.glAccountCombinationSurrogateKey').alias('glAccountCombinationSurrogateKey')\
                      ,lit(0).alias('bussinessProcessSorrogateKey')\
                      ,col('glb2.glJEAggIntoLineItem').alias('aggIntoLineItem')\
                      ,when(col('glb2.direction').isNull(),'').otherwise(col('glb2.direction')).alias('bifurcationDirection')\
                      ,col('glb2.enhancedAttribute').alias('bifurcationEnhancedAttribute')\
                      ,col('glJEFPId').alias('fingerPrintSurrogateKey')\
                      ,lit(0).alias('isMultiBPViewAssignment')\
                      ,when(col('glb2.glJELineItem').isNull(),lit(0))\
                           .otherwise(col('glb2.glJELineItem')).alias('lineItem')\
                      ,col('glb2.glJELineItemCalc').alias('lineItemCalculated')\
                      ,col('glb2.glJEFixedDrCrIndicator').alias('fixedDebitCreditTypeID')\
                      ,when(col('glb2.glJELineItemProcOrder').isNull(),lit(0))\
                                .otherwise(col('glb2.glJELineItemProcOrder')).alias('lineItemProcessingOrder')
                      ,col('glb2.loopCount').alias('bifurcationLoopCount')\
                      ,when(col('glb2.glJERelatedLineItem').isNull(),lit(0))\
                           .otherwise(col('glb2.glJERelatedLineItem')).alias('relatedLineItem')\
                      ,col('glb2.glJERelatedLineItemCalc').alias('relatedLineItemCalculated')\
                      ,col('glb2.glJERelatedDrCrIndicator').alias('relatedDebitCreditTypeID')\
                      ,col('glb2.glJERuleSequence').alias('ruleSequence')\
                      ,col('glb2.glJEBIDRCRAmount').alias('amountDrCr')\
                      ,col('glb2.glJEBIDRAmount').alias('amountDr')\
                      ,col('glb2.glJEBICRAmount').alias('amountCr')\
                      ,col('glb2.documentNumberWithCompanyCode').alias('documentNumberWithCompanyCode')\
                      ,col('glb2.glBifurcationblockID').alias('bifurcationBlockID')\
                      ,col('glb2.glBifurcationResultCategory').alias('bifurcationResultCategory')\
                      ,(col('glb2.glJEBIDRAmount')+col('glb2.glJEBICRAmount')).cast(DecimalType(32,6)).alias('amountDrCrTotal')\
                      ,lit(1).alias('partitionKeyForGLACube')\
                      ,col('glb2.JEAdditionalAttributeSurrogateKey').alias('JEAdditionalAttributeSurrogateKey')\
                      ,col('glb2.shiftedDateOfEntrySurrogateKey').alias('shiftedDateOfEntrySurrogateKey')\
                      ,col('glb2.glJEFixedLineSort').alias('glJEFixedLineSort')\
                      ,col('glb2.glJERelatedLineSort').alias('glJERelatedLineSort')\
                      ,when(col('bos2.glJEMainOffsetAccountIdSK').isNull(),col('glb2.glJERelatedAccountIdSK'))\
                               .otherwise(col('bos2.glJEMainOffsetAccountIdSK')).alias('mainOffsetGLAccountSurrogateKey')\
                      ,expr("CASE WHEN bos2.glJEMainOffsetAmount IS NOT NULL\
			                      THEN bos2.glJEMainOffsetAmount\
			                      ELSE CASE WHEN glb2.glJEFixedDrCrIndicator = 'D'\
						                   THEN -1* glb2.glJEBIDRCRAmount\
						                   ELSE glb2.glJEBIDRCRAmount \
					                     END\
		                      END").cast(DecimalType(32,6)).alias('mainOffsetAmount'))

        factBifurcationUnionValues =[(0,0,'0000000000',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'NONE','NONE','NONE',0,1,'NONE','NONE','NONE'\
                             ,0,'NONE','NONE','NONE','NONE',0,0,0,0,0,0,'6_NONE',0,-1,0,0,'NONE','NONE',0,0)]

        factBifurcationUnion_Schema = StructType([ 
                                     StructField("bifurcationIdDerived",IntegerType(),True), 
                                     StructField("glJEBifurcationId",IntegerType(),True),
                                     StructField("transactionNo",StringType(),True),
                                     StructField("organizationUnitSurrogateKey",IntegerType(),True),
                                     StructField("userSurrogateKey",IntegerType(),True),
                                     StructField("postingDateSurrogateKey",IntegerType(),True),
                                     StructField("documentClassificationSurrogateKey",IntegerType(),True),
                                     StructField("customerSurrogateKey",IntegerType(),True),
                                     StructField("vendorSurrogateKey",IntegerType(),True),
                                     StructField("productSurrogateKey",IntegerType(),True),
                                     StructField("lineStatisticsSurrogateKey",IntegerType(),True),
                                     StructField("footPrintSurrogateKey",IntegerType(),True),
                                     StructField("patternSourceSurrogateKey",IntegerType(),True),
                                     StructField("journalSurrogateKey",IntegerType(),True),
                                     StructField("glBifurcationRuleTypeSurrogateKey",IntegerType(),True),
                                     StructField("glBifurcationDataTypeSurrogateKey",IntegerType(),True),
                                     StructField("fixedGLAccountSurrogateKey",IntegerType(),True),
                                     StructField("relatedGLAccountSurrogateKey",IntegerType(),True),
                                     StructField("glAccountCombinationSurrogateKey",IntegerType(),True),
                                     StructField("bussinessProcessSorrogateKey",IntegerType(),True),
                                     StructField("aggIntoLineItem",StringType(),True),
                                     StructField("bifurcationDirection",StringType(),True),
                                     StructField("bifurcationEnhancedAttribute",StringType(),True),
                                     StructField("fingerPrintSurrogateKey",IntegerType(),True),
                                     StructField("isMultiBPViewAssignment",IntegerType(),True),
                                     StructField("lineItem",StringType(),True),
                                     StructField("lineItemCalculated",StringType(),True),
                                     StructField("fixedDebitCreditTypeID",StringType(),True),
                                     StructField("lineItemProcessingOrder",IntegerType(),True),
                                     StructField("bifurcationLoopCount",StringType(),True),
                                     StructField("relatedLineItem",StringType(),True),
                                     StructField("relatedLineItemCalculated",StringType(),True),
                                     StructField("relatedDebitCreditTypeID",StringType(),True),
                                     StructField("ruleSequence",IntegerType(),True),
                                     StructField("amountDrCr",IntegerType(),True),
                                     StructField("amountDr",IntegerType(),True),
                                     StructField("amountCr",IntegerType(),True),
                                     StructField("documentNumberWithCompanyCode",IntegerType(),True),
                                     StructField("bifurcationBlockID",IntegerType(),True),
                                     StructField("bifurcationResultCategory",StringType(),True),
                                     StructField("amountDrCrTotal",IntegerType(),True),
                                     StructField("partitionKeyForGLACube",IntegerType(),True),
                                     StructField("JEAdditionalAttributeSurrogateKey",IntegerType(),True),
                                     StructField("shiftedDateOfEntrySurrogateKey",IntegerType(),True),
                                     StructField("glJEFixedLineSort",StringType(),True),
                                     StructField("glJERelatedLineSort",StringType(),True),
                                     StructField("mainOffsetGLAccountSurrogateKey",IntegerType(),True),
                                     StructField("mainOffsetAmount",IntegerType(),True),
                                         ])
        DF2 = spark.createDataFrame(data = factBifurcationUnionValues , schema = factBifurcationUnion_Schema)
        fin_L3_STG_JEBifurcation = DF1.unionAll(DF2)
        fin_L3_STG_JEBifurcation = objDataTransformation.gen_convertToCDMStructure_generate(\
                                   fin_L3_STG_JEBifurcation, 'dwh','vw_FACT_JEBifurcation',True)[0]

        fin_L3_STG_JEBifurcation  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L3_STG_JEBifurcation,'fin','L3_STG_JEBifurcation',False)

        objGenHelper.gen_writeToFile_perfom(fin_L3_STG_JEBifurcation,gl_CDMLayer2Path + "fin_L2_FACT_GLBifurcation.parquet" )

        executionStatus = "fin_L3_STG_JEBifurcation populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
