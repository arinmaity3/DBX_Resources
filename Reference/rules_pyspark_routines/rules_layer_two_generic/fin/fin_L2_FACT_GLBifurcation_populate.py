# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when,dense_rank
from pyspark.sql.window import Window

def fin_L2_FACT_GLBifurcation_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L2_FACT_GLBifurcation
        
        org2_nullif = expr("nullif(glb2.businessUnitCode,'')")
        dgf2_nullif = expr("nullif(glb2.fixedAccount,'')")
        dgr2_nullif = expr("nullif(glb2.relatedAccount,'')")
        glBifurcationResultCategory = when(col('glb2.patternID').between(lit(1),lit(3)), lit('1_Unique'))\
                              .when(col('glb2.patternID').between(lit(4),lit(7)), lit('2_Unique acc/comb') )\
                              .when(col('glb2.patternID').between(lit(14),lit(15)), lit('2_Unique acc/comb') )\
                              .when(col('glb2.patternID') == lit(10), lit('2_Unique acc/comb') )\
                              .when(col('glb2.patternID').between(lit(8),lit(9)), lit('3_Position/Amount matching') )\
                              .when(col('glb2.patternID').between(lit(11),lit(13)), lit('3_Position/Amount matching') )\
                              .when(col('glb2.patternID')== lit(16), lit('3_Position/Amount matching') )\
                              .when((col('glb2.patternID') == lit(0)) & (col('glb2.rule') != lit('unbifurcated')), lit('4_No direct match'))\
                              .when(col('glb2.rule')== 'unbifurcated', lit('5_Unresolved') )\
                              .otherwise(lit('6_NONE'))
        w = Window().orderBy(lit(''))
        dr_window = Window().orderBy("organizationUnitSurrogateKey","glJETransactionNo")
        df_maxlength = fin_L1_STG_JEBifurcation_01_Combined.withColumn("maxLength",length(col("fixedOriginalLineItem"))).groupBy().max("maxLength")
        if(df_maxlength.first()[0] is not None):
            maxlinelength = df_maxlength.first()[0]+1
        else:
            maxlinelength = 0
      
        fin_L2_FACT_GLBifurcation = fin_L1_STG_JEBifurcation_01_Combined.alias('glb2')\
             .join(gen_L2_DIM_Organization.alias('org2')\
                 ,(col('org2.companyCode') == when((lit(org2_nullif).isNull()),'#NA#').otherwise(lit(org2_nullif))),how='inner')\
             .join(knw_L2_DIM_BifurcationPatternSource.alias('pat')\
                  ,((col('pat.patternSource')==col('glb2.patternSource'))\
                                      & (col('pat.patternID')==col('glb2.patternID'))\
                                      & (col('pat.blockSource')== col('glb2.blockSource'))),how='inner')\
             .join(fin_L2_DIM_GLAccount.alias('dgf2')\
                  ,((col('dgf2.accountNumber')==when((lit(dgf2_nullif).isNull()),'#NA#').otherwise(lit(dgf2_nullif)))\
                                      & (col('dgf2.organizationUnitSurrogateKey')==col('org2.organizationUnitSurrogateKey'))\
                                      & (col('dgf2.accountCategory')== '#NA#')),how='left')\
             .join(fin_L2_DIM_GLAccount.alias('dgr2')\
                   ,((col('dgr2.accountNumber')==when((lit(dgr2_nullif).isNull()),'#NA#').otherwise(lit(dgr2_nullif)))\
                                      & (col('dgr2.organizationUnitSurrogateKey')==col('org2.organizationUnitSurrogateKey'))\
                                      & (col('dgr2.accountCategory')== '#NA#')),how='left')\
             .join(knw_L2_DIM_BifurcationRuleType.alias('dbrt')\
                  ,((col('dbrt.glBifurcationRuleType')==col('glb2.bifurcationType'))\
                                      & (col('dbrt.glBifurcationRule')==col('glb2.rule'))),how='left')\
             .join(fin_L2_DIM_BifurcationDataType.alias('dbdt')\
                 ,(col('dbdt.glBifurcationDataType')==col('glb2.dataType')),how='left')\
             .join(fin_L2_DIM_GLAccountCombination.alias('DAC')\
                 ,((col('DAC.debitglAccountSurrogateKey')==col('dgf2.glAccountSurrogateKey'))\
                                      & (col('DAC.creditglAccountSurrogateKey')==col('dgr2.glAccountSurrogateKey'))\
                                      & (col('DAC.organizationUnitSurrogateKey')== col('org2.organizationUnitSurrogateKey'))\
                                      & (col('glb2.fixedLIType')== lit('D'))),how='left')\
             .join(fin_L2_DIM_GLAccountCombination.alias('CAC')\
                  ,((col('CAC.creditglAccountSurrogateKey')==col('dgf2.glAccountSurrogateKey'))\
                                      & (col('CAC.debitglAccountSurrogateKey')==col('dgr2.glAccountSurrogateKey'))\
                                      & (col('CAC.organizationUnitSurrogateKey')== col('org2.organizationUnitSurrogateKey'))\
                                      & (col('glb2.fixedLIType')== lit('C'))),how='left')\
             .select(trim(col('glb2.journalId')).alias('glJETransactionNo')\
                  ,date_format(to_date(when(col('glb2.postingDate').isNull(),'01-01-1900') \
                                                  .otherwise(col('glb2.postingDate'))\
                                                  ,"dd-MM-yyyy"),"yyyyMMdd").cast(IntegerType()).alias('glDatesk')\
                  ,date_format(to_date(lit("01-01-1900"),"dd-MM-yyyy"),"yyyyMMdd")\
                                                            .cast(IntegerType()).alias('transDateSk')
                  ,col('org2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                  ,col('glb2.journalSurrogateKey').alias('journalSurrogateKey')\
                  ,col('glb2.lineItemNumber').alias('glJELineItem')\
                  ,when(col('glb2.relatedLineItemNumber').isNull(),'')\
                            .otherwise(col('glb2.relatedLineItemNumber')).alias('glJERelatedLineItem')\
                  ,when(col('glb2.aggregatedInToLineItem').isNull(),'')\
                            .otherwise(col('glb2.aggregatedInToLineItem')).alias('glJEAggIntoLineItem')\
                  ,when((col('dgf2.glAccountSurrogateKey').isNull()),'0')\
                      .otherwise(col('dgf2.glAccountSurrogateKey')).alias('glJEFixedAccountIdSK')\
                  ,when((col('dgr2.glAccountSurrogateKey').isNull()),'0')\
                     .otherwise(col('dgr2.glAccountSurrogateKey')).alias('glJERelatedAccountIdSK')\
                  ,col('glb2.fixedLIType').alias('glJEFixedDrCrIndicator')\
                  ,col('glb2.amount').alias('glJEBIDRCRAmount')\
                  ,when(col('glb2.fixedLIType')=='D',col('glb2.amount'))\
                             .otherwise(lit(0.0)).alias('glJEBIDRAmount')\
                  ,expr("CASE	WHEN glb2.fixedLIType='C' THEN -1.0 * glb2.amount ELSE 0.0 END AS glJEBICRAmount")\
                  ,when(col('glb2.relatedLIType').isNull(),'')\
                           .otherwise(col('glb2.relatedLIType')).alias('glJERelatedDrCrIndicator')\
                  ,col('glb2.ruleSequence').alias('glJERuleSequence')\
                  ,when(col('glb2.jeLIProcessingOrder').isNull(),'NULL')\
                                .otherwise(col('glb2.jeLIProcessingOrder')).alias('glJELineItemProcOrder')\
                  ,lit(0).alias('glJEFPId')\
                  ,lit(0).alias('glAccountMatrixSurrogateKey')\
                  ,when((col('dbrt.glBifurcationRuleTypeSurrogateKey').isNull()),'0')\
                       .otherwise(col('dbrt.glBifurcationRuleTypeSurrogateKey')).alias('glBifurcationRuleTypeSurrogateKey')\
                  ,lit(0).alias('glJEKAAPAccountCombinationId')
                  ,when((col('dbdt.glBifurcationDataTypeSurrogateKey').isNull()),'0')\
                       .otherwise(col('dbdt.glBifurcationDataTypeSurrogateKey')).alias('glBifurcationDataTypeSurrogateKey')\
                  ,col('fixedOriginalLineItem').alias('glJELineItemCalc')\
                  ,lit(None).alias('glJEBIDRCRAmountTotal')\
                  ,when(col('glb2.loopCount').isNull(),'').otherwise(col('glb2.loopCount')).alias('loopCount')\
                  ,when(col('glb2.direction').isNull(),'')\
                       .otherwise(col('glb2.direction')).alias('direction')\
                  ,col('glb2.rule').alias('ruleDetails')\
                  ,col('glb2.enhancedAttribute').alias('enhancedAttribute')\
                  ,when(col('glb2.relatedOrginalLineItem').isNull(),'')\
                      .otherwise(col('glb2.relatedOrginalLineItem')).alias('glJERelatedLineItemCalc')\
                  ,col('pat.patternSourceSurrogateKey').alias('patternSourceSurrogateKey')\
                  ,col('glb2.blockID').alias('glBifurcationblockID')\
                  ,lpad(col('fixedOriginalLineItem'),maxlinelength,'0').alias('glJEFixedLineSort')\
                  ,when((col('relatedOrginalLineItem').isNull()),'0000')\
                     .otherwise(lpad(col('relatedOrginalLineItem'),maxlinelength,'0'))\
                                                     .alias('glJERelatedLineSort')\
                  ,lit(glBifurcationResultCategory).alias('glBifurcationResultCategory')\
                  ,coalesce(col("DAC.glKAAPAccountCombinationSK")\
                             ,col("CAC.glKAAPAccountCombinationSK"),lit(0)).alias("glAccountCombinationSurrogateKey"))
        fin_L2_FACT_GLBifurcation = fin_L2_FACT_GLBifurcation.withColumn("glJEBifurcationId", row_number().over(w))
        fin_L2_FACT_GLBifurcation = fin_L2_FACT_GLBifurcation.withColumn("documentNumberWithCompanyCode",F.dense_rank().over(dr_window))
       
        subqry_ffj1 = fin_L2_FACT_Journal.alias('jrnl2')\
                          .join(fin_L2_DIM_JEDocumentClassification.alias('jdcl2')\
						  ,col('jdcl2.JEDocumentClassificationSurrogateKey')==col('jrnl2.JEDocumentClassificationSurrogateKey'),how='inner')\
                          .select(col('jrnl2.organizationUnitSurrogateKey'),col('jrnl2.customerSurrogateKey'),col('jrnl2.vendorSurrogateKey')\
                                 ,col('jrnl2.productSurrogateKey'),col('jrnl2.postingDateSurrogateKey'),col('jdcl2.debitCreditIndicator')\
                                 ,col('jrnl2.documentNumber'),col('jrnl2.documentLineNumber'))
        CusSurKeyNULL = when(col('ctmr2#2.customerNumber').isNull(),lit('#NA#'))\
                                  .otherwise(col('ctmr2#2.customerNumber'))
        glJECustomerSurrogateKey = when(col('ctmr2#1.customerNumber')!='#NA#',col('ctmr2#1.customerSurrogateKey'))\
                                  .when((col('ctmr2#1.customerNumber')=='#NA#')&(lit(CusSurKeyNULL)!='#NA#'),col('ctmr2#2.customerSurrogateKey'))\
                                  .when((col('ctmr2#1.customerNumber')=='#NA#')&(lit(CusSurKeyNULL)=='#NA#'),col('ctmr2#1.customerSurrogateKey'))
        VendSurKeyNULL = when((col('vend2#2.vendorNumber').isNull()),lit('#NA#'))\
                                  .otherwise(col('vend2#2.vendorNumber'))			
        glJEVendorSurrogateKey = when(col('vend2#1.vendorNumber')!='#NA#',col('vend2#1.vendorSurrogateKey'))\
                                 .when((col('vend2#1.vendorNumber')=='#NA#')&(lit(VendSurKeyNULL)!='#NA#'),col('vend2#2.vendorSurrogateKey'))\
                                 .when((col('vend2#1.vendorNumber')=='#NA#')&(lit(VendSurKeyNULL)=='#NA#'),col('vend2#1.vendorSurrogateKey'))
        ProdSurkeyNULL = when(col('pdt2#2.productNumber').isNull(),lit('#NA#'))\
                                 .otherwise(col('pdt2#2.productNumber'))
        glJEProductSurrogateKey = when((col('pdt2#1.productNumber')!='#NA#')|(col('pdt2#1.productNumber').isNotNull())\
                                ,col('pdt2#1.productSurrogateKey'))\
                               .when(((col('pdt2#1.productNumber')=='#NA#')|(col('pdt2#1.productNumber').isNull()))\
                                   &(lit(ProdSurkeyNULL)!='#NA#')&(col('pdt2#2.productNumber').isNotNull()),col('pdt2#2.productSurrogateKey'))\
                               .when(((col('pdt2#1.productNumber')=='#NA#')|(col('pdt2#1.productNumber').isNull()))\
                                   &((lit(ProdSurkeyNULL)=='#NA#')|(col('pdt2#2.productNumber').isNull())),col('jrnl2.productSurrogateKey')) 
        fin_L2_FACT_GLBifurcation = fin_L2_FACT_GLBifurcation.alias('gbi2')\
                     .join(fin_L2_FACT_Journal.alias('jrnl2')\
                          ,col('jrnl2.journalSurrogateKey')==col('gbi2.journalSurrogateKey'),how='inner')\
                     .join(otc_L2_DIM_Customer.alias('ctmr2#1')\
                           ,col('ctmr2#1.customerSurrogateKey')==col('jrnl2.customerSurrogateKey'),how='left')\
                     .join(ptp_L2_DIM_Vendor.alias('vend2#1')\
 						   ,col('vend2#1.vendorSurrogateKey')==col('jrnl2.vendorSurrogateKey'),how='left')\
                     .join(gen_L2_DIM_Product.alias('pdt2#1'),(col('pdt2#1.productSurrogateKey')==col('jrnl2.productSurrogateKey')),how='left')\
                     .join(subqry_ffj1.alias('ffj1'),(col('ffj1.organizationUnitSurrogateKey')==col('gbi2.organizationUnitSurrogateKey'))\
                                                 & (col('ffj1.documentNumber')==col('gbi2.glJETransactionNo'))\
                                                 & (col('ffj1.documentLineNumber')== col('gbi2.glJERelatedLineItemCalc'))\
                                                 & (col('ffj1.debitCreditIndicator')==col('gbi2.glJERelatedDrCrIndicator'))\
 												 & (col('ffj1.postingDateSurrogateKey')== col('jrnl2.postingDateSurrogateKey')),how='left')\
                     .join(otc_L2_DIM_Customer.alias('ctmr2#2')\
                           ,col('ctmr2#2.customerSurrogateKey')==col('ffj1.customerSurrogateKey'),how='left')\
                     .join(ptp_L2_DIM_Vendor.alias('vend2#2')\
                           ,col('vend2#2.vendorSurrogateKey')==col('ffj1.vendorSurrogateKey'),how='left')\
                     .join(gen_L2_DIM_Product.alias('pdt2#2')\
                           ,col('pdt2#2.productSurrogateKey')==col('ffj1.productSurrogateKey'),how='left')\
                     .select(col('glJEBifurcationId'),col('glJETransactionNo'),col('glDatesk'),col('transDateSk')\
                             ,col('gbi2.organizationUnitSurrogateKey'),col('gbi2.journalSurrogateKey'),col('gbi2.glJELineItem')\
                             ,col('gbi2.glJERelatedLineItem'),col('gbi2.glJEAggIntoLineItem'),col('gbi2.glJEFixedAccountIdSK')\
                             ,col('gbi2.glJERelatedAccountIdSK'),col('gbi2.glJEFixedDrCrIndicator'),col('gbi2.glJEBIDRCRAmount')\
                             ,col('gbi2.glJEBIDRAmount'),col('gbi2.glJEBICRAmount'),col('gbi2.glJERelatedDrCrIndicator')\
                             ,col('gbi2.glJERuleSequence'),col('gbi2.glJELineItemProcOrder'),col('gbi2.glJEFPId')\
                             ,col('gbi2.glAccountMatrixSurrogateKey'),col('gbi2.glBifurcationRuleTypeSurrogateKey')\
                             ,col('gbi2.glJEKAAPAccountCombinationId'),col('gbi2.glBifurcationDataTypeSurrogateKey')\
                             ,col('gbi2.glJELineItemCalc'),col('gbi2.glJEBIDRCRAmountTotal'),col('gbi2.loopCount'),col('gbi2.direction')\
                             ,col('gbi2.ruleDetails'),col('gbi2.enhancedAttribute'),col('gbi2.glJERelatedLineItemCalc')\
                             ,col('gbi2.patternSourceSurrogateKey'),col('gbi2.glBifurcationblockID'),col('gbi2.glJEFixedLineSort')\
                             ,col('gbi2.glJERelatedLineSort'),col('gbi2.glBifurcationResultCategory')\
                             ,col('gbi2.documentNumberWithCompanyCode'),col('gbi2.glAccountCombinationSurrogateKey')\
                            ,when(col('jrnl2.userSurrogateKey').isNull(),lit(0))\
                                     .otherwise(col('jrnl2.userSurrogateKey')).alias('glJEUserSurrogateKey')\
                            ,when(col('jrnl2.postingDateSurrogateKey').isNull(),lit(0))\
                                     .otherwise(col('jrnl2.postingDateSurrogateKey')).alias('glJEPostingDateSurrogateKey')\
                            ,when(col('jrnl2.JEDocumentClassificationSurrogateKey').isNull(),lit(0))\
                                     .otherwise(col('jrnl2.JEDocumentClassificationSurrogateKey')).alias('glJEDocumentClassificationSurrogateKey')\
                            ,when(col('jrnl2.footPrintSurrogateKey').isNull(),lit(0))\
                                    .otherwise(col('jrnl2.footPrintSurrogateKey')).alias('glJEFootPrintSurrogateKey')\
                            ,when(col('jrnl2.lineStatisticsSurrogateKey').isNull(),lit(0))\
                                    .otherwise(col('jrnl2.lineStatisticsSurrogateKey')).alias('glJELineStatisticsSurrogateKey')\
                            ,when(lit(glJECustomerSurrogateKey).isNull(),lit(0))\
                                    .otherwise(lit(glJECustomerSurrogateKey)).alias('glJECustomerSurrogateKey')\
                            ,when((lit(glJEVendorSurrogateKey).isNull()),lit(0))\
                                    .otherwise(lit(glJEVendorSurrogateKey)).alias('glJEVendorSurrogateKey')\
                            ,when((lit(glJEProductSurrogateKey).isNull()),lit(0))\
                                     .otherwise(lit(glJEProductSurrogateKey)).alias('glJEProductSurrogateKey')\
                            ,when(col('jrnl2.JEAdditionalAttributeSurrogateKey').isNull(),lit(0))\
                                     .otherwise(col('jrnl2.JEAdditionalAttributeSurrogateKey')).alias('JEAdditionalAttributeSurrogateKey')\
                            ,when(col('jrnl2.shiftedDateOfEntrySurrogateKey').isNull(),lit(0))\
                                     .otherwise(col('jrnl2.shiftedDateOfEntrySurrogateKey')).alias('shiftedDateOfEntrySurrogateKey'))
       
        fin_L2_FACT_GLBifurcation = objDataTransformation.gen_convertToCDMandCache(fin_L2_FACT_GLBifurcation,'fin','L2_FACT_GLBifurcation',False)
       

        executionStatus = "fin_L2_FACT_GLBifurcation populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

