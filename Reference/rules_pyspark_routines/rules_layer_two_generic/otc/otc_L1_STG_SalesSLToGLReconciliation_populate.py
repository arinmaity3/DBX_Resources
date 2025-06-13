# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,abs,col,lit,expr,when,sum,max,min,coalesce,count,concat,round
from pyspark.sql.types import StructType,StructField,IntegerType,DecimalType
from pyspark.sql.window import Window

def otc_L1_STG_SalesSLToGLReconciliation_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

      global otc_L1_STG_SLToGLReconciliationResult
      global otc_L1_STG_SLToGLReconciliationBillingToFILines

      w = Window().orderBy(lit(''))
      w1 = Window().orderBy('glDocumentNumber') 
      dr_window= Window.partitionBy("billingDocumentNumber").orderBy("idGLJE")
      
      knw_vw_LK_GD_ClientKPMGDocumentType = gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType']
            
      otc_L1_TMP_SalesSLToGLReconciliationDistinctJE = fin_L1_STG_JELineItem\
          .select('companyCode','financialYear','GLJEDocumentNumber','idGLJE').distinct()

      v_charindex = expr("charindex('-',JOU.referenceSubledgerDocumentNumber)")
      v_Ltrim = expr("LEFT(JOU.referenceSubledgerDocumentNumber,(charindex('-',JOU.referenceSubledgerDocumentNumber)-1))")
      v_referenceSubledgerDocumentNumber = when(lit(v_charindex)>0,lit(v_Ltrim))\
          .otherwise(col('JOU.referenceSubledgerDocumentNumber'))
      
      otc_L1_TMP_SalesSLToGLReconciliationFI = fin_L1_TD_Journal.alias("JOU")\
           .join(fin_L1_STG_JEAccountFootPrint.alias('FPR')\
                  ,((col('FPR.companyCode').eqNullSafe(col('JOU.companyCode')))
                  &(col('FPR.fiscalYear').eqNullSafe(col('JOU.fiscalYear')))
                  &(col('FPR.documentNumber').eqNullSafe(col('JOU.documentNumber')))),'left')\
           .join(fin_L1_TD_Journal_ReferenceId.alias('joulr')\
                 ,((col('JOU.companyCode').eqNullSafe(col('joulr.companyCode')))
                 &(col('JOU.documentNumber').eqNullSafe(col('joulr.documentNumber')))),'left')\
           .filter(col('JOU.referenceSubledger')=='Sales S/L')\
           .select(when(col('joulr.referenceSubledgerDocumentNumber').isNull(),lit(v_referenceSubledgerDocumentNumber))\
                   .otherwise(col('joulr.referenceSubledgerDocumentNumber')).alias('referenceSubledgerDocumentNumber')\
                  ,col('JOU.companyCode').alias('companyCode')\
                  ,col('JOU.fiscalYear').alias('fiscalYear')\
                  ,col('JOU.localCurrency').alias('localCurrency')\
                  ,col('JOU.documentNumber').alias('DocumentNumber')\
                  ,col('FPR.DCL5AccountCatFootprint').alias('AccountFootprint')\
                  , when(col('JOU.debitCreditIndicator')==lit('D'),when(col('JOU.amountLC').isNull(),lit(0))\
                        .otherwise(col('JOU.amountLC'))).otherwise(lit(0)).alias('FIAmountLC'))\
           .groupBy('referenceSubledgerDocumentNumber','companyCode','fiscalYear','localCurrency','documentNumber','AccountFootprint')\
           .agg(sum(col('FIAmountLC')).alias('FIAmountLC'))

      otc_L1_TMP_SalesSLToGLReconciliationSDReceivable = fin_L1_STG_Receivable\
           .filter(col('isBillingFromSD')==lit(1))\
           .select('billingDocumentNumber','GLJEDocumentNumber','companyCode','fiscalYear','isSLBillingDocumentAmbiguous'\
                   ,'isSLBillingDocumentMissing'\
                  ,when(col('debitCreditKey')==lit('D'),when(col('amountLC').isNull(),lit(0))\
                      .otherwise(col('amountLC')))\
                  .otherwise(lit(-1)*(when(col('amountLC').isNull(),lit(0)).otherwise(col('amountLC')))).alias('SumARGLValueLC'))\
           .groupBy('billingDocumentNumber','GLJEDocumentNumber','companyCode','fiscalYear','isSLBillingDocumentAmbiguous'\
                   ,'isSLBillingDocumentMissing')\
           .agg(sum('SumARGLValueLC').alias('SumARGLValueLC'))

      otc_L1_TMP_SalesSLToGLReconciliationSDRevenue = fin_L1_STG_Revenue.alias('lsr')\
          .join(fin_L1_TD_Journal_ReferenceId.alias('joulr'),\
                (col('lsr.billingdocumentnumber').eqNullSafe(col('joulr.referenceid'))),'left')\
          .filter(col('lsr.isBillingFromSD')==lit(1))\
          .select(when(col('joulr.referencesubledgerdocumentnumber').isNull(),col('lsr.billingDocumentNumber'))\
                     .otherwise(col('joulr.referencesubledgerdocumentnumber')).alias('billingDocumentNumber')\
                 ,col('lsr.glDocumentNumber').alias('GLJEDocumentNumber')\
                 ,col('lsr.companyCode').alias('companyCode')\
                 ,col('lsr.fiscalYear').alias('FiscalYear')\
                 ,col('lsr.isSLBillingDocumentAmbigous').alias('isSLBillingDocumentAmbiguous')\
                 ,col('lsr.isSLBillingDocumentMissing').alias('isSLBillingDocumentMissing')\
                 ,col('lsr.postingDate').alias('postingDate')\
                 ,col('lsr.financialPeriod').alias('financialPeriod')\
                 ,when(col('lsr.revenueGLValueLC').isNull(),lit(0))\
                     .otherwise(col('lsr.revenueGLValueLC')).alias('SumRevenueGLValueLC')\
                 ,when(col('lsr.revenueGLValueDC').isNull(),lit(0))\
                     .otherwise(col('lsr.revenueGLValueDC')).alias('SumRevenueGLValueDC'))\
          .groupBy(col('billingDocumentNumber'),'GLJEDocumentNumber','companyCode','FiscalYear',\
                  'isSLBillingDocumentAmbiguous','isSLBillingDocumentMissing'\
                   ,'postingDate','financialPeriod')\
          .agg(sum('SumRevenueGLValueLC').alias('SumRevenueGLValueLC')\
              ,sum('SumRevenueGLValueDC').alias('SumRevenueGLValueDC'))

      v_TDJ_referenceSubledgerDocumentNumber = when(col('TDJ.referenceSubledgerDocumentNumber').isNull(),lit(''))\
              .otherwise(col('TDJ.referenceSubledgerDocumentNumber'))
      v_AR_billingDocumentNumber =  when(col('AR.billingDocumentNumber').isNull(),lit(''))\
                    .otherwise(col('AR.billingDocumentNumber'))
      v_REV_billingDocumentNumber = when(col('REV.billingDocumentNumber').isNull(),lit(''))\
                    .otherwise(col('REV.billingDocumentNumber'))
      
      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SalesSLToGLReconciliationFI.alias('TDJ')\
         .join(otc_L1_TMP_SalesSLToGLReconciliationDistinctJE.alias('DJE'),\
                ((col('DJE.companyCode').eqNullSafe(col('TDJ.companyCode')))&\
                 (col('DJE.financialYear').eqNullSafe(col('TDJ.fiscalYear')))&\
                 (col('DJE.GLJEDocumentNumber').eqNullSafe(col('TDJ.DocumentNumber')))),'left')\
         .join(otc_L1_TMP_SalesSLToGLReconciliationSDReceivable.alias('AR'),\
              ((col('AR.companyCode').eqNullSafe(col('TDJ.companyCode')))&\
               (col('AR.fiscalYear').eqNullSafe(col('TDJ.fiscalYear')))&\
               (col('AR.GLJEDocumentNumber').eqNullSafe(col('TDJ.DocumentNumber')))&\
               ((lit(v_TDJ_referenceSubledgerDocumentNumber)==lit(v_AR_billingDocumentNumber))|\
                (lit(v_TDJ_referenceSubledgerDocumentNumber)=='')|\
                (lit(v_AR_billingDocumentNumber)==''))),'left')\
         .join(otc_L1_TMP_SalesSLToGLReconciliationSDRevenue.alias('REV')\
              ,((col('REV.companyCode').eqNullSafe(col('TDJ.companyCode')))&\
                 (col('REV.fiscalYear').eqNullSafe(col('TDJ.fiscalYear')))&\
                 (col('REV.GLJEDocumentNumber').eqNullSafe(col('TDJ.DocumentNumber')))&\
                 ((lit(v_TDJ_referenceSubledgerDocumentNumber)==(lit(v_REV_billingDocumentNumber)))|\
                  (lit(v_TDJ_referenceSubledgerDocumentNumber)=='')|\
                  (lit(v_REV_billingDocumentNumber)==''))),'left')\
         .select(coalesce(col('REV.billingDocumentNumber'),col('AR.billingDocumentNumber'),col('TDJ.referenceSubledgerDocumentNumber'))\
                   .alias('billingDocumentNumber')\
                 ,col('DJE.idGLJE').alias('idGLJE'),col('REV.postingDate').alias('revenuePostingDate'),\
                 col('REV.financialPeriod').alias('revenueFinancialPeriod')\
                ,coalesce(col('REV.companyCode'),col('AR.companyCode'),col('TDJ.companyCode')).alias('companyCode')\
                ,coalesce(col('REV.FiscalYear'),col('AR.FiscalYear'),col('TDJ.FiscalYear')).alias('fiscalYear')\
                ,col('TDJ.localCurrency').alias('localCurrency')\
                ,coalesce(col('REV.GLJEDocumentNumber'),col('AR.GLJEDocumentNumber'),col('TDJ.DocumentNumber')).alias('glDocumentNumber')\
                ,coalesce(col('REV.isSLBillingDocumentAmbiguous'),col('AR.isSLBillingDocumentAmbiguous')\
                          ,lit(False)).alias('isSLBillingDocumentAmbiguous')\
                ,lit(False).alias('isAmbiguityResolved')\
                ,coalesce(col('REV.isSLBillingDocumentMissing'),col('AR.isSLBillingDocumentMissing'))\
                       .alias('isSLBillingDocumentMissing')\
                ,lit(0).alias('SLBillingDocumentMissingCategoryID')\
                ,when((when(col('REV.GLJEDocumentNumber').isNull(),'').otherwise(col('REV.GLJEDocumentNumber')))=='',lit(False)).\
                      otherwise(lit(True)).alias('hasRevenue')\
                ,when((when(col('AR.GLJEDocumentNumber').isNull(),'').otherwise(col('AR.GLJEDocumentNumber')))=='',lit(False)).\
                      otherwise(lit(True)).alias('hasAR')\
                ,col('TDJ.AccountFootprint').alias('AccountFootprint'),lit(True).alias('hasFIPosting')\
                ,lit(None).alias('countSDPositions'),col('TDJ.FIAmountLC').alias('JEAmountLC')\
                ,col('REV.SumRevenueGLValueLC').alias('SumRevenueGLValueLC'),col('REV.SumRevenueGLValueDC').alias('SumRevenueGLValueDC')\
                ,col('AR.SumARGLValueLC').alias('SumARGLValueLC')\
                ,lit(None).alias('sumSDBillingAmountLC'),lit(None).alias('SumSDBillingAmountDC')\
                ,lit(None).alias('SLtoGLRevenueDifferenceInLC'),lit(None).alias('SLtoGLRevenueDifferenceInDC'))
      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.\
           withColumn("combinedpopulationID", row_number().over(w))

      otc_L1_TMP_SLToGLReconciliationSDPopulation = otc_L1_TD_Billing.alias('ltb')\
          .join(fin_L1_TD_Journal_ReferenceId.alias('ltj'),\
               ((col('ltb.billingDocumentNumber').eqNullSafe(col('ltj.referenceSubledgerDocumentNumber')))&\
               (col('ltb.billingDocumentLineItem').eqNullSafe(col('ltj.referenceSubledgerDocumentLineNumber')))&\
               (col('ltb.billingDocumentCompanyCode').eqNullSafe(col('ltj.CompanyCode')))),'left')\
          .select(col('ltj.referenceID').alias('referenceID')\
                 ,col('ltb.billingDocumentNumber').alias('billingDocumentNumber')\
                 ,col('ltb.billingDocumentCompanyCode').alias('billingDocumentCompanyCode')\
                 ,col('ltb.billingDocumentLocalCurrency').alias('billingDocumentLocalCurrency') \
                ,when(col('ltb.billingDocumentAmountLC').isNull(),lit(0))\
                     .otherwise(col('ltb.billingDocumentAmountLC'))\
                     .alias('sumSDBillingAmountLC')\
                ,when(col('ltb.billingDocumentAmountDC').isNull(),lit(0))\
                     .otherwise(col('ltb.billingDocumentAmountDC'))\
                     .alias('SumSDBillingAmountDC'))\
         .groupBy('referenceID','billingDocumentNumber','billingDocumentCompanyCode','billingDocumentLocalCurrency')\
         .agg(count(lit(1)).alias('countSDPositions')\
              ,sum('sumSDBillingAmountLC').alias('sumSDBillingAmountLC')\
              ,sum('SumSDBillingAmountDC').alias('SumSDBillingAmountDC'))

      otc_L1_TMP_SLToGLReconciliationSDPopulationAggregated = otc_L1_TMP_SLToGLReconciliationSDPopulation\
         .groupBy('referenceID','billingDocumentCompanyCode','billingDocumentLocalCurrency')\
         .agg(sum('countSDPositions').alias('countSDPositions')\
              ,sum('sumSDBillingAmountLC').alias('sumSDBillingAmountLC')\
              ,sum('SumSDBillingAmountDC').alias('SumSDBillingAmountDC'))\
         .select('referenceID','billingDocumentCompanyCode','billingDocumentLocalCurrency'\
                 ,'countSDPositions','sumSDBillingAmountLC','SumSDBillingAmountDC')

      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC')\
         .join(otc_L1_TMP_SLToGLReconciliationSDPopulation.alias('SD'),\
              (col('SD.billingDocumentNumber').eqNullSafe(col('REC.billingDocumentNumber'))),'left')\
         .join(otc_L1_TMP_SLToGLReconciliationSDPopulationAggregated.alias('sdag')\
              ,(col('sdag.referenceID').eqNullSafe(when(col('SD.referenceID').isNull(),col('REC.billingDocumentNumber'))\
                                                   .otherwise(col('SD.referenceID')))),'left')\
         .select(col('REC.billingDocumentNumber'),col('REC.idGLJE')\
                 ,col('REC.revenuePostingDate'),col('REC.revenueFinancialPeriod')\
                 ,coalesce(col('REC.companyCode'),col('sdag.billingDocumentCompanyCode')\
                     ,col('SD.billingDocumentCompanyCode')).alias('companyCode'),col('REC.fiscalYear')\
                 ,coalesce(col('REC.localCurrency'),col('sdag.billingDocumentLocalCurrency')\
                     ,col('SD.billingDocumentLocalCurrency')).alias('localCurrency')\
                 ,col('REC.glDocumentNumber'),col('REC.isSLBillingDocumentAmbiguous')\
                 ,col('REC.isAmbiguityResolved')\
                 ,when(when(col('sdag.referenceID').isNull(),col('SD.billingDocumentNumber')).otherwise(col('sdag.referenceID'))\
                      .isNull(),lit(True)).otherwise(lit(False)).alias('isSLBillingDocumentMissing')\
                 ,col('REC.SLBillingDocumentMissingCategoryID'),col('REC.hasRevenue'),col('REC.hasAR')\
                 ,col('REC.AccountFootprint'),col('REC.hasFIPosting')\
                 ,coalesce(col('sdag.countSDPositions'),col('SD.countSDPositions'))\
                    .alias('countSDPositions')\
                 ,col('REC.JEAmountLC'),col('REC.SumRevenueGLValueLC'),col('REC.SumRevenueGLValueDC'),col('REC.SumARGLValueLC')\
                 ,coalesce(col('sdag.sumSDBillingAmountLC'),col('SD.sumSDBillingAmountLC'))\
                   .alias('sumSDBillingAmountLC')\
                 ,coalesce(col('sdag.SumSDBillingAmountDC'),col('SD.SumSDBillingAmountDC'))\
                   .alias('SumSDBillingAmountDC')\
                 ,col('REC.SLtoGLRevenueDifferenceInLC'),col('REC.SLtoGLRevenueDifferenceInDC')\
                 ,col('REC.combinedpopulationID'))

      maxrow = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.agg(max('combinedPopulationID').alias('combinedPopulationID'))
      maxrow = maxrow.select('combinedPopulationID').collect()[0][0]

      df_ = otc_L1_TMP_SLToGLReconciliationSDPopulation.alias('SD')\
           .join(otc_L1_TMP_SLToGLReconciliationSDPopulationAggregated.alias('sdag'),\
                ((col('sdag.referenceID').eqNullSafe(col('sd.referenceID')))&\
                (col('sdag.billingDocumentCompanyCode').eqNullSafe(col('sd.billingDocumentCompanyCode')))),'left')\
          .join(otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC'),\
               (col('REC.billingDocumentNumber').eqNullSafe(col('SD.billingDocumentNumber'))),'left')\
          .filter(col('REC.billingDocumentNumber').isNull())\
          .select(col('SD.billingDocumentNumber').alias('billingDocumentNumber'),lit(None).alias('idGLJE'),lit(None).alias('revenuePostingDate')\
                 ,lit(None).alias('revenueFinancialPeriod')\
                 ,when(col('sdag.billingDocumentCompanyCode').isNull(),col('SD.billingDocumentCompanyCode'))\
                    .otherwise(col('sdag.billingDocumentCompanyCode')).alias('companyCode')\
                 ,lit(None).alias('fiscalYear')\
                 ,when(col('sdag.billingDocumentLocalCurrency').isNull(),col('SD.billingDocumentLocalCurrency'))\
                    .otherwise(col('sdag.billingDocumentLocalCurrency')).alias('localCurrency')\
                 ,lit(None).alias('glDocumentNumber'),lit(False).alias('isSLBillingDocumentAmbiguous'),lit(False).alias('isAmbiguityResolved')\
                 ,lit(False).alias('isSLBillingDocumentMissing'),lit(0).alias('SLBillingDocumentMissingCategoryID'),lit(False).alias('hasRevenue')\
                 ,lit(False).alias('hasAR'),lit(None).alias('AccountFootprint'),lit(False).alias('hasFIPosting')\
                 ,when(col('sdag.countSDPositions').isNull(),col('SD.countSDPositions'))\
                     .otherwise(col('sdag.countSDPositions')).alias('countSDPositions')\
                 ,lit(0).alias('JEAmountLC'),lit(0).alias('SumRevenueGLValueLC'),lit(0).alias('SumRevenueGLValueDC')\
                 ,lit(0).alias('SumARGLValueLC'),when(col('sdag.sumSDBillingAmountLC').isNull(),\
                       col('SD.sumSDBillingAmountLC')).otherwise(col('sdag.sumSDBillingAmountLC')).alias('sumSDBillingAmountLC')\
                 ,when(col('sdag.SumSDBillingAmountDC').isNull(),col('SD.SumSDBillingAmountDC'))\
                       .otherwise(col('sdag.SumSDBillingAmountDC')).alias('SumSDBillingAmountDC')\
                 ,lit(None).alias('SLtoGLRevenueDifferenceInLC'),lit(None).alias('SLtoGLRevenueDifferenceInDC'))
      df_ = df_.withColumn("v_combinedPopulationID",row_number().over(w))\
        .withColumn("combinedPopulationID",lit(maxrow)+col('v_combinedPopulationID'))

      otc_L1_TMP_SLToGLReconciliationCombinedPopulation_1 = df_.select('billingDocumentNumber','idGLJE','revenuePostingDate'\
             ,'revenueFinancialPeriod','companyCode','fiscalYear','localCurrency','glDocumentNumber','isSLBillingDocumentAmbiguous'\
             ,'isAmbiguityResolved','isSLBillingDocumentMissing','SLBillingDocumentMissingCategoryID'\
             ,'hasRevenue','hasAR','AccountFootprint','hasFIPosting','countSDPositions','JEAmountLC','SumRevenueGLValueLC'\
             ,'SumRevenueGLValueDC','SumARGLValueLC','sumSDBillingAmountLC','SumSDBillingAmountDC','SLtoGLRevenueDifferenceInLC'\
             ,'SLtoGLRevenueDifferenceInDC','combinedPopulationID')
      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation\
                                .union(otc_L1_TMP_SLToGLReconciliationCombinedPopulation_1)

      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation\
                     .withColumn("ID", row_number().over(w1))

      CTE_SalesFlow = otc_L1_STG_26_SalesFlowTreeParent.alias('SFTP')\
               .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias('CDT'),\
                     ((col('SFTP.salesDocumentTypeClientID')==(col('CDT.documentTypeClientID')))&\
                     (col('CDT.documentTypeKPMGGroupID')==lit(3))),'inner')\
               .filter(col('SFTP.isArtificial')==lit(1))\
               .select(col('salesDocumentNumber').alias('salesDocumentNumber'))\
               .distinct()
      
      DF_update1 = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC')\
           .join(CTE_SalesFlow.alias('SFL'),\
                 (col('SFL.salesDocumentNumber').eqNullSafe(col('REC.billingDocumentNumber'))),'left')\
          .filter(col('REC.isSLBillingDocumentMissing')==lit(1))\
          .select(when(col('REC.SLBillingDocumentMissingCategoryID').isNull(),col('REC.SLBillingDocumentMissingCategoryID'))\
                  .otherwise(when(col('SFL.salesDocumentNumber').isNotNull(),lit(1)).otherwise(lit(2)))\
                     .alias('v_SLBillingDocumentMissingCategoryID')\
                 ,col('REC.ID'))
      
      
      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('org')\
               .join(DF_update1.alias('try'),(col('org.ID').eqNullSafe(col('try.ID')))
                    ,'left')\
               .select(col('org.billingDocumentNumber'),col('org.idGLJE')\
                      ,when(col('try.v_SLBillingDocumentMissingCategoryID').isNull(),col('org.SLBillingDocumentMissingCategoryID'))\
                           .otherwise(col('try.v_SLBillingDocumentMissingCategoryID')).alias('SLBillingDocumentMissingCategoryID')\
                      ,col('org.revenuePostingDate'),col('org.revenueFinancialPeriod')\
                 ,col('org.companyCode')\
                 ,col('org.fiscalYear')\
                 ,col('org.localCurrency')\
                 ,col('org.glDocumentNumber'),col('org.isSLBillingDocumentAmbiguous'),col('org.isAmbiguityResolved')\
                 ,col('org.isSLBillingDocumentMissing'),col('org.hasRevenue')\
                 ,col('org.hasAR'),col('org.AccountFootprint'),col('org.hasFIPosting')\
                 ,col('org.countSDPositions')\
                 ,col('org.JEAmountLC'),col('org.SumRevenueGLValueLC'),col('org.SumRevenueGLValueDC')\
                 ,col('org.SumARGLValueLC'),col('org.sumSDBillingAmountLC')\
                 ,col('org.SumSDBillingAmountDC')\
                 ,col('org.SLtoGLRevenueDifferenceInLC'),col('org.SLtoGLRevenueDifferenceInDC'),col('org.combinedpopulationID'))
     
      otc_L1_TMP_SLToGLReconciliationCombinedPopulation.createOrReplaceTempView("otc_L1_TMP_SLToGLReconciliationCombinedPopulation")
      otc_L1_TMP_CombinedPopulation_AmbiguousBilling = spark.sql("SELECT \
			 trev1.companyCode \
			,trev1.billingDocumentNumber \
			,count(DISTINCT trev1.idGLJE) as glDocumentNumberCount	\
		FROM otc_L1_TMP_SLToGLReconciliationCombinedPopulation trev1 \
		GROUP BY \
			 trev1.companyCode \
			,trev1.billingDocumentNumber\
            HAVING \
			count(DISTINCT trev1.idGLJE) > 1")

      otc_L1_TMP_CombinedPopulation_AmbiguousBillingMinFiscalYr = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('trev1')\
         .join(otc_L1_TMP_CombinedPopulation_AmbiguousBilling.alias('ambbill')\
              ,((col('trev1.billingDocumentNumber')==(col('ambbill.billingDocumentNumber')))&\
               (col('trev1.companyCode')==(col('ambbill.companyCode')))),how='inner')\
         .select(col('trev1.companyCode'),col('trev1.billingDocumentNumber'),col('trev1.glDocumentNumber').alias('GLJEDocumentNumber')\
                ,col('trev1.fiscalYear'))\
         .groupBy('companyCode','billingDocumentNumber','GLJEDocumentNumber')\
         .agg(min('fiscalYear').alias('minFiscalYear'))

      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation\
                     .withColumn("ID", row_number().over(w1))

      DF_update2 = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('revt1')\
           .join(otc_L1_TMP_CombinedPopulation_AmbiguousBilling.alias('ramb1'),\
                ((col('revt1.billingDocumentNumber')==(col('ramb1.billingDocumentNumber')))&\
                 (col('revt1.companyCode')==(col('ramb1.companyCode')))),'inner')\
          .join(otc_L1_TMP_CombinedPopulation_AmbiguousBillingMinFiscalYr.alias('minfsc'),\
               ((col('revt1.billingDocumentNumber')==(col('minfsc.billingDocumentNumber')))&\
               (col('revt1.companyCode')==(col('minfsc.companyCode')))&\
               (col('revt1.glDocumentNumber')==(col('minfsc.GLJEDocumentNumber')))),'inner')\
         .select(col('revt1.ID')\
                 ,when(((col('revt1.glDocumentNumber') == col('revt1.billingDocumentNumber')) & \
                      (col('revt1.fiscalYear') == col('minfsc.minFiscalYear'))), lit(False))\
                 .otherwise(lit(True)).alias('v_isSLBillingDocumentAmbiguous'))

      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('org')\
         .join(DF_update2.alias('try'),(col('org.ID').eqNullSafe(col('try.ID')))
              ,'left')\
         .select(col('org.billingDocumentNumber'),col('org.idGLJE')\
                ,col('org.SLBillingDocumentMissingCategoryID')\
                ,col('org.revenuePostingDate'),col('org.revenueFinancialPeriod')\
           ,when(col('try.v_isSLBillingDocumentAmbiguous').isNull(),col('org.isSLBillingDocumentAmbiguous'))\
                     .otherwise(col('try.v_isSLBillingDocumentAmbiguous')).alias('isSLBillingDocumentAmbiguous')\
           ,col('org.companyCode')\
           ,col('org.fiscalYear')\
           ,col('org.localCurrency')\
           ,col('org.glDocumentNumber'),col('org.isSLBillingDocumentMissing'),col('org.isAmbiguityResolved')\
           ,col('org.hasRevenue')\
           ,col('org.hasAR'),col('org.AccountFootprint'),col('org.hasFIPosting')\
           ,col('org.countSDPositions')\
           ,col('org.JEAmountLC'),col('org.SumRevenueGLValueLC'),col('org.SumRevenueGLValueDC')\
           ,col('org.SumARGLValueLC'),col('org.sumSDBillingAmountLC')\
           ,col('org.SumSDBillingAmountDC')\
           ,col('org.SLtoGLRevenueDifferenceInLC'),col('org.SLtoGLRevenueDifferenceInDC'),col('org.combinedpopulationID'))

      df_Ambiguity_union1 = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC_A')\
                .join(otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC_B'),\
                      ((col('REC_B.billingDocumentNumber')==(col('REC_A.billingDocumentNumber')))&\
                      (col('REC_B.isSLBillingDocumentAmbiguous')==(lit(0)))),'inner')\
                .filter(col('REC_A.isSLBillingDocumentAmbiguous')==lit(1))\
                .select(col('REC_A.billingDocumentNumber').alias('billingDocumentNumber')\
                       ,col('REC_A.idGLJE').alias('idGLJE')\
                       ,lit(1).cast('smallint').alias('caseIdentifier'))\
                .distinct()

      df_Ambiguity_union2 = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('rec')\
                 .select(col('rec.billingDocumentNumber').alias('billingDocumentNumber'),col('rec.idGLJE').alias('idGLJE')\
                        ,lit(2).cast('smallint').alias('caseIdentifier'))\
                 .filter(col('rec.isSLBillingDocumentAmbiguous')==lit(1))\
                 .distinct()
      
      otc_L1_TMP_SalesSLToGLReconciliationAmbiguity01 = df_Ambiguity_union1.union(df_Ambiguity_union2)

      otc_L1_TMP_SalesSLToGLReconciliationAmbiguity02 = otc_L1_TMP_SalesSLToGLReconciliationAmbiguity01\
                 .groupBy('billingDocumentNumber','idGLJE')\
                 .agg(min(col('caseIdentifier')).alias('caseIdentifier'))\
                 .select(col('billingDocumentNumber'),col('idGLJE'),col('caseIdentifier'))

      otc_L1_TMP_SLToGLReconciliationAmbiguityCases = otc_L1_TMP_SalesSLToGLReconciliationAmbiguity02\
                .select('billingDocumentNumber','idGLJE','caseIdentifier')

      otc_L1_TMP_SLToGLReconciliationAmbiguityCases = otc_L1_TMP_SLToGLReconciliationAmbiguityCases\
                .withColumn("AmbiguityOrder",F.dense_rank().over(dr_window))

      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation\
                     .withColumn("ID", row_number().over(w1))

      DF_update3 = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC_A')\
                .join(otc_L1_TMP_SLToGLReconciliationAmbiguityCases.alias('AMB'),\
                      ((col('AMB.billingDocumentNumber')==(col('REC_A.billingDocumentNumber')))&\
                      (col('AMB.idGLJE')==(col('REC_A.idGLJE')))&\
                      (col('AMB.AmbiguityOrder')==lit(1))&\
                      (col('AMB.caseIdentifier')==lit(2))),'inner')\
                .select(lit(True).alias('v_isAmbiguityResolved')\
                       ,col('REC_A.ID'))
      
      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('org')\
             .join(DF_update3.alias('try'),(col('org.ID')==(col('try.ID'))),'left')\
             .select(col('org.billingDocumentNumber'),col('org.idGLJE')\
                      ,col('org.SLBillingDocumentMissingCategoryID')\
                      ,col('org.revenuePostingDate'),col('org.revenueFinancialPeriod')\
                 ,col('org.isSLBillingDocumentAmbiguous')\
                 ,col('org.companyCode')\
                 ,col('org.fiscalYear')\
                 ,col('org.localCurrency')\
                 ,col('org.glDocumentNumber'),col('org.isSLBillingDocumentMissing')\
                 ,when(col('try.v_isAmbiguityResolved').isNull(),col('org.isAmbiguityResolved'))\
                           .otherwise(col('try.v_isAmbiguityResolved')).alias('isAmbiguityResolved')\
                 ,col('org.hasRevenue')\
                 ,col('org.hasAR'),col('org.AccountFootprint'),col('org.hasFIPosting')\
                 ,col('org.countSDPositions')\
                 ,col('org.JEAmountLC'),col('org.SumRevenueGLValueLC'),col('org.SumRevenueGLValueDC')\
                 ,col('org.SumARGLValueLC'),col('org.sumSDBillingAmountLC')\
                 ,col('org.SumSDBillingAmountDC')\
                 ,col('org.SLtoGLRevenueDifferenceInLC'),col('org.SLtoGLRevenueDifferenceInDC'),col('org.combinedpopulationID')).cache()
      
      billingDocumentNumber = when(col('REC.isAmbiguityResolved') == lit(1), col('AMB.billingDocumentNumber'))\
            .when((col('REC.isAmbiguityResolved') == lit(0)) & (col('AMB.caseIdentifier') == lit(1))\
			       ,concat(col('AMB.AmbiguityOrder'),lit('A'),col('AMB.billingDocumentNumber')))\
			.when((col('REC.isAmbiguityResolved') == lit(0)) & (col('AMB.caseIdentifier') == lit(2))\
			       ,concat((col('AMB.AmbiguityOrder')-1).cast("int"),lit('A'),col('AMB.billingDocumentNumber')))

      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = \
            otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC')\
            .join(otc_L1_TMP_SLToGLReconciliationAmbiguityCases.alias('AMB'),\
                  ((col('AMB.billingDocumentNumber').eqNullSafe(col('REC.billingDocumentNumber')))&\
                  (col('AMB.idGLJE').eqNullSafe(col('REC.idGLJE')))&\
                  (col('REC.isSLBillingDocumentAmbiguous')==lit(1))),how='left')\
           .select(when(col('AMB.billingDocumentNumber').isNotNull(),lit(billingDocumentNumber))\
                       .otherwise(col('REC.billingDocumentNumber')).alias('billingDocumentNumber')\
                   ,col('REC.idGLJE')\
                   ,col('REC.SLBillingDocumentMissingCategoryID')\
                   ,col('REC.revenuePostingDate'),col('REC.revenueFinancialPeriod')\
                 ,col('REC.isSLBillingDocumentAmbiguous')\
                 ,col('REC.companyCode')\
                 ,col('REC.fiscalYear')\
                 ,col('REC.localCurrency')\
                 ,col('REC.glDocumentNumber'),col('REC.isSLBillingDocumentMissing')\
                 ,col('REC.isAmbiguityResolved')\
                 ,col('REC.hasRevenue')\
                 ,col('REC.hasAR'),col('REC.AccountFootprint'),col('REC.hasFIPosting')\
                 ,col('REC.countSDPositions')\
                 ,col('REC.JEAmountLC'),col('REC.SumRevenueGLValueLC'),col('REC.SumRevenueGLValueDC')\
                 ,col('REC.SumARGLValueLC'),col('REC.sumSDBillingAmountLC')\
                 ,col('REC.SumSDBillingAmountDC')\
                 ,col('REC.SLtoGLRevenueDifferenceInLC'),col('REC.SLtoGLRevenueDifferenceInDC'),col('REC.combinedpopulationID'))
      
      otc_L1_TMP_SLToGLReconciliationCombinedPopulation = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC')\
           .withColumn("SLtoGLRevenueDifferenceInLC",(round((when(col('REC.sumSDBillingAmountLC').isNull(),lit(0))\
                 .otherwise(col('REC.sumSDBillingAmountLC'))),2)-\
                 (round((when(col('REC.SumRevenueGLValueLC').isNull(),lit(0))\
                 .otherwise(col('REC.SumRevenueGLValueLC'))),2)))\
                         .cast(DecimalType(32,6)))\
          .withColumn("SLtoGLRevenueDifferenceInDC",(round((when(col('REC.SumSDBillingAmountDC').isNull(),lit(0))\
                 .otherwise(col('REC.SumSDBillingAmountDC'))),2)-\
                 (round((when(col('REC.SumRevenueGLValueDC').isNull(),lit(0))\
                 .otherwise(col('REC.SumRevenueGLValueDC'))),2)))\
                         .cast(DecimalType(32,6)))    
      
      innerVr_AccountClassification = \
           when((col('TDJ.lineItemID') == lit('T')) | (col('GLA.isIncomeTax') == lit(1))|(col('isDeferredIncomeTax')==lit(1))|\
                         (col('isOtherTax')==lit(1)), lit('Tax'))\
           .when(col('GLA.isRevenue')==lit(1),lit('Revenue'))\
           .when(col('GLA.isCash')==lit(1),lit('Cash'))\
           .when(col('GLA.isBank')==lit(1),lit('Bank'))\
           .when(col('GLA.isTradeAR')==lit(1),lit('Trade AR'))\
           .when(col('GLA.isTradeAP')==lit(1),lit('Trade AP'))\
           .when(col('GLA.isCostofSales')==lit(1),lit('Cost Of Sales'))\
           .when(col('GLA.isInterest')==lit(1),lit('Interest'))\
           .when(col('GLA.isExpense')==lit(1),lit('Expense'))\
           .when(col('GLA.isGRIR')==lit(1),lit('GR/IR'))\
           .when(col('GLA.isAccrued')==lit(1),lit('Accrued'))\
           .when(col('GLA.isLiability')==lit(1),lit('Liability'))\
           .when(col('GLA.isOtherPayables')==lit(1),lit('Other Payable'))\
           .when(col('GLA.isIncome')==lit(1),lit('Income (Not Revenue)'))\
           .when((col('GLA.isRawMaterialsGross') == lit('1')) | (col('GLA.isWorkinProgressGross') == lit(1))\
             |(col('GLA.isManufacturedFinishedGoodsGross')==lit(1))|(col('GLA.isPurchasedFinishedGoodsGross') == lit(1))|\
                         (col('isAllowanceForWorkInProgress')==lit(1))|(col('isAllowanceForRawMaterialsAndFactorySupplies') == lit(1))\
           			  |(col('isAllowanceforManufacturedFinishedGoods') == lit(1))|(col('isAllowanceforPurchasedFinishedGoods') == lit(1)), lit('Inventory'))\
           .when(col('GLA.isAssets')==lit(1),lit('Assets'))\
           .when(col('GLA.isSalesReturns')==lit(1),lit('Sales Return'))\
           .otherwise(lit('Other'))
           
  
      otc_L1_TMP_SLToGLReconciliationBillingToFILines  = otc_L1_TMP_SLToGLReconciliationCombinedPopulation.alias('REC')\
         .join(fin_L1_TD_Journal.alias('TDJ'),\
               ((col('TDJ.companyCode').eqNullSafe(col('REC.companyCode')))&\
               (col('TDJ.fiscalYear').eqNullSafe(col('REC.fiscalYear')))&\
               (col('TDJ.documentNumber').eqNullSafe(col('REC.glDocumentNumber')))),'left')\
         .join(fin_L1_TD_Journal_ReferenceId.alias('TDJ1'),\
              ((when(col('TDJ1.referenceSubledgerDocumentNumber').isNull(),lit(''))\
                    .otherwise(col('TDJ1.referenceSubledgerDocumentNumber')).eqNullSafe\
               (when(col('REC.billingDocumentNumber').isNull(),lit(''))\
                    .otherwise(col('REC.billingDocumentNumber'))))&\
              (col('TDJ.companyCode').eqNullSafe(col('TDJ1.companyCode')))&\
              (col('TDJ.documentNumber').eqNullSafe(col('TDJ1.documentNumber')))&\
              (col('TDJ.lineitem').eqNullSafe(col('TDJ1.lineitem')))),'left')\
        .join(gen_L2_DIM_Organization.alias('DOU'),(col('DOU.companyCode').eqNullSafe(col('TDJ.companyCode'))),'left')\
        .join(fin_L3_STG_GLAccount.alias('GLA'),\
              ((col('GLA.organizationUnitSurrogateKey').eqNullSafe(col('DOU.organizationUnitSurrogateKey')))&\
             (col('GLA.accountNumber').eqNullSafe(col('TDJ.accountNumber')))),'left')\
        .select(col('REC.combinedPopulationID').alias('combinedPopulationID')\
                ,col('REC.billingDocumentNumber').alias('billingDocumentNumber')\
               ,col('REC.idGLJE').alias('idGLJE'),col('REC.SumRevenueGLValueLC').alias('SumRevenueGLValueLC')\
               ,col('REC.SLtoGLRevenueDifferenceInLC').alias('SLtoGLRevenueDifferenceInLC')\
               ,col('TDJ.companyCode').alias('CompanyCode'),col('TDJ.fiscalYear').alias('FinancialYear')\
               ,col('TDJ.documentNumber').alias('FIDocumentNumber'),col('TDJ.lineItem').alias('FIDocumentLineItem')\
               ,col('TDJ.debitCreditIndicator').alias('debitCreditIndicator')\
               ,when(col('TDJ.debitCreditIndicator')==lit('D'),col('TDJ.amountLC'))\
                    .otherwise(-1*col('TDJ.amountLC')).cast(DecimalType(32,6)).alias('FIAmountLineLC')\
               ,when(col('GLA.glAccountSurrogateKey').isNull(),lit(0))\
                    .otherwise(col('GLA.glAccountSurrogateKey')).alias('glAccountSurrogateKey')\
               ,col('GLA.accountNumber').alias('AccountNumber')\
                ,col('GLA.accountNumberName').alias('accountNumberName')\
               ,col('GLA.accountCategoryTypeDescription').alias('AccountCategory')\
                ,col('GLA.knowledgeAccountName').alias('KnowledgeAccountName')\
               ,when(col('GLA.knowledgeAccountName')==lit('UnAssigned'),lit('Unssigned'))\
                     .otherwise(col('GLA.accountCategoryL4')).alias('AccountCategoryL4')\
               ,when(col('GLA.knowledgeAccountName')==lit('UnAssigned'),lit('Unssigned'))\
                     .otherwise(col('GLA.accountCategoryL3')).alias('AccountCategoryL3')\
               ,when(col('REC.hasFIPosting')==lit(1),lit(innerVr_AccountClassification))\
                    .otherwise(lit(None)).alias('AccountClassification')\
               ,when(col('GLA.isRevenue').isNull(),lit(False)).otherwise(col('GLA.isRevenue')).alias('isRevenue'))

      otc_L1_STG_SLToGLReconciliationResult = otc_L1_TMP_SLToGLReconciliationCombinedPopulation\
          .select('combinedpopulationID','billingDocumentNumber','idGLJE','revenuePostingDate','revenueFinancialPeriod','glDocumentNumber'\
		      ,'companyCode','fiscalYear','localCurrency','AccountFootPrint','SLBillingDocumentMissingCategoryID'\
              ,when((col('isSLBillingDocumentMissing') == lit(0)) & (col('hasFIPosting') == lit(1)), lit(1))\
                .when((col('isSLBillingDocumentMissing') == lit(0)) & (col('hasFIPosting') == lit(0)), lit(2))\
                .when((col('isSLBillingDocumentMissing') == lit(1)) & (col('hasFIPosting') == lit(1)), lit(3))\
                .otherwise(lit(0)).alias('SDFIExistenceCategoryID')\
              ,when((col('hasRevenue') == lit(1)) & (col('hasAR') == lit(1))&(col('isSLBillingDocumentAmbiguous') == lit(0)), lit(1))\
               .when((col('hasRevenue') == lit(1)) & (col('hasAR') == lit(0))&(col('isSLBillingDocumentAmbiguous') == lit(0)), lit(2))\
               .when((col('hasRevenue') == lit(0)) & (col('hasAR') == lit(1))&(col('isSLBillingDocumentAmbiguous') == lit(0)), lit(3))\
               .when((col('hasRevenue') == lit(0)) & (col('hasAR') == lit(0))&(col('isSLBillingDocumentAmbiguous') == lit(0)), lit(4))\
               .when((col('isSLBillingDocumentAmbiguous') == lit(1)) & (col('isAmbiguityResolved') == lit(1)), lit(6))\
               .when((col('isSLBillingDocumentAmbiguous') == lit(1)) & (col('isAmbiguityResolved') == lit(0)), lit(5))\
               .otherwise(lit(0)).alias('PostedCategoryID')\
             ,when((when(col('SumRevenueGLValueLC').isNull(),lit(0)).otherwise(col('SumRevenueGLValueLC'))!=lit(0)) \
                 & (when(col('SLtoGLRevenueDifferenceInLC').isNull(),lit(0))\
                 .otherwise(col('SLtoGLRevenueDifferenceInLC'))==lit(0)), lit(1))\
              .when((when(col('SumRevenueGLValueLC').isNull(),lit(0)).otherwise(col('SumRevenueGLValueLC'))!=lit(0)) \
                 & (when(col('SLtoGLRevenueDifferenceInLC').isNull(),lit(0))\
                 .otherwise(col('SLtoGLRevenueDifferenceInLC'))!=lit(0)), lit(2))\
              .when((when(col('SumRevenueGLValueLC').isNull(),lit(0)).otherwise(col('SumRevenueGLValueLC'))==lit(0)) \
                 & (when(col('SLtoGLRevenueDifferenceInLC').isNull(),lit(0))\
                 .otherwise(col('SLtoGLRevenueDifferenceInLC'))==lit(0)), lit(3))\
              .when((when(col('SumRevenueGLValueLC').isNull(),lit(0)).otherwise(col('SumRevenueGLValueLC'))==lit(0)) \
                 & (when(col('SLtoGLRevenueDifferenceInLC').isNull(),lit(0))\
                 .otherwise(col('SLtoGLRevenueDifferenceInLC'))!=lit(0)), lit(4)).alias('RevenueMatchCategoryID')\
             ,lit(99).alias('DifferenceInterpretationID'),'SumRevenueGLValueLC','sumSDBillingAmountLC'\
             ,'SLtoGLRevenueDifferenceInDC').cache()
             
      otc_L1_TMP_SLToGLReconciliationBillingToFILinesClassSums = otc_L1_TMP_SLToGLReconciliationBillingToFILines\
        .groupBy('billingDocumentNumber','idGLJE','SLtoGLRevenueDifferenceInLC')\
        .agg(sum(when(col('AccountClassification')==lit('Tax'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Tax')\
            ,sum(when(col('AccountClassification')==lit('Revenue'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Revenue')\
            ,sum(when(col('AccountClassification')==lit('Cash'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Cash')\
            ,sum(when(col('AccountClassification')==lit('Bank'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Bank')\
            ,sum(when(col('AccountClassification')==lit('Trade AP'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('TradeAP')\
            ,sum(when(col('AccountClassification')==lit('Trade AR'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('TradeAR')\
            ,sum(when(col('AccountClassification')==lit('Cost Of Sales'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('COGS')\
            ,sum(when(col('AccountClassification')==lit('Interest'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Interest')\
            ,sum(when(col('AccountClassification')==lit('Expense'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Expense')\
            ,sum(when(col('AccountClassification')==lit('GR/IR'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('GRIR')\
            ,sum(when(col('AccountClassification')==lit('Accrued'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Accrued')\
            ,sum(when(col('AccountClassification')==lit('Liability'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Liability')\
            ,sum(when(col('AccountClassification')==lit('Other Payable'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('OtherPayables')\
            ,sum(when(col('AccountClassification')==lit('Income (Not Revenue)'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('NonRevenueIncome')\
            ,sum(when(col('AccountClassification')==lit('Inventory'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Inventory')\
            ,sum(when(col('AccountClassification')==lit('Assets'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Assets')\
            ,sum(when(col('AccountClassification')==lit('Sales Return'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Sales Return')\
            ,sum(when(col('AccountClassification')==lit('Other'),col('FIAmountLineLC'))\
                 .otherwise(lit(0))).alias('Other'))\
        .select('billingDocumentNumber','idGLJE','SLtoGLRevenueDifferenceInLC','Tax','Revenue',\
               'Cash','Bank','TradeAP','TradeAR','COGS','Interest','Expense','GRIR','Accrued',\
               'Liability','OtherPayables','NonRevenueIncome','Inventory','Assets'\
               ,'Sales Return','Other')

      otc_L1_STG_SLToGLReconciliationResult = otc_L1_STG_SLToGLReconciliationResult.withColumn("ID", row_number().over(w1))

      DF_update4 = otc_L1_STG_SLToGLReconciliationResult.alias('RES')\
          .join(otc_L1_TMP_SLToGLReconciliationBillingToFILinesClassSums.alias('GRP')\
               ,((col('GRP.billingDocumentNumber').eqNullSafe(col('RES.billingDocumentNumber')))&\
                (col('GRP.idGLJE').eqNullSafe(col('RES.idGLJE')))),'inner') \
          .filter(((col('RES.PostedCategoryID')==3) & (col('RES.SDFIExistenceCategoryID')==1))|\
                 ((col('RES.PostedCategoryID')==4) & (col('RES.SDFIExistenceCategoryID')==1))|\
                 ((col('RES.RevenueMatchCategoryID')==2) & (col('RES.SDFIExistenceCategoryID')==1))|\
                 ((col('RES.RevenueMatchCategoryID')==4) & (col('RES.SDFIExistenceCategoryID')==1)))\
          .select(col('RES.ID')\
                 ,when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.COGS'))))==lit(0),lit(1))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.Expense'))))==lit(0),lit(2))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.Interest'))))==lit(0),lit(3))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.GRIR'))))==lit(0),lit(4))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.Accrued'))))==lit(0),lit(5))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.Liability'))))==lit(0),lit(6))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.OtherPayables'))))==lit(0),lit(7))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.NonRevenueIncome'))))==lit(0),lit(8))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.Inventory'))))==lit(0),lit(9))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.Assets'))))==lit(0),lit(10))\
          .when((abs(when(col('GRP.SLtoGLRevenueDifferenceInLC').isNull(),lit(0)).otherwise(col('GRP.SLtoGLRevenueDifferenceInLC')))\
               -(abs(col('GRP.Sales Return'))))==lit(0),lit(11))\
          .otherwise(lit(98)).alias('v_DifferenceInterpretationID'))

      otc_L1_STG_SLToGLReconciliationResult = otc_L1_STG_SLToGLReconciliationResult.alias('org')\
          .join(DF_update4.alias('try'),(col('org.ID').eqNullSafe(col('try.ID'))),'left')\
          .select(col('org.combinedPopulationID'),col('org.billingDocumentNumber'),col('org.idGLJE')\
               ,col('org.revenuePostingDate'),col('org.revenueFinancialPeriod'),col('org.glDocumentNumber')\
               ,when(col('try.v_DifferenceInterpretationID').isNull(),col('org.DifferenceInterpretationID'))\
                   .otherwise(col('try.v_DifferenceInterpretationID')).alias('DifferenceInterpretationID')\
               ,col('org.companyCode'),col('org.fiscalYear')\
               ,col('org.localCurrency'),col('org.AccountFootPrint')\
               ,col('org.SLBillingDocumentMissingCategoryID')\
               ,col('org.SDFIExistenceCategoryID'),col('org.PostedCategoryID')\
               ,col('org.RevenueMatchCategoryID'),col('org.SumRevenueGLValueLC'),col('org.sumSDBillingAmountLC')\
               ,col('org.SLtoGLRevenueDifferenceInDC')).cache()

      otc_L1_STG_SLToGLReconciliationResult = otc_L1_STG_SLToGLReconciliationResult.withColumn("ID", row_number().over(w1))
      DF_update5 = otc_L1_STG_SLToGLReconciliationResult.alias('RES')\
                    .select(col('RES.ID'),lit(30).alias('vr_DifferenceInterpretationID'))\
                   .filter((((col('RES.RevenueMatchCategoryID')==lit(2))&(col('RES.SDFIExistenceCategoryID')==lit(1)))|\
      			         ((col('RES.RevenueMatchCategoryID')==lit(4))&(col('RES.SDFIExistenceCategoryID')==lit(1))))&\
      					 (col('DifferenceInterpretationID')==lit(98))&\
      					 (col('SLtoGLRevenueDifferenceInDC')==lit(0)))
      					 
      otc_L1_STG_SLToGLReconciliationResult = otc_L1_STG_SLToGLReconciliationResult.alias('org')\
                   .join(DF_update5.alias('try'),(col('org.ID').eqNullSafe(col('try.ID'))),'left')\
      			 .select(col('org.combinedPopulationID'),col('org.billingDocumentNumber'),col('org.idGLJE'),col('org.revenuePostingDate')\
                     ,col('org.revenueFinancialPeriod'),col('org.glDocumentNumber')\
                     ,when(col('try.vr_DifferenceInterpretationID').isNull(),col('org.DifferenceInterpretationID'))\
                           .otherwise(col('try.vr_DifferenceInterpretationID')).alias('DifferenceInterpretationID')\
                     ,col('org.companyCode'),col('org.fiscalYear'),col('org.localCurrency'),col('org.AccountFootPrint')\
                     ,col('org.SLBillingDocumentMissingCategoryID'),col('org.SDFIExistenceCategoryID'),col('org.PostedCategoryID')\
                     ,col('org.RevenueMatchCategoryID'),col('org.SumRevenueGLValueLC'),col('org.sumSDBillingAmountLC')\
                     ,col('org.SLtoGLRevenueDifferenceInDC'))

      otc_L1_STG_SLToGLReconciliationResult  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_SLToGLReconciliationResult,'otc','L1_STG_SLToGLReconciliationResult',False)

      otc_L1_STG_SLToGLReconciliationBillingToFILines = otc_L1_TMP_SLToGLReconciliationBillingToFILines\
              .groupBy('combinedPopulationID'\
			           ,when(col('accountNumberName').isNull(),lit('NONE'))\
                           .otherwise(col('accountNumberName')).alias('AccountNumberName')\
					   ,when(col('glAccountSurrogateKey').isNull(),lit(0))\
                           .otherwise(col('glAccountSurrogateKey')).alias('glAccountSurrogateKey')\
					   ,'isRevenue')\
			  .agg(sum(col('FIAmountLineLC')).alias('FIAmountLC'))\
			  .select(col('combinedPopulationID').alias('combinedPopulationID')\
			          ,col('AccountNumberName').alias('AccountNumberName')\
					  ,col('glAccountSurrogateKey')\
                      ,col('FIAmountLC')\
					  ,col('isRevenue').alias('isRevenue'))

      otc_L1_STG_SLToGLReconciliationBillingToFILines  = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_SLToGLReconciliationBillingToFILines,'otc','L1_STG_SLToGLReconciliationBillingToFILines',False)

      executionStatus = "L1_STG_SalesSLToGLReconciliation populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
