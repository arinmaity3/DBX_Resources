# Databricks notebook source
#fin_usp_L2_DIM_GLAccountCombination_populate
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import lit,col,row_number,sum,avg,max,when,concat,coalesce
from pyspark.sql.window import Window

def fin_L2_DIM_GLAccountCombination_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L2_DIM_GLAccountCombination   
    
    #L1_TMP_GLAccountCombination
    L1_TMP_GLAccountCombination=fin_L1_STG_JEBifurcation_01_Combined.alias("jeb")\
                      .select(col("businessUnitCode").alias("businessUnitCode")\
                             ,when(col("jeb.fixedLIType")==lit("D") ,col("fixedAccount"))\
                              .otherwise(col('relatedAccount')).alias("debitAccountID")\
                             ,when(col("jeb.fixedLIType")==lit("D") ,col("relatedAccount"))\
                              .otherwise(col('fixedAccount')).alias("creditAccountID")\
                             ,lit(None).alias("T1")\
                             ,lit('').alias("T2")\
                             )\
                      .filter((col('jeb.fixedLIType').isin('D','C')) & (col('jeb.dataType')=='O'))\
                      .distinct()

    cond = ((col('ka.knowledgeAccountType') == lit('Account')) & \
                    (col('ka.knowledgeAccountID').isin(col('hier1.identifier_01'),col('hier1.identifier_02')
                     ,col('hier1.identifier_03'),col('hier1.identifier_04'),col('hier1.identifier_05')\
                     ,col('hier1.identifier_06'),col('hier1.identifier_07')\
                     ,col('hier1.identifier_08'),col('hier1.identifier_09')\
                                             ,'')))
    #Populate L1_STG_GLAccountHierarchy
    L1_STG_GLAccountHierarchy=fin_L1_STG_GLAccountMapping.alias("glmap1")\
                     .join(fin_L1_STG_GLAccountCategoryMaster.alias("glcm1")\
                             ,(col("glcm1.glAccountCategoryMasterSurrogateKey")==col("glmap1.accountCategoryID")),"inner")\
                     .join(fin_L1_STG_GLAccountFlattenHierarchy.alias("hier1")\
                             ,(col("hier1.leafID")==col("glmap1.accountCategoryID")),"inner")\
                     .join(knw_LK_CD_KnowledgeAccountSnapshot.alias("ka"),cond,"left")\
                     .select(col("glmap1.accountNumber").alias("accountNumber")\
                                    ,col("glcm1.accountCategory").alias("accountCategory")\
                                    ,col("ka.knowledgeAccountID").alias("knowledgeAccountID")\
                                    ,col("ka.displayName").alias("displayName")\
                                    ,col("ka.knowledgeAccountName").alias("knowledgeAccountName")\
                                    ,col("hier1.identifier_01").alias("identifier_01")\
                                    ,col("hier1.accountName_01").alias("accountName_01")\
                                    ,col("hier1.sortOrder_01").alias("sortOrder_01")\
                                    ,col("hier1.identifier_02").alias("identifier_02")\
                                    ,col("hier1.accountName_02").alias("accountName_02")\
                                    ,col("hier1.sortOrder_02").alias("sortOrder_02")\
                                    ,col("hier1.identifier_03").alias("identifier_03")\
                                    ,col("hier1.accountName_03").alias("accountName_03")\
                                    ,col("hier1.sortOrder_03").alias("sortOrder_03")\
                                    ,col("hier1.identifier_04").alias("identifier_04")\
                                    ,col("hier1.accountName_04").alias("accountName_04")\
                                    ,col("hier1.sortOrder_04").alias("sortOrder_04")\
                                    ,col("hier1.identifier_05").alias("identifier_05")\
                                    ,col("hier1.accountName_05").alias("accountName_05")\
                                    ,col("hier1.sortOrder_05").alias("sortOrder_05")\
                                    ,col("hier1.identifier_06").alias("identifier_06")\
                                    ,col("hier1.accountName_06").alias("accountName_06")\
                                    ,col("hier1.sortOrder_06").alias("sortOrder_06")\
                                    ,col("hier1.identifier_07").alias("identifier_07")\
                                    ,col("hier1.accountName_07").alias("accountName_07")\
                                    ,col("hier1.sortOrder_07").alias("sortOrder_07")\
                                    ,col("hier1.identifier_08").alias("identifier_08")\
                                    ,col("hier1.accountName_08").alias("accountName_08")\
                                    ,col("hier1.sortOrder_08").alias("sortOrder_08")\
                                    ,col("hier1.identifier_09").alias("identifier_09")\
                                    ,col("hier1.accountName_09").alias("accountName_09")\
                                    ,col("hier1.sortOrder_09").alias("sortOrder_09")\
                                   )
    #Populate L2_DIM_GLAccount_Details
    L2_DIM_GLAccount_Details=fin_L2_DIM_GLAccount.alias("gla")\
                      .join(fin_L2_DIM_GLAccountText.alias("txt")\
                                ,(col("txt.glAccountSurrogateKey")==col("gla.glAccountSurrogateKey")),"inner")\
                      .filter(col("gla.accountCategory")=='#NA#')\
                      .select(col("gla.organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey")\
                             ,col("gla.glAccountSurrogateKey").alias("glAccountSurrogateKey")\
                             ,col("gla.accountNumber").alias("accountNumber")\
                             ,col("gla.accountCategoryAudit").alias("accountCategoryAudit")\
                             ,col("gla.glAccountCategoryL1").alias("glAccountCategoryL1")\
                             ,col("gla.glAccountCategoryL2").alias("glAccountCategoryL2")\
                             ,col("gla.glAccountCategoryL3").alias("glAccountCategoryL3")\
                             ,col("gla.glAccountCategoryL4").alias("glAccountCategoryL4")\
                             ,col("gla.glAccountCategoryL5").alias("glAccountCategoryL5")\
                             ,col("gla.glAccountCategoryL6").alias("glAccountCategoryL6")\
                             ,col("gla.glAccountCategoryL7").alias("glAccountCategoryL7")\
                             ,col("gla.glAccountCategoryL8").alias("glAccountCategoryL8")\
                             ,col("gla.glAccountCategoryL9").alias("glAccountCategoryL9")\
                             ,col("txt.accountName").alias("accountName")\
                             ,col("txt.languageCode").alias("languageCode")\
                          )
    
    #Populate fin_L2_DIM_GLAccountCombination_TMP
    v_dcAccountNameCombination=concat(col('acc2.debitAccountID'),lit(' (')\
                                  ,(when(col('gla1.accountName').isNotNull(), col('gla1.accountName'))\
                              .otherwise(lit('Unbifurcated')))\
                                  ,lit(') | '),col('acc2.creditAccountID'),lit(' (')\
                                  ,(when(col('gla2.accountName').isNotNull(), col('gla2.accountName'))\
                              .otherwise(lit('Unbifurcated')))\
                                   ,lit(')')\
                                 )

    v_dcAccountCombination=concat((when(col('glmap1.accountCategory').isNotNull(), col('glmap1.accountCategory'))\
                           .otherwise(lit('Un-Assigned')))\
                                  ,lit(' | ')\
                           ,(when(col('glmap2.accountCategory').isNotNull(), col('glmap2.accountCategory'))\
                                    .otherwise(lit('Un-Assigned')))\
                               )

    v_dcKnowledgeAccountCombination=concat(coalesce(col('glmap1.knowledgeAccountName'),col('glmap1.knowledgeAccountName')\
                          ,lit('UnAssigned'))\
                          ,lit(' | ')\
                          ,coalesce(col('glmap2.knowledgeAccountName'),col('glmap2.knowledgeAccountName'),lit('UnAssigned'))\
                                        )
    
    v_isSameGLAccCombination=when(coalesce(col('gla1.glAccountSurrogateKey'),lit(0))==\
                                   coalesce(col('gla2.glAccountSurrogateKey'),lit(0)),lit('Yes'))\
                          .otherwise(lit('No'))

    v_dcAccountCombination_L1=when(((col("glmap1.accountName_01").isNull()) & (col("glmap2.accountName_01").isNull()))\
                          ,lit('Un-Assigned | Un-Assigned'))\
                          .otherwise(concat(coalesce(col('glmap1.accountName_01'),lit('Un-Assigned'))\
                              ,lit(' | ')\
                              ,coalesce(col('glmap2.accountName_01'),lit('Un-Assigned'))\
                              ))
        
    fin_L2_DIM_GLAccountCombination_TMP=L1_TMP_GLAccountCombination.alias("acc2")\
          .join(gen_L2_DIM_Organization.alias("org2"),(col("org2.companyCode")==\
                          (F.when(F.col("acc2.businessUnitCode").isNull(),lit('#NA#'))\
                            .when(F.col("acc2.businessUnitCode")=='',lit('#NA#'))\
                            .otherwise(F.col("acc2.businessUnitCode"))) ),"inner")\
         .join(L2_DIM_GLAccount_Details.alias("gla1"),( (col("gla1.organizationUnitSurrogateKey")\
               .eqNullSafe(col("org2.organizationUnitSurrogateKey")))\
               & (col("gla1.accountNumber").eqNullSafe(F.when(F.col("acc2.debitAccountID").isNull(),lit('#NA#'))\
                            .when(F.col("acc2.debitAccountID")=='',lit('#NA#'))\
                            .otherwise(F.col("acc2.debitAccountID"))) )),"left")\
         .join(L2_DIM_GLAccount_Details.alias("gla2"),( (col("gla2.organizationUnitSurrogateKey")\
               .eqNullSafe(col("org2.organizationUnitSurrogateKey")))\
               & (col("gla2.accountNumber").eqNullSafe(F.when(F.col("acc2.creditAccountID").isNull(),lit('#NA#'))\
                            .when(F.col("acc2.creditAccountID")=='',lit('#NA#'))\
                            .otherwise(F.col("acc2.creditAccountID"))) )),"left")\
         .join(L1_STG_GLAccountHierarchy.alias("glmap1")\
                        ,(col("glmap1.accountNumber").eqNullSafe(col("gla1.accountNumber"))),"left")\
         .join(L1_STG_GLAccountHierarchy.alias("glmap2")\
                        ,(col("glmap2.accountNumber").eqNullSafe(col("gla2.accountNumber"))),"left")\
         .select(col('org2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
             ,F.when(F.col("gla1.glAccountSurrogateKey").isNull(),lit(0))\
                 .otherwise(F.col("gla1.glAccountSurrogateKey")).alias("debitglAccountSurrogateKey")\
             ,F.when(F.col("gla2.glAccountSurrogateKey").isNull(),lit(0))\
                 .otherwise(F.col("gla2.glAccountSurrogateKey")).alias("creditglAccountSurrogateKey")\
             ,F.when(F.col("glmap1.knowledgeAccountID").isNull(),lit(0))\
                 .otherwise(F.col("glmap1.knowledgeAccountID")).alias("debitL4Identifier")\
             ,F.when(F.col("glmap2.knowledgeAccountID").isNull(),lit(0))\
                 .otherwise(F.col("glmap2.knowledgeAccountID")).alias("creditL4Identifier")\
             ,lit(v_dcAccountNameCombination).alias("dcAccountNameCombination")\
             ,lit(v_dcAccountCombination).alias("dcAccountCombination")\
             ,lit(v_dcKnowledgeAccountCombination).alias("dcKnowledgeAccountCombination")\
             ,lit(v_isSameGLAccCombination).alias("isSameGLAccCombination")\
             ,lit(v_dcAccountCombination_L1).alias("dcAccountCombination_L1")\
              #accountName
              ,when(((col('glmap1.accountName_01').isNull()) & (col('glmap2.accountName_01').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_01')\
              ,when(((col('glmap1.accountName_02').isNull()) & (col('glmap2.accountName_02').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_02')\
              ,when(((col('glmap1.accountName_03').isNull()) & (col('glmap2.accountName_03').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_03')\
              ,when(((col('glmap1.accountName_04').isNull()) & (col('glmap2.accountName_04').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_04')\
              ,when(((col('glmap1.accountName_05').isNull()) & (col('glmap2.accountName_05').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_05')\
              ,when(((col('glmap1.accountName_06').isNull()) & (col('glmap2.accountName_06').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_06')\
              ,when(((col('glmap1.accountName_07').isNull()) & (col('glmap2.accountName_07').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_07')\
              ,when(((col('glmap1.accountName_08').isNull()) & (col('glmap2.accountName_08').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_08')\
              ,when(((col('glmap1.accountName_09').isNull()) & (col('glmap2.accountName_09').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulaccName_09')\
              ,coalesce(col('glmap1.accountName_01'),lit('Un-Assigned')).alias("h1accName_01")\
              ,coalesce(col('glmap1.accountName_02'),col('glmap1.accountName_01'),lit('Un-Assigned')).alias("h1accName_02")\
              ,coalesce(col('glmap1.accountName_03'),col('glmap1.accountName_02'),col('glmap1.accountName_01')\
                        ,lit('Un-Assigned')).alias("h1accName_03")\
              ,coalesce(col('glmap1.accountName_04'),col('glmap1.accountName_03'),col('glmap1.accountName_02')\
                        ,col('glmap1.accountName_01')\
                         ,lit('Un-Assigned')).alias("h1accName_04")\
              ,coalesce(col('glmap1.accountName_05'),col('glmap1.accountName_04')\
                         ,col('glmap1.accountName_03'),col('glmap1.accountName_02'),col('glmap1.accountName_01')\
                        ,lit('Un-Assigned')).alias("h1accName_05")\
              ,coalesce(col('glmap1.accountName_06'),col('glmap1.accountName_05'),col('glmap1.accountName_04')\
                        ,col('glmap1.accountName_03'),col('glmap1.accountName_02'),col('glmap1.accountName_01')\
                        ,lit('Un-Assigned')).alias("h1accName_06")\
              ,coalesce(col('glmap1.accountName_07'),col('glmap1.accountName_06'),col('glmap1.accountName_05')\
                        ,col('glmap1.accountName_04')\
                        ,col('glmap1.accountName_03'),col('glmap1.accountName_02'),col('glmap1.accountName_01')\
                        ,lit('Un-Assigned')).alias("h1accName_07")\
              ,coalesce(col('glmap1.accountName_08')\
                         ,col('glmap1.accountName_07'),col('glmap1.accountName_06'),col('glmap1.accountName_05')\
                        ,col('glmap1.accountName_04')\
                        ,col('glmap1.accountName_03'),col('glmap1.accountName_02'),col('glmap1.accountName_01')\
                        ,lit('Un-Assigned')).alias("h1accName_08")\
              ,coalesce(col('glmap1.accountName_09'),col('glmap1.accountName_08')\
                         ,col('glmap1.accountName_07'),col('glmap1.accountName_06'),col('glmap1.accountName_05')\
                        ,col('glmap1.accountName_04')\
                        ,col('glmap1.accountName_03'),col('glmap1.accountName_02'),col('glmap1.accountName_01')\
                        ,lit('Un-Assigned')).alias("h1accName_09")\
              ,coalesce(col('glmap2.accountName_01'),lit('Un-Assigned')).alias("h2accName_01")\
              ,coalesce(col('glmap2.accountName_02'),col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_02")\
              ,coalesce(col('glmap2.accountName_03'),col('glmap2.accountName_02'),col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_03")\
              ,coalesce(col('glmap2.accountName_04'),col('glmap2.accountName_03'),col('glmap2.accountName_02')\
                        ,col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_04")\
              ,coalesce(col('glmap2.accountName_05'),col('glmap2.accountName_04')\
                        ,col('glmap2.accountName_03'),col('glmap2.accountName_02'),col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_05")\
              ,coalesce(col('glmap2.accountName_06'),col('glmap2.accountName_05'),col('glmap2.accountName_04')\
                        ,col('glmap2.accountName_03'),col('glmap2.accountName_02'),col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_06")\
              ,coalesce(col('glmap2.accountName_07'),col('glmap2.accountName_06'),col('glmap2.accountName_05')\
                        ,col('glmap2.accountName_04')\
                        ,col('glmap2.accountName_03'),col('glmap2.accountName_02'),col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_07")\
              ,coalesce(col('glmap2.accountName_08')\
                        ,col('glmap2.accountName_07'),col('glmap2.accountName_06'),col('glmap2.accountName_05')\
                        ,col('glmap2.accountName_04')\
                        ,col('glmap2.accountName_03'),col('glmap2.accountName_02'),col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_08")\
              ,coalesce(col('glmap2.accountName_09'),col('glmap2.accountName_08')\
                        ,col('glmap2.accountName_07'),col('glmap2.accountName_06'),col('glmap2.accountName_05')\
                        ,col('glmap2.accountName_04')\
                        ,col('glmap2.accountName_03'),col('glmap2.accountName_02'),col('glmap2.accountName_01')\
                        ,lit('Un-Assigned')).alias("h2accName_09")\
              ##sortOrder
              ,when(((col('glmap1.sortOrder_01').isNull()) & (col('glmap2.sortOrder_01').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_01')\
              ,when(((col('glmap1.sortOrder_02').isNull()) & (col('glmap2.sortOrder_02').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_02')\
              ,when(((col('glmap1.sortOrder_03').isNull()) & (col('glmap2.sortOrder_03').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_03')\
              ,when(((col('glmap1.sortOrder_04').isNull()) & (col('glmap2.sortOrder_04').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_04')\
              ,when(((col('glmap1.sortOrder_05').isNull()) & (col('glmap2.sortOrder_05').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_05')\
              ,when(((col('glmap1.sortOrder_06').isNull()) & (col('glmap2.sortOrder_06').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_06')\
              ,when(((col('glmap1.sortOrder_07').isNull()) & (col('glmap2.sortOrder_07').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_07')\
              ,when(((col('glmap1.sortOrder_08').isNull()) & (col('glmap2.sortOrder_08').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_08')\
              ,when(((col('glmap1.sortOrder_09').isNull()) & (col('glmap2.sortOrder_09').isNull())),lit('1'))\
                    .otherwise(lit('0')).alias('isNulSOrder_09')\
              ,coalesce(col('glmap1.sortOrder_01'),lit('-1')).alias("h1SOrder_01")\
              ,coalesce(col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01'),lit('-1')).alias("h1SOrder_02")\
              ,coalesce(col('glmap1.sortOrder_03'),col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01')\
                        ,lit('-1')).alias("h1SOrder_03")\
              ,coalesce(col('glmap1.sortOrder_04')\
                        ,col('glmap1.sortOrder_03'),col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01')\
                        ,lit('-1')).alias("h1SOrder_04")\
              ,coalesce(col('glmap1.sortOrder_05'),col('glmap1.sortOrder_04')\
                        ,col('glmap1.sortOrder_03'),col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01')\
                        ,lit('-1')).alias("h1SOrder_05")\
              ,coalesce(col('glmap1.sortOrder_06'),col('glmap1.sortOrder_05'),col('glmap1.sortOrder_04')\
                        ,col('glmap1.sortOrder_03'),col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01')\
                        ,lit('-1')).alias("h1SOrder_06")\
              ,coalesce(col('glmap1.sortOrder_07'),col('glmap1.sortOrder_06'),col('glmap1.sortOrder_05')\
                        ,col('glmap1.sortOrder_04')\
                        ,col('glmap1.sortOrder_03'),col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01')\
                        ,lit('-1')).alias("h1SOrder_07")\
              ,coalesce(col('glmap1.sortOrder_08')\
                        ,col('glmap1.sortOrder_07'),col('glmap1.sortOrder_06'),col('glmap1.sortOrder_05')\
                        ,col('glmap1.sortOrder_04')\
                        ,col('glmap1.sortOrder_03'),col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01')\
                        ,lit('-1')).alias("h1SOrder_08")\
              ,coalesce(col('glmap1.sortOrder_09'),col('glmap1.sortOrder_08')\
                      ,col('glmap1.sortOrder_07'),col('glmap1.sortOrder_06'),col('glmap1.sortOrder_05')\
                      ,col('glmap1.sortOrder_04')\
                      ,col('glmap1.sortOrder_03'),col('glmap1.sortOrder_02'),col('glmap1.sortOrder_01')\
                      ,lit('-1')).alias("h1SOrder_09")\
              ,coalesce(col('glmap2.sortOrder_01'),lit('-1')).alias("h2SOrder_01")\
              ,coalesce(col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_02")\
              ,coalesce(col('glmap2.sortOrder_03'),col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_03")\
              ,coalesce(col('glmap2.sortOrder_04')\
                        ,col('glmap2.sortOrder_03'),col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_04")\
              ,coalesce(col('glmap2.sortOrder_05'),col('glmap2.sortOrder_04')\
                        ,col('glmap2.sortOrder_03'),col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_05")\
              ,coalesce(col('glmap2.sortOrder_06'),col('glmap2.sortOrder_05'),col('glmap2.sortOrder_04')\
                        ,col('glmap2.sortOrder_03'),col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_06")\
              ,coalesce(col('glmap2.sortOrder_07'),col('glmap2.sortOrder_06'),col('glmap2.sortOrder_05')\
                        ,col('glmap2.sortOrder_04')\
                        ,col('glmap2.sortOrder_03'),col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_07")\
              ,coalesce(col('glmap2.sortOrder_08')\
                        ,col('glmap2.sortOrder_07'),col('glmap2.sortOrder_06'),col('glmap2.sortOrder_05')\
                        ,col('glmap2.sortOrder_04')\
                        ,col('glmap2.sortOrder_03'),col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_08")\
              ,coalesce(col('glmap2.sortOrder_09'),col('glmap2.sortOrder_08')\
                        ,col('glmap2.sortOrder_07'),col('glmap2.sortOrder_06'),col('glmap2.sortOrder_05')\
                        ,col('glmap2.sortOrder_04')\
                        ,col('glmap2.sortOrder_03'),col('glmap2.sortOrder_02'),col('glmap2.sortOrder_01')\
                        ,lit('-1')).alias("h2SOrder_09")\
              ,concat(coalesce(col('acc2.debitAccountID'),lit('')),lit('-')\
                        ,coalesce(col('gla1.accountName'),lit(''))).alias('debitAccontDtls')\
              ,concat(coalesce(col('acc2.creditAccountID'),lit('')),lit('-')\
                        ,coalesce(col('gla2.accountName'),lit(''))).alias('creditAccontDtls')\
              #AccountCategoryL1
              ,concat(coalesce(col('gla1.glAccountCategoryL1'),lit('NONE')),lit(' | ')\
                ,coalesce(col('gla2.glAccountCategoryL1'),lit('NONE'))).alias('dcKAAPAccountCombination_L1')		
              ,when(((col('gla1.glAccountCategoryL2').isNull()) & (col('gla2.glAccountCategoryL2').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_02')\
              ,when(((col('gla1.glAccountCategoryL3').isNull()) & (col('gla2.glAccountCategoryL3').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_03')\
              ,when(((col('gla1.glAccountCategoryL4').isNull()) & (col('gla2.glAccountCategoryL4').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_04')\
              ,when(((col('gla1.glAccountCategoryL5').isNull()) & (col('gla2.glAccountCategoryL5').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_05')\
              ,when(((col('gla1.glAccountCategoryL6').isNull()) & (col('gla2.glAccountCategoryL6').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_06')\
              ,when(((col('gla1.glAccountCategoryL7').isNull()) & (col('gla2.glAccountCategoryL7').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_07')\
              ,when(((col('gla1.glAccountCategoryL8').isNull()) & (col('gla2.glAccountCategoryL8').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_08')\
              ,when(((col('gla1.glAccountCategoryL9').isNull()) & (col('gla2.glAccountCategoryL9').isNull()))\
                ,lit('1')).otherwise(lit('0')).alias('isNulGlaCtgry_09')\
              ,concat(coalesce(col('gla1.accountCategoryAudit'),lit('NONE')),lit('|')\
                ,coalesce(col('gla2.accountCategoryAudit'),lit('NONE')))\
                    .alias("KAAPAccountCombinationCategoryAudit")\
              ,concat(coalesce(col('acc2.debitAccountID'),lit('0000000000')),coalesce(col('acc2.creditAccountID')\
                    ,lit('0000000000'))).alias('rowNumber')\
              ,coalesce(col("gla1.languageCode"),col("gla2.languageCode")).alias("languageCode")\
              )
    
    w = Window().orderBy('rowNumber')
    dcACIdentifier_w = Window.orderBy('debitAccontDtls')
    creditAccontDtls_w = Window.orderBy('creditAccontDtls')

    fin_L2_DIM_GLAccountCombination_TMP=fin_L2_DIM_GLAccountCombination_TMP\
            .withColumn("glKAAPAccountCombinationSK",row_number().over(w))\
            .withColumn("dcACIdentifier",concat(F.dense_rank().over(dcACIdentifier_w)\
                                         ,lit(' | '),F.dense_rank().over(creditAccontDtls_w)))
    
    fin_L2_DIM_GLAccountCombination=fin_L2_DIM_GLAccountCombination_TMP.alias("glac")\
             .select(\
               col('glKAAPAccountCombinationSK').alias("glKAAPAccountCombinationSK")\
               ,col("glac.organizationUnitSurrogateKey").alias('organizationUnitSurrogateKey')\
               ,col("glac.debitglAccountSurrogateKey").alias('debitglAccountSurrogateKey')\
               ,col("glac.creditglAccountSurrogateKey").alias('creditglAccountSurrogateKey')\
               ,col("glac.debitL4Identifier").alias('debitL4Identifier')\
               ,col("glac.creditL4Identifier").alias('creditL4Identifier')\
               ,col("glac.dcAccountNameCombination").alias('dcAccountNameCombination')\
               ,col("glac.dcAccountCombination").alias('dcAccountCombination')\
               ,col("glac.dcKnowledgeAccountCombination").alias('dcKnowledgeAccountCombination')\
               ,col("glac.isSameGLAccCombination").alias('isSameGLAccCombination')\
               ##dcAccountCombination
               ,col("glac.dcAccountCombination_L1").alias('dcAccountCombination_L1')\
               ,when(col("glac.isNulaccName_02")== 1,col("dcAccountNameCombination"))\
                    .otherwise(concat(coalesce(col('glac.h1accName_02'),lit('Un-Assigned'))\
                              ,lit(' | '),coalesce(col('glac.h2accName_02'),lit('Un-Assigned'))\
                              )).alias("dcAccountCombination_L2")\
               ,when(col("glac.isNulaccName_03")== 1,col("dcAccountNameCombination"))\
                   .otherwise(concat(coalesce(col('glac.h1accName_03'),lit('Un-Assigned'))\
                             ,lit(' | '),coalesce(col('glac.h2accName_03'),lit('Un-Assigned'))\
                             )).alias("dcAccountCombination_L3")\
               ,when(col("glac.isNulaccName_04")== 1,col("dcAccountNameCombination"))\
                    .otherwise(concat(coalesce(col('glac.h1accName_04'),lit('Un-Assigned'))\
                               ,lit(' | '),coalesce(col('glac.h2accName_04'),lit('Un-Assigned'))\
                                )).alias("dcAccountCombination_L4")\
               ,when(col("glac.isNulaccName_05")== 1,col("dcAccountNameCombination"))\
                    .otherwise(concat(coalesce(col('glac.h1accName_05'),lit('Un-Assigned'))\
                               ,lit(' | '),coalesce(col('glac.h2accName_05'),lit('Un-Assigned'))\
                               )).alias("dcAccountCombination_L5")\
               ,when(col("glac.isNulaccName_06")== 1,col("dcAccountNameCombination"))\
                   .otherwise(concat(coalesce(col('glac.h1accName_06'),lit('Un-Assigned'))\
                              ,lit(' | '),coalesce(col('glac.h2accName_06'),lit('Un-Assigned'))\
                               )).alias("dcAccountCombination_L6")\
               ,when(col("glac.isNulaccName_07")== 1,col("dcAccountNameCombination"))\
                   .otherwise(concat(coalesce(col('glac.h1accName_07'),lit('Un-Assigned'))\
                              ,lit(' | '),coalesce(col('glac.h2accName_07'),lit('Un-Assigned'))\
                              )).alias("dcAccountCombination_L7")\
               ,when(col("glac.isNulaccName_08")== 1,col("dcAccountNameCombination"))\
                   .otherwise(concat(coalesce(col('glac.h1accName_08'),lit('Un-Assigned'))\
                              ,lit(' | '),coalesce(col('glac.h2accName_08'),lit('Un-Assigned'))\
                              )).alias("dcAccountCombination_L8")\
               ,when(col("glac.isNulaccName_09")== 1,col("dcAccountNameCombination"))\
                   .otherwise(concat(coalesce(col('glac.h1accName_09'),lit('Un-Assigned'))\
                              ,lit(' | '),coalesce(col('glac.h2accName_09'),lit('Un-Assigned'))\
                              )).alias("dcAccountCombination_L9")\
                ##ACIdentifier
               ,when(col("glac.isNulSOrder_01")== 1,lit('-1|-1'))\
                    .otherwise(concat(col('glac.h1SOrder_01'),lit(' | '),col('glac.h2SOrder_01'))).alias("dcACIdentifier_L1")\
               ,when(col("glac.isNulSOrder_02")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_02'),lit(' | '),col('glac.h2SOrder_02'))).alias("dcACIdentifier_L2")\
               ,when(col("glac.isNulSOrder_03")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_03'),lit(' | '),col('glac.h2SOrder_03'))).alias("dcACIdentifier_L3")\
               ,when(col("glac.isNulSOrder_04")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_04'),lit(' | '),col('glac.h2SOrder_04'))).alias("dcACIdentifier_L4")\
               ,when(col("glac.isNulSOrder_05")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_05'),lit(' | '),col('glac.h2SOrder_05'))).alias("dcACIdentifier_L5")\
               ,when(col("glac.isNulSOrder_06")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_06'),lit(' | '),col('glac.h2SOrder_06'))).alias("dcACIdentifier_L6")\
               ,when(col("glac.isNulSOrder_07")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_07'),lit(' | '),col('glac.h2SOrder_07'))).alias("dcACIdentifier_L7")\
               ,when(col("glac.isNulSOrder_08")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_08'),lit(' | '),col('glac.h2SOrder_08'))).alias("dcACIdentifier_L8")\
               ,when(col("glac.isNulSOrder_09")== 1,col('dcACIdentifier'))\
                    .otherwise(concat(col('glac.h1SOrder_09'),lit(' | '),col('glac.h2SOrder_09'))).alias("dcACIdentifier_L9")\
               ,col("glac.dcKAAPAccountCombination_L1").alias('dcKAAPAccountCombination_L1')\
               ,when(col("glac.isNulGlaCtgry_02")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L2")\
               ,when(col("glac.isNulGlaCtgry_03")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L3")\
               ,when(col("glac.isNulGlaCtgry_04")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L4")\
               ,when(col("glac.isNulGlaCtgry_05")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L5")\
               ,when(col("glac.isNulGlaCtgry_06")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L6")\
               ,when(col("glac.isNulGlaCtgry_07")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L7")\
               ,when(col("glac.isNulGlaCtgry_08")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L8")\
               ,when(col("glac.isNulGlaCtgry_09")== 1,col("dcAccountNameCombination"))\
                    .otherwise(col("KAAPAccountCombinationCategoryAudit")).alias("dcKAAPAccountCombination_L9")\
               ,col("glac.languageCode").alias('languageCode')\
                                       )
    
    fin_L2_DIM_GLAccountCombination = objDataTransformation.gen_convertToCDMandCache(fin_L2_DIM_GLAccountCombination\
                                                     ,'fin','L2_DIM_GLAccountCombination',False)
    #Writing to parquet file in dwh schema format
    vw_DIM_GLAccountCombination_TMP=fin_L2_DIM_GLAccountCombination.alias("glco")\
         .join(fin_L2_DIM_KnowledgeAccountCombinationExpectation.alias("glbc"),(\
               (col("glco.debitL4Identifier").eqNullSafe(col("glbc.debitKnowledgeAccountID")))\
               & (col("glco.creditL4Identifier").eqNullSafe(col("glbc.creditKnowledgeAccountID")))),"left")\
          .select( col('glco.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
            ,col('glco.glKAAPAccountCombinationSK').alias('accountCombinationSurrogateKey')\
            ,col('glco.debitglAccountSurrogateKey').alias('debitglAccountSurrogateKey')\
            ,col('glco.creditglAccountSurrogateKey').alias('creditglAccountSurrogateKey')\
            ,col('glco.dcAccountNameCombination').alias('dcAccountNameCombination')\
            ,col('glco.dcAccountCombination').alias('dcAccountCombination')\
            ,col('glco.dcKnowledgeAccountCombination').alias('dcKnowledgeAccountCombination')\
            ,col('glco.isSameGLAccCombination').alias('isSameGLAccCombination')\
            ,col('glco.dcAccountCombination_L1').alias('dcAccountCombination_L1')\
            ,col('glco.dcAccountCombination_L2').alias('dcAccountCombination_L2')\
            ,col('glco.dcAccountCombination_L3').alias('dcAccountCombination_L3')\
            ,col('glco.dcAccountCombination_L4').alias('dcAccountCombination_L4')\
            ,col('glco.dcAccountCombination_L5').alias('dcAccountCombination_L5')\
            ,col('glco.dcAccountCombination_L6').alias('dcAccountCombination_L6')\
            ,col('glco.dcAccountCombination_L7').alias('dcAccountCombination_L7')\
            ,col('glco.dcAccountCombination_L8').alias('dcAccountCombination_L8')\
            ,col('glco.dcAccountCombination_L9').alias('dcAccountCombination_L9')\
            ,col('glco.dcACIdentifier_L1').alias('dcACIdentifier_L1')\
            ,col('glco.dcACIdentifier_L2').alias('dcACIdentifier_L2')\
            ,col('glco.dcACIdentifier_L3').alias('dcACIdentifier_L3')\
            ,col('glco.dcACIdentifier_L4').alias('dcACIdentifier_L4')\
            ,col('glco.dcACIdentifier_L5').alias('dcACIdentifier_L5')\
            ,col('glco.dcACIdentifier_L6').alias('dcACIdentifier_L6')\
            ,col('glco.dcACIdentifier_L7').alias('dcACIdentifier_L7')\
            ,col('glco.dcACIdentifier_L8').alias('dcACIdentifier_L8')\
            ,col('glco.dcACIdentifier_L9').alias('dcACIdentifier_L9')\
            ,F.when(F.lower(F.col('glco.dcAccountCombination_L2')).like('%unbifurcated%'),lit('unbifurcated'))\
                  .otherwise(coalesce(F.col('glbc.expectation'),lit('Unexpected'))).alias('Expectation')\
            ,col('glco.dcAccountCombination_L2').alias('expectationSortOrder')\
              )

    dwh_vw_DIM_GLAccountCombination=vw_DIM_GLAccountCombination_TMP.alias("glco").select(
            col('glco.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
        ,col('glco.accountCombinationSurrogateKey').alias('accountCombinationSurrogateKey')\
        ,col('glco.debitglAccountSurrogateKey').alias('debitglAccountSurrogateKey')\
        ,col('glco.creditglAccountSurrogateKey').alias('creditglAccountSurrogateKey')\
        ,col('glco.dcAccountNameCombination').alias('dcAccountNameCombination')\
        ,col('glco.dcAccountCombination').alias('dcAccountCombination')\
        ,col('glco.dcKnowledgeAccountCombination').alias('dcKnowledgeAccountCombination')\
        ,col('glco.isSameGLAccCombination').alias('isSameGLAccCombination')\
        ,col('glco.dcAccountCombination_L1').alias('dcAccountCombination_L1')\
        ,col('glco.dcAccountCombination_L2').alias('dcAccountCombination_L2')\
        ,col('glco.dcAccountCombination_L3').alias('dcAccountCombination_L3')\
        ,col('glco.dcAccountCombination_L4').alias('dcAccountCombination_L4')\
        ,col('glco.dcAccountCombination_L5').alias('dcAccountCombination_L5')\
        ,col('glco.dcAccountCombination_L6').alias('dcAccountCombination_L6')\
        ,col('glco.dcAccountCombination_L7').alias('dcAccountCombination_L7')\
        ,col('glco.dcAccountCombination_L8').alias('dcAccountCombination_L8')\
        ,col('glco.dcAccountCombination_L9').alias('dcAccountCombination_L9')\
        ,col('glco.dcACIdentifier_L1').alias('dcACIdentifier_L1')\
        ,col('glco.dcACIdentifier_L2').alias('dcACIdentifier_L2')\
        ,col('glco.dcACIdentifier_L3').alias('dcACIdentifier_L3')\
        ,col('glco.dcACIdentifier_L4').alias('dcACIdentifier_L4')\
        ,col('glco.dcACIdentifier_L5').alias('dcACIdentifier_L5')\
        ,col('glco.dcACIdentifier_L6').alias('dcACIdentifier_L6')\
        ,col('glco.dcACIdentifier_L7').alias('dcACIdentifier_L7')\
        ,col('glco.dcACIdentifier_L8').alias('dcACIdentifier_L8')\
        ,col('glco.dcACIdentifier_L9').alias('dcACIdentifier_L9')\
        ,col('glco.Expectation').alias('Expectation')
        ,F.when(F.lower(F.col('glco.Expectation'))==lit('expected'),1)\
          .when(F.lower(F.col('glco.Expectation'))==lit('unexpected'),2)\
          .when(F.lower(F.col('glco.Expectation'))==lit('same'),3)\
          .when(F.lower(F.col('glco.Expectation'))==lit('unique'),4)\
          .when(F.lower(F.col('glco.Expectation'))==lit('unbifurcated'),5)\
          .when(F.lower(F.col('glco.Expectation'))==lit('adjustment'),6)\
          .when(F.lower(F.col('glco.Expectation'))==lit('undetermined'),7)\
          .when(F.lower(F.col('glco.Expectation'))==lit('unknown'),8)\
          .when(F.lower(F.col('glco.Expectation'))==lit('none'),9)\
          .when(F.lower(F.col('glco.Expectation'))==lit('n/a'),10)\
          .otherwise(11).alias('expectationSortOrder')\
          )
          
    unknown_list = []
    #accountCombinationSurrogateKey_0
    lst_accountCombinationSurrogateKey_0=[['0', '0','0','0','NONE','NONE','NONE','No','NONE','NONE'\
                                           ,'NONE','NONE','NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE',9]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_0)

    #lst_accountCombinationSurrogateKey_1
    lst_accountCombinationSurrogateKey_1=[['0', '-1','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','expected',1]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_1)

    #lst_accountCombinationSurrogateKey_2
    lst_accountCombinationSurrogateKey_2=[['0', '-2','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','unexpected',2]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_2)

    #lst_accountCombinationSurrogateKey_3
    lst_accountCombinationSurrogateKey_3=[['0', '-3','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','unique',4]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_3)

    #lst_accountCombinationSurrogateKey_4
    lst_accountCombinationSurrogateKey_4=[['0', '-4','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','same',3]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_4)

    #lst_accountCombinationSurrogateKey_5
    lst_accountCombinationSurrogateKey_5=[['0', '-5','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','unique',4]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_5)

    #lst_accountCombinationSurrogateKey_6
    lst_accountCombinationSurrogateKey_6=[['0', '-6','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','undetermined',7]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_6)

    #lst_accountCombinationSurrogateKey_7
    lst_accountCombinationSurrogateKey_7=[['0', '-7','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','adjustment',6]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_7)

    #lst_accountCombinationSurrogateKey_8
    lst_accountCombinationSurrogateKey_8=[['0', '-8','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','unknown',8]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_8)

    #lst_accountCombinationSurrogateKey_9
    lst_accountCombinationSurrogateKey_9=[['0', '-9','0','0','NONE','NONE','NONE','No','NONE','NONE','NONE','NONE'\
                                           ,'NONE','NONE','NONE'\
                     ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','unbifurcated',5]]
    unknown_list.extend(lst_accountCombinationSurrogateKey_9)

    unknown_df = spark.createDataFrame(unknown_list)
    dwh_vw_DIM_GLAccountCombination = dwh_vw_DIM_GLAccountCombination.union(unknown_df)

    dwh_vw_DIM_GLAccountCombination = objDataTransformation.gen_convertToCDMStructure_generate(dwh_vw_DIM_GLAccountCombination\
                                     , 'dwh','vw_DIM_GLAccountCombination',True)[0]
    objGenHelper.gen_writeSingleCsvFile_perform(df = dwh_vw_DIM_GLAccountCombination,targetFile = gl_CDMLayer2Path + "fin_L2_DIM_GLAccountCombination.csv")
    executionStatus = "L2_DIM_GLAccountCombination populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
