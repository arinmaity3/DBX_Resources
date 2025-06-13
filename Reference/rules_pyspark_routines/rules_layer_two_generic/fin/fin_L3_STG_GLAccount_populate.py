# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
#fin_L3_STG_GLAccount_populate
def fin_L3_STG_GLAccount_populate():
  try:
      
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    global fin_L3_STG_GLAccount
    erpSystemIDGeneric = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    if erpSystemIDGeneric is None:
      erpSystemIDGeneric = 10
    
    df_reportingLanguage=knw_LK_CD_ReportingSetup.select(col('KPMGDataReportingLanguage')).alias('languageCode')\
                         .distinct()
    reportingLanguage=df_reportingLanguage.first()[0]
    if reportingLanguage is None:
      reportingLanguage='EN'
    
    
    #fin.usp_L3_STG_GLAccount_populate
    #Step1
    new_join_BPTD1 = when(col('glAcc.isInterCoFromAccountMapping').isNull(), lit(3) ) \
                              .when(col('glAcc.isInterCoFromAccountMapping')==0, lit(3) ) \
                              .otherwise(col('glAcc.isInterCoFromAccountMapping'))

    new_join_BPTD2 = when(col('glAcc.isInterCoFromMasterData').isNull(), lit(3) ) \
                              .when(col('glAcc.isInterCoFromMasterData')==0, lit(3) ) \
                              .otherwise(col('glAcc.isInterCoFromMasterData'))

    new_join_BPTD3 = when(col('glAcc.isInterCoFromTransactionData').isNull(), lit(3) ) \
                              .when(col('glAcc.isInterCoFromTransactionData')==0, lit(3) ) \
                              .otherwise(col('glAcc.isInterCoFromTransactionData'))

    new_col_accountCategoryType=when( col('map.isBank')==1,                   lit(1)  ) \
                              .when(col('map.isGRIR')==1,                   lit(2)  ) \
                              .when(col('map.isAccrued')==1,                lit(3)  ) \
                              .when(col('map.isOtherPayables')==1,          lit(4)  ) \
                              .when(col('map.isTradeAR')==1,                lit(5)  ) \
                              .when(col('map.isTradeAP')==1,                lit(6)  ) \
                              .when(col('map.isInterest')==1,               lit(7)  ) \
                              .when(col('map.isCash')==1,                   lit(8)  ) \
                              .when(col('map.isRevenue')==1,                lit(9)  ) \
                              .when(col('map.isLiability')==1,              lit(10)  ) \
                              .when(col('map.isExpense')==1,                lit(11)  ) \
                              .when(col('map.isIncome')==1,                 lit(12)  ) \
                              .when(col('map.isAssets')==1,                 lit(13)  ) \
                              .when(col('map.isWorkinProgressGross')==1,    lit(14)  ) \
                              .when(col('map.isManufacturedFinishedgoodsGross')==1,       lit(15)  ) \
                              .when(col('map.isPurchasedFinishedGoodsGross')==1,          lit(16)  ) \
                              .when(col('map.isCostofSales')==1,            lit(17)  ) \
                              .when(col('map.isRawMaterialsGross')==1,      lit(18)  ) \
                              .when(col('map.isAllowanceForWorkinProgress')==1,           lit(19)  ) \
                              .when(col('map.isAllowanceforRawmaterialsandFactorySupplies')==1,     lit(20)  ) \
                              .when(col('map.isAllowanceforManufacturedFinishedGoods')==1,          lit(21)  ) \
                              .when(col('map.isAllowanceforPurchasedFinishedGoods')==1,             lit(22)  ) \
                              .when(col('map.isSalesDiscountsandVolumeRebates')==1,       lit(23)  ) \
                              .when(col('map.isRevenuefromSalesofGoods')==1,              lit(24)  ) \
                              .when(col('map.isServicesRevenue')==1,                      lit(25)  ) \
                              .when(col('map.isSalesReturns')==1,           lit(26)  ) \
                              .when(col('map.isBadDebtExpense')==1,         lit(27)  ) \
                              .when(col('map.isAllowanceforDoubtfulAccounts')==1,         lit(28)  ) \
                              .when(col('map.isTradeReceivablesNet')==1,                  lit(29)  ) \
                              .when(col('map.isPropertyPlantandEquipmentNet')==1,         lit(30)  ) \
                              .when(col('map.isIntangibleAssetsAcquired')==1,             lit(31)  ) \
                              .when(col('map.isAmortizationExpense')==1,                  lit(32)  ) \
                              .when(col('map.isDepreciationExpense')==1,                  lit(33)  ) \
                              .when(col('map.isHistoricalCostsIntangibleAcquired')==1,    lit(34)  ) \
                              .when(col('map.isNetInvestmentPPE')==1,                     lit(35)  ) \
                              .when(col('map.isPersonnelExpense')==1,                     lit(36)  ) \
                              .when(col('map.isRepairAndMaintenanceExpense')==1,          lit(37)  ) \
                              .when(col('map.isCalculationRelevantWagesAndSalaries')==1,  lit(38)  ) \
                              .when(col('map.isSalaries')==1,                             lit(39)  ) \
                              .when(col('map.isEmployerShareSocialSecuritySalaries')==1,  lit(41)  ) \
                              .when(col('map.isWages')==1,                                lit(42)  ) \
                              .when(col('map.isEmployerShareSocialSecurityWages')==1,     lit(43)  ) \
                              .when(col('map.isNetIncomeLoss')==1,                        lit(44)  ) \
                              .when(col('map.isIncomeTax')==1,                            lit(45)  ) \
                              .when(col('map.isDeferredIncomeTax')==1,                    lit(46)  ) \
                              .when(col('map.isOtherTax')==1,                             lit(47)  ) \
                              .when(col('map.isOtheroperatingexpenses')==1,               lit(48)  ) \
                              .when(col('map.isNetInvestmentPropertyPlantAndEquipment')==1,          lit(49)  ) \
                              .when(col('map.isIntercompanyLoanIncome')==1,               lit(50)  ) \
                              .when(col('map.isIntercompanyLoanExpense')==1,              lit(51)  ) \
                              .when(col('map.isInterestIncomeRelatedCompaniesAtShortNotice')==1,          lit(52)  ) \
                              .when(col('map.isInterestExpenseAffiliatedCompaniesAtShortNotice')==1,      lit(53)  ) \
                              .when(col('map.isHistoricalCostsPPEAcquired')==1,          lit(54)  ) \
                              .when(col('map.isPPEAccumulatedDepreciation')==1,          lit(55)  ) \
                              .when(col('map.isAccumulatedAmortizationIntangible')==1,   lit(56)  ) \
                              .when(col('map.isOtherLiabilityTax')==1,                   lit(57)  ) \
                              .when(col('map.isFixedAssets')==1,                         lit(58)  ) \
                              .when(col('map.isInventories')==1,                         lit(59)  ) \
                              .when(col('map.isAdvancePaymentsOnInventories')==1,        lit(60)  ) \
                              .when(col('map.isOtherInventories')==1,                    lit(61)  ) \
                              .when(col('map.isAllowanceforOtherInventories')==1,        lit(62)  ) \
                              .when(col('map.isRawMaterialsExpense')==1,                 lit(63)  ) \
                              .when(col('map.isChangesInInventoriesFGandWIP')==1,        lit(64)  ) \
                              .when(col('map.isCurrentAsset')==1,                        lit(65)  ) \
                              .when(col('map.isCurrentLiabilities')==1,                  lit(66)  ) \
                              .when(col('map.isNetProfitorLoss')==1,                     lit(67)  ) \
                              .when(col('map.isEquity')==1,                              lit(68)  ) \
                              .when(col('map.isProfitBeforeInterestAndTaxes')==1,        lit(69)  ) \
                              .when(col('map.isProfitBeforeTaxes')==1,                   lit(70)  ) \
                              .when(col('map.isTradeARNet')==1,                          lit(71)  ) \
                              .when(col('map.isTradeAPIntercompany')==1,                 lit(72)  ) \
                              .when(col('map.isTradeAR')==1,                             lit(73)  ) \
                              .otherwise(lit(0))

    new_col_isInterCoFlag=when(col('prd2.productTypeClient').isNull(), lit('#NA#') ) \
                                  .otherwise(col('prd2.productTypeClient'))

    dr_key= Window.orderBy(lit('A'))

    df_fin_L3_TMP_GLAccount=gen_L2_DIM_Organization.alias('org2')\
                  .join(fin_L2_DIM_GLAccount.alias('glAcc'),on=['organizationUnitSurrogateKey'], how='inner')\
                  .join(fin_L1_STG_GLAccountMapping.alias('map'),on=['accountNumber'],how='left')\
                  .join(fin_L2_DIM_GLAccountText.alias('glAcctxt'),on=['glAccountSurrogateKey'],how='inner')\
                  .join(knw_L2_DIM_BusinessPartnerTypeDescription.alias('BPTD'),(\
                   (col('BPTD.accountMappingFlag')==lit(new_join_BPTD1))\
                    & (col('BPTD.masterDataFlag')==lit(new_join_BPTD2) )\
                    & (col('BPTD.transactionDataFlag')==lit(new_join_BPTD3) )\
                   ),how='inner')\
                  .join(knw_LK_CD_ReportingSetup.alias('repSetup'),\
                        (col('repSetup.companyCode')==col('org2.companyCode')),how='inner')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdintNA'),\
                        ((col('lkbdintNA.businessDatatype')==lit('Is InterCompany Flag'))\
                         & (col('lkbdintNA.targetLanguageCode')==col('repSetup.KPMGDataReportingLanguage'))\
                         & (col('lkbdintNA.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdintNA.sourceSystemValue')==lit('NA'))\
                        ),how='inner')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdint'),\
                        ((col('lkbdint.businessDatatype')==lit('Is InterCompany Flag'))\
                         & (col('lkbdint.targetLanguageCode').eqNullSafe(col('repSetup.KPMGDataReportingLanguage')))\
                         & (col('lkbdint.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdint.sourceSystemValue').eqNullSafe(col('glAcc.isInterCoFlag').cast('String')))\
                        ),how='left')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdAccNA'),\
                        ((col('lkbdAccNA.businessDatatype')==lit('Is Intercompany From Accountmapping'))\
                         & (col('lkbdAccNA.targetLanguageCode')==col('repSetup.KPMGDataReportingLanguage'))\
                         & (col('lkbdAccNA.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdAccNA.sourceSystemValue')==lit('NA'))\
                        ),how='inner')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdAcc'),\
                        ((col('lkbdAcc.businessDatatype')==lit('Is Intercompany From Accountmapping'))\
                         & (col('lkbdAcc.targetLanguageCode').eqNullSafe(col('repSetup.KPMGDataReportingLanguage')))\
                         & (col('lkbdAcc.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdAcc.sourceSystemValue').eqNullSafe(col('glAcc.isInterCoFromAccountMapping').cast('String')))\
                        ),how='left')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdMasNA'),\
                        ((col('lkbdMasNA.businessDatatype')==lit('Is InterCompany From MasterData'))\
                         & (col('lkbdMasNA.targetLanguageCode')==col('repSetup.KPMGDataReportingLanguage'))\
                         & (col('lkbdMasNA.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdMasNA.sourceSystemValue')==lit('NA'))\
                        ),how='inner')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdMas'),\
                        ((col('lkbdMas.businessDatatype')==lit('Is InterCompany From MasterData'))\
                         & (col('lkbdMas.targetLanguageCode').eqNullSafe(col('repSetup.KPMGDataReportingLanguage')))\
                         & (col('lkbdMas.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdMas.sourceSystemValue').eqNullSafe(col('glAcc.isInterCoFromMasterData').cast('String')))\
                        ),how='left')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdTrnNA'),\
                        ((col('lkbdTrnNA.businessDatatype')==lit('Is InterCompany From TransactionData'))\
                         & (col('lkbdTrnNA.targetLanguageCode')==col('repSetup.KPMGDataReportingLanguage'))\
                         & (col('lkbdTrnNA.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdTrnNA.sourceSystemValue')==lit('NA'))\
                        ),how='inner')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdTrn'),\
                        ((col('lkbdTrn.businessDatatype')==lit('Is InterCompany From TransactionData'))\
                         & (col('lkbdTrn.targetLanguageCode').eqNullSafe(col('repSetup.KPMGDataReportingLanguage')))\
                         & (col('lkbdTrn.targetERPSystemID')==lit(erpSystemIDGeneric))\
                         & (col('lkbdTrn.sourceSystemValue').eqNullSafe(col('glAcc.isInterCoFromTransactionData').cast('String')))\
                        ),how='left')\
                  .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdpos'),\
                        ((col('lkbdpos.businessDatatype')==lit('Allowed Posting Creation Type'))\
                        & (col('lkbdpos.targetLanguageCode')==col('repSetup.KPMGDataReportingLanguage'))\
                        & (col('lkbdpos.targetERPSystemID')==lit(erpSystemIDGeneric))\
                        & (col('lkbdpos.sourceSystemValue')==col('glAcc.isOnlyAutomatedPostingAllowed'))\
                        ),how='inner')\
                  .filter( ~(col('org2.companyCode').isin('#NA#','')) & (col('glAcc.accountCategory')=='#NA#') )\
                      .select(col('glAcc.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
                        ,col('org2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                        ,col('org2.chartOfAccounts').alias('chartOfAccount')\
                        ,col('glAcc.accountNumber').alias('accountNumber')\
                        ,col('glAcctxt.accountName').alias('accountName')\
                        ,col('glAcc.accountType').alias('accountType')\
                        ,col('glAcc.accountCategoryAudit').alias('accountCategoryAudit')\
                        ,lit(new_col_accountCategoryType).alias('accountCategoryType')\
                        #,lit(None).alias('accountCategoryTypeDescription')\
                        ,col('bptd.ouputText').alias('ouputText')\
                        ,coalesce(col('lkbdint.targetSystemValueDescription'),\
                                  col('lkbdintNA.targetSystemValueDescription')).alias('isInterCoFlag')\
                        ,coalesce(col('lkbdAcc.targetSystemValueDescription'),\
                                  col('lkbdAccNA.targetSystemValueDescription')).alias('isInterCoFromAccountMapping')\
                        ,coalesce(col('lkbdMas.targetSystemValueDescription'),\
                                  col('lkbdMasNA.targetSystemValueDescription')).alias('isInterCoFromMasterData')\
                        ,coalesce(col('lkbdTrn.targetSystemValueDescription'),\
                                  col('lkbdTrnNA.targetSystemValueDescription')).alias('isInterCoFromTransactionData')\
                        ,col('repSetup.KPMGDataReportingLanguage').alias('KPMGDataReportingLanguage')\
                        ,col('lkbdpos.targetSystemValueDescription').alias('allowedPostingType')\
                        ,when((col('glAcc.accountGroup').isNull()),'#NA#') \
                         .when(((col("glAcc.accountGroup")=='' )  ),'#NA#') \
                         .otherwise(col("glAcc.accountGroup")).alias('accountGroup')\
                        ,F.row_number().over(dr_key).alias("key_ID")\
                        )
    
    #Step2

    df_fin_L3_TMP_GLAccount_1=df_fin_L3_TMP_GLAccount.alias('gla3')\
              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt0'),\
                                ((col('bdt0.businessDatatype')==lit('GL Account Category Type'))\
                                 & (col('bdt0.sourceSystemValue')==col('gla3.accountCategoryType'))\
                                 & (col('bdt0.sourceERPSystemID')==lit(erpSystemIDGeneric))\
                                 & (col('bdt0.targetERPSystemID')==lit(erpSystemIDGeneric))\
                                 & (col('bdt0.targetLanguageCode')==lit(reportingLanguage).cast('String'))\
                                ),how='inner')\
              .select(col('gla3.key_ID').alias('key_ID')\
                      ,col('bdt0.targetSystemValueDescription').alias('accountCategoryTypeDescription'))    

    new_col_accountCategoryTypeDesc=when(col('gla2.accountCategoryTypeDescription').isNotNull()\
                                         , col('gla2.accountCategoryTypeDescription') ) \
                                         .otherwise(lit(None))

    df_fin_L3_TMP_GLAccount_2=df_fin_L3_TMP_GLAccount.alias('gla1')\
              .join(df_fin_L3_TMP_GLAccount_1.alias('gla2'),\
                                ((col('gla1.key_ID').eqNullSafe(col('gla2.key_ID'))) ),how='left')\
              .select("gla1.*",lit(new_col_accountCategoryTypeDesc).alias('accountCategoryTypeDescription'))
    #Step3    

    join_kna_filter=[col('glafh.identifier_01'),\
                     col('glafh.identifier_02'),\
                     col('glafh.identifier_03'),\
                     col('glafh.identifier_04'),\
                     col('glafh.identifier_05'),\
                     col('glafh.identifier_06'),\
                     col('glafh.identifier_07'),\
                     col('glafh.identifier_08'),\
                     col('glafh.identifier_09')]
    
    new_col_accountCategoryL1 = when(col("glafh.accountName_01").isNotNull(), col("glafh.accountName_01")) \
                                .otherwise( when(col("cat.accountCategory").isNull(), lit('Un-Assigned'))\
                                            .otherwise(lit('Not Mapped to Leaf Category'))\
                                          )
    new_col_accountCategoryL2=when(col('glafh.accountName_02').isNotNull(), col('glafh.accountName_02')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))
    new_col_accountCategoryL3=when(col('glafh.accountName_03').isNotNull(), col('glafh.accountName_03')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))
    new_col_accountCategoryL4=when(col('glafh.accountName_04').isNotNull(), col('glafh.accountName_04')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))
    new_col_accountCategoryL5=when(col('glafh.accountName_05').isNotNull(), col('glafh.accountName_05')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))
    new_col_accountCategoryL6=when(col('glafh.accountName_06').isNotNull(), col('glafh.accountName_06')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))
    new_col_accountCategoryL7=when(col('glafh.accountName_07').isNotNull(), col('glafh.accountName_07')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))
    new_col_accountCategoryL8=when(col('glafh.accountName_08').isNotNull(), col('glafh.accountName_08')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))
    new_col_accountCategoryL9=when(col('glafh.accountName_09').isNotNull(), col('glafh.accountName_09')) \
                            .otherwise(concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')))

    new_col_acntCtgIdentifierL1 = when(col('glafh.identifier_01').isNotNull(), col('glafh.identifier_01')) \
                                .otherwise( when(col('cat.accountCategory').isNull(), lit(9998))\
                                            .otherwise(lit(9999))\
                                          )
    new_col_acntCtgIdentifierL2=when(col('glafh.identifier_02').isNotNull(), col('glafh.identifier_02')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgIdentifierL3=when(col('glafh.identifier_03').isNotNull(), col('glafh.identifier_03')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgIdentifierL4=when(col('glafh.identifier_04').isNotNull(), col('glafh.identifier_04')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgIdentifierL5=when(col('glafh.identifier_05').isNotNull(), col('glafh.identifier_05')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgIdentifierL6=when(col('glafh.identifier_06').isNotNull(), col('glafh.identifier_06')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgIdentifierL7=when(col('glafh.identifier_07').isNotNull(), col('glafh.identifier_07')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgIdentifierL8=when(col('glafh.identifier_08').isNotNull(), col('glafh.identifier_08')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgIdentifierL9=when(col('glafh.identifier_09').isNotNull(), col('glafh.identifier_09')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))

    new_col_acntCtgSortOrderL1 = when(col('glafh.sortOrder_01').isNotNull(), col('glafh.identifier_01')) \
                                .otherwise( when(col('cat.accountCategory').isNull(), lit(9998))\
                                            .otherwise(lit(9999))\
                                          )
    new_col_acntCtgSortOrderL2=when(col('glafh.sortOrder_02').isNotNull(), col('glafh.sortOrder_02')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgSortOrderL3=when(col('glafh.sortOrder_03').isNotNull(), col('glafh.sortOrder_03')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgSortOrderL4=when(col('glafh.sortOrder_04').isNotNull(), col('glafh.sortOrder_04')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgSortOrderL5=when(col('glafh.sortOrder_05').isNotNull(), col('glafh.sortOrder_05')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgSortOrderL6=when(col('glafh.sortOrder_06').isNotNull(), col('glafh.sortOrder_06')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgSortOrderL7=when(col('glafh.sortOrder_07').isNotNull(), col('glafh.sortOrder_07')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgSortOrderL8=when(col('glafh.sortOrder_08').isNotNull(), col('glafh.sortOrder_08')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))
    new_col_acntCtgSortOrderL9=when(col('glafh.sortOrder_09').isNotNull(), col('glafh.sortOrder_09')) \
                            .otherwise(concat(col('glac2.accountNumber'),col('glac2.accountName')))

    df_fin_L3_TMP_GLAccount=df_fin_L3_TMP_GLAccount_2.alias('glac2')\
              .join(fin_L1_STG_GLAccountMapping.alias('map'),on=['accountNumber'],how='left')\
              .join(fin_L1_STG_GLAccountCategoryMaster.alias('cat'),\
                           (col('cat.glAccountCategoryMasterSurrogateKey').eqNullSafe(col('map.accountCategoryID')))\
                           ,how='left')\
              .join(fin_L1_STG_GLAccountFlattenHierarchy.alias('glafh'),\
                           (col('glafh.leafID').eqNullSafe(col('map.accountCategoryID')))\
                           ,how='left')\
              .join(knw_LK_CD_KnowledgeAccountSnapshot.alias('kna'),(\
                           (col('kna.knowledgeAccountID').isin(join_kna_filter))\
                            & (col('kna.knowledgeAccountType')==lit('Account'))\
                            ),how='left')\
              .join(knw_LK_CD_ProcessToKnowledgeAccountsRelationSnapshot.alias('prel'),\
                           (col('prel.knowledgeAccountID').eqNullSafe(col('kna.knowledgeAccountID')))\
                           ,how='left')\
              .join(knw_LK_CD_Process.alias('pro'),\
                           (col('pro.processId').eqNullSafe(col('prel.processID'))),how='left')\
              .join(knw_LK_CD_FinancialStructureDetailSnapshot.alias('fsd'),\
                           (col('fsd.knowledgeAccountNodeID').eqNullSafe(col('kna.knowledgeAccountID')))\
                           ,how='left')\
              .join(knw_LK_CD_KnowledgeAccountSnapshot.alias('fsc'),\
                               col('fsc.knowledgeAccountID').eqNullSafe(col('fsd.knowledgeAccountParentID')),how='left')\
              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbddupNA'),\
                            ((col('lkbddupNA.businessDatatype')==lit('Is Duplicate Knowledge Account'))\
                             & (col('lkbddupNA.targetLanguageCode')==col('glac2.KPMGDataReportingLanguage'))\
                             & (col('lkbddupNA.targetERPSystemID')==lit(erpSystemIDGeneric))\
                             & (col('lkbddupNA.sourceSystemValue')==lit('NA'))\
                            ),how='inner')\
               .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbddup'),\
                            ((col('lkbddup.businessDatatype')==lit('Is Duplicate Knowledge Account'))\
                             & (col('lkbddup.targetLanguageCode').eqNullSafe(col('glac2.KPMGDataReportingLanguage')))\
                             & (col('lkbddup.targetERPSystemID')==lit(erpSystemIDGeneric))\
                             & (col('lkbddup.sourceSystemValue').eqNullSafe(col('kna.isDuplicateKnowledgeAccount').cast('String')))\
                            ),how='left')\
              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdunqNA'),\
                            ((col('lkbdunqNA.businessDatatype')==lit('Is Unique Account'))\
                             & (col('lkbdunqNA.targetLanguageCode')==col('glac2.KPMGDataReportingLanguage'))\
                             & (col('lkbdunqNA.targetERPSystemID')==lit(erpSystemIDGeneric))\
                             & (col('lkbdunqNA.sourceSystemValue')==lit('NA'))\
                            ),how='inner')\
              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('lkbdunq'),\
                          ((col('lkbddup.businessDatatype')==lit('Is Unique Account'))\
                           & (col('lkbddup.targetLanguageCode').eqNullSafe(col('glac2.KPMGDataReportingLanguage')))\
                           & (col('lkbddup.targetERPSystemID')==lit(erpSystemIDGeneric))\
                           & (col('lkbddup.sourceSystemValue').eqNullSafe(col('kna.isUnique').cast('String')))\
                          ),how='left')\
             .select(col('glac2.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
             ,col('glac2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
             ,concat(col('glac2.accountNumber'),lit(' ('),col('glac2.accountName'),lit(')')).alias('accountNumberName')\
             ,col('glac2.chartOfAccount').alias('chartOfAccount')\
             ,(when(col('cat.accountCategory').isNotNull(), col('cat.accountCategory'))\
                      .otherwise(lit('UnAssigned'))).alias('AccountCategoryName')\
             ,(when(col('cat.identifier').isNotNull(), col('cat.identifier'))\
                     .otherwise(lit(999))).alias('AccountCategoryIdentifier')\
             ,(when(col('cat.sortOrder').isNotNull(), col('cat.sortOrder'))\
                     .otherwise(lit(999))).alias('AccountCategorySortOrder')\
             ,coalesce(col('kna.displayName'),col('kna.knowledgeAccountName'),lit('UnAssigned')).alias('knowledgeAccountName')\
             ,(when(col('kna.knowledgeAccountID').isNotNull(), col('kna.knowledgeAccountID')).otherwise(lit('999')))\
                     .alias('knowledgeAccountIdentifier')\
             ,(when(col('fsd.sortOrder').isNotNull(), col('fsd.sortOrder'))\
                     .otherwise(lit(999))).alias('knowledgeAccountSortOrder')\
             ,coalesce(col('fsc.displayName'),col('fsc.knowledgeAccountName'),lit('UnAssigned'))\
                     .alias('financialStatementCaptionName')\
             ,(when(col('fsc.knowledgeAccountID').isNotNull(), col('fsc.knowledgeAccountID'))\
                      .otherwise(lit('999'))).alias('financialStatementCaptionIdentifier')\
             ,(when(col('fsc.sortOrder').isNotNull(),col('fsc.sortOrder'))\
                      .otherwise(lit(999))).alias('financialStatementCaptionSortOrder')\
             ,lit(new_col_accountCategoryL1).alias('accountCategoryL1')\
             ,lit(new_col_accountCategoryL2).alias('accountCategoryL2')\
             ,lit(new_col_accountCategoryL3).alias('accountCategoryL3')\
             ,lit(new_col_accountCategoryL4).alias('accountCategoryL4')\
             ,lit(new_col_accountCategoryL5).alias('accountCategoryL5')\
             ,lit(new_col_accountCategoryL6).alias('accountCategoryL6')\
             ,lit(new_col_accountCategoryL7).alias('accountCategoryL7')\
             ,lit(new_col_accountCategoryL8).alias('accountCategoryL8')\
             ,lit(new_col_accountCategoryL9).alias('accountCategoryL9')\
             ,col('glac2.accountNumber').alias('accountNumber')\
             ,col('glac2.accountType').alias('accountType')\
             ,lit(new_col_acntCtgIdentifierL1).alias('accountCategoryIdentifierL1')\
             ,lit(new_col_acntCtgIdentifierL2).alias('accountCategoryIdentifierL2')\
             ,lit(new_col_acntCtgIdentifierL3).alias('accountCategoryIdentifierL3')\
             ,lit(new_col_acntCtgIdentifierL4).alias('accountCategoryIdentifierL4')\
             ,lit(new_col_acntCtgIdentifierL5).alias('accountCategoryIdentifierL5')\
             ,lit(new_col_acntCtgIdentifierL6).alias('accountCategoryIdentifierL6')\
             ,lit(new_col_acntCtgIdentifierL7).alias('accountCategoryIdentifierL7')\
             ,lit(new_col_acntCtgIdentifierL8).alias('accountCategoryIdentifierL8')\
             ,lit(new_col_acntCtgIdentifierL9).alias('accountCategoryIdentifierL9')\
             ,lit(new_col_acntCtgSortOrderL1).alias('accountCategorySortOrderL1')\
             ,lit(new_col_acntCtgSortOrderL2).alias('accountCategorySortOrderL2')\
             ,lit(new_col_acntCtgSortOrderL3).alias('accountCategorySortOrderL3')\
             ,lit(new_col_acntCtgSortOrderL4).alias('accountCategorySortOrderL4')\
             ,lit(new_col_acntCtgSortOrderL5).alias('accountCategorySortOrderL5')\
             ,lit(new_col_acntCtgSortOrderL6).alias('accountCategorySortOrderL6')\
             ,lit(new_col_acntCtgSortOrderL7).alias('accountCategorySortOrderL7')\
             ,lit(new_col_acntCtgSortOrderL8).alias('accountCategorySortOrderL8')\
             ,lit(new_col_acntCtgSortOrderL9).alias('accountCategorySortOrderL9')\
             ,col('glac2.accountCategoryAudit').alias('accountCategoryAudit')\
             ,col('glac2.accountCategoryType').alias('accountCategoryType')\
             ,(when(col('glac2.accountCategoryTypeDescription').isNotNull(), col('glac2.accountCategoryTypeDescription'))\
               .otherwise(lit(''))).alias('accountCategoryTypeDescription')\
             ,col('glac2.isInterCoFlag').alias('interCoFlag')\
             ,col('glac2.isInterCoFromAccountMapping').alias('interCoFromAccountMapping')\
             ,col('glac2.isInterCoFromMasterData').alias('interCoFromMasterData')\
             ,col('glac2.isInterCoFromTransactionData').alias('interCoFromTransactionData')\
             ,col('glac2.ouputText').alias('businessPartnerType')\
             ,(when(col('pro.processName').isNotNull(), col('pro.processName'))\
                        .otherwise(lit('#NA#'))).alias('accountProcess')\
              ,(when(col('pro.sortOrder').isNotNull(), col('pro.sortOrder'))\
                       .otherwise(lit(999.99))).alias('accountProcessSortOrder')\
              ,(when(col('lkbddup.targetSystemValueDescription').isNotNull(), col('lkbddup.targetSystemValueDescription'))\
                       .otherwise(col('lkbddupNA.targetSystemValueDescription'))).alias('duplicateAccount')\
              ,(when(col('lkbdunq.targetSystemValueDescription').isNotNull(), col('lkbdunq.targetSystemValueDescription'))\
                        .otherwise(col('lkbdunqNA.targetSystemValueDescription'))).alias('uniqueAccount')\
              ,(when(col('map.isScopedforBifurcation')== True, lit('Is Scoped Account - Yes'))\
                     .otherwise(lit('Is Scoped Account - No'))).alias('scopedAccount')\
              ,col('glac2.allowedPostingType').alias('allowedPostingType')\
              ,(when(col('map.isRevenue').isNotNull(), col('map.isRevenue'))\
                       .otherwise(lit(False))).alias('isRevenue')\
              ,(when(col('map.isBank').isNotNull(), col('map.isCash'))\
                       .otherwise(lit(False))).alias('isBank')\
              ,(when(col('map.isCash').isNotNull(), col('map.isCash'))\
                       .otherwise(lit(False))).alias('isCash')\
              ,(when(col('map.isTradeAR').isNotNull(), col('map.isTradeAR'))\
                       .otherwise(lit(False))).alias('isTradeAR')\
              ,(when(col('map.isTradeAP').isNotNull(), col('map.isTradeAP'))\
                       .otherwise(lit(False))).alias('isTradeAP')\
              ,(when(col('map.isIntercompany').isNotNull(), col('map.isIntercompany'))\
                       .otherwise(lit(False))).alias('isIntercompany')\
              ,(when(col('map.is3rdParty').isNotNull(), col('map.is3rdParty'))\
                       .otherwise(lit(False))).alias('is3rdParty')\
              ,(when(col('map.isExpense').isNotNull(), col('map.isExpense'))\
                       .otherwise(lit(False))).alias('isExpense')\
              ,(when(col('map.isInterest').isNotNull(), col('map.isInterest'))\
                       .otherwise(lit(False))).alias('isInterest')\
              ,(when(col('map.isOtherPayables').isNotNull(), col('map.isOtherPayables'))\
                       .otherwise(lit(False))).alias('isOtherPayables')\
              ,(when(col('map.isGRIR').isNotNull(), col('map.isGRIR'))\
                       .otherwise(lit(False))).alias('isGRIR')\
              ,(when(col('map.isIncome').isNotNull(), col('map.isIncome'))\
                       .otherwise(lit(False))).alias('isIncome')\
              ,(when(col('map.isLiability').isNotNull(), col('map.isLiability'))\
                       .otherwise(lit(False))).alias('isLiability')\
              ,(when(col('map.isAccrued').isNotNull(), col('map.isAccrued'))\
                       .otherwise(lit(False))).alias('isAccrued')\
              ,(when(col('map.isAssets').isNotNull(), col('map.isAssets'))\
                       .otherwise(lit(False))).alias('isAssets')\
              ,(when(col('map.isWorkinProgressGross').isNotNull(), col('map.isWorkinProgressGross'))\
                       .otherwise(lit(False))).alias('isWorkinProgressGross')\
              ,(when(col('map.isManufacturedFinishedGoodsGross').isNotNull(), col('map.isManufacturedFinishedGoodsGross'))\
                       .otherwise(lit(False))).alias('isManufacturedFinishedGoodsGross')\
              ,(when(col('map.isPurchasedFinishedGoodsGross').isNotNull(), col('map.isPurchasedFinishedGoodsGross'))\
                       .otherwise(lit(False))).alias('isPurchasedFinishedGoodsGross')\
              ,(when(col('map.isCostofSales').isNotNull(), col('map.isCostofSales'))\
                       .otherwise(lit(False))).alias('isCostofSales')\
              ,(when(col('map.isRawMaterialsGross').isNotNull(), col('map.isRawMaterialsGross'))\
                       .otherwise(lit(False))).alias('isRawMaterialsGross')\
              ,(when(col('map.isAllowanceForWorkInProgress').isNotNull(), col('map.isAllowanceForWorkInProgress'))\
                       .otherwise(lit(False))).alias('isAllowanceForWorkInProgress')\
              ,(when(col('map.isAllowanceForRawMaterialsAndFactorySupplies').isNotNull(),\
                        col('map.isAllowanceForRawMaterialsAndFactorySupplies'))\
                       .otherwise(lit(False))).alias('isAllowanceForRawMaterialsAndFactorySupplies')\
              ,(when(col('map.isAllowanceforManufacturedFinishedGoods').isNotNull(), col('map.isAllowanceforManufacturedFinishedGoods'))\
                       .otherwise(lit(False))).alias('isAllowanceforManufacturedFinishedGoods')\
              ,(when(col('map.isAllowanceforPurchasedFinishedGoods').isNotNull(), col('map.isAllowanceforPurchasedFinishedGoods'))\
                       .otherwise(lit(False))).alias('isAllowanceforPurchasedFinishedGoods')\
              ,(when(col('map.isSalesDiscountsandVolumeRebates').isNotNull(), col('map.isSalesDiscountsandVolumeRebates'))\
                       .otherwise(lit(False))).alias('isSalesDiscountsandVolumeRebates')\
              ,(when(col('map.isRevenueFromSalesOfGoods').isNotNull(), col('map.isRevenueFromSalesOfGoods'))\
                       .otherwise(lit(False))).alias('isRevenueFromSalesOfGoods')\
              ,(when(col('map.isServicesRevenue').isNotNull(), col('map.isServicesRevenue'))\
                       .otherwise(lit(False))).alias('isServicesRevenue')\
              ,(when(col('map.isSalesReturns').isNotNull(), col('map.isSalesReturns'))\
                       .otherwise(lit(False))).alias('isSalesReturns')\
              ,(when(col('map.isBadDebtExpense').isNotNull(), col('map.isBadDebtExpense'))\
                       .otherwise(lit(False))).alias('isBadDebtExpense')\
              ,(when(col('map.isAllowanceforDoubtfulAccounts').isNotNull(), col('map.isAllowanceforDoubtfulAccounts'))\
                       .otherwise(lit(False))).alias('isAllowanceforDoubtfulAccounts')\
              ,(when(col('map.isTradeReceivablesNet').isNotNull(), col('map.isTradeReceivablesNet'))\
                       .otherwise(lit(False))).alias('isTradeReceivablesNet')\
              ,(when(col('map.isPropertyPlantandEquipmentNet').isNotNull(), col('map.isPropertyPlantandEquipmentNet'))\
                       .otherwise(lit(False))).alias('isPropertyPlantandEquipmentNet')\
              ,(when(col('map.isIntangibleAssetsAcquired').isNotNull(), col('map.isIntangibleAssetsAcquired'))\
                       .otherwise(lit(False))).alias('isIntangibleAssetsAcquired')\
              ,(when(col('map.isAmortizationExpense').isNotNull(), col('map.isAmortizationExpense'))\
                       .otherwise(lit(False))).alias('isAmortizationExpense')\
              ,(when(col('map.isDepreciationExpense').isNotNull(), col('map.isDepreciationExpense'))\
                       .otherwise(lit(False))).alias('isDepreciationExpense')\
              ,(when(col('map.isHistoricalCostsIntangibleAcquired').isNotNull(), col('map.isHistoricalCostsIntangibleAcquired'))\
                       .otherwise(lit(False))).alias('isHistoricalCostsIntangibleAcquired')\
              ,(when(col('map.isNetInvestmentPPE').isNotNull(), col('map.isNetInvestmentPPE'))\
                       .otherwise(lit(False))).alias('isNetInvestmentPPE')\
              ,(when(col('map.isPersonnelExpense').isNotNull(), col('map.isPersonnelExpense'))\
                       .otherwise(lit(False))).alias('isPersonnelExpense')\
              ,(when(col('map.isRepairAndMaintenanceExpense').isNotNull(), col('map.isRepairAndMaintenanceExpense'))\
                       .otherwise(lit(False))).alias('isRepairAndMaintenanceExpense')\
              ,(when(col('map.isCalculationRelevantWagesAndSalaries').isNotNull(), col('map.isCalculationRelevantWagesAndSalaries'))\
                       .otherwise(lit(False))).alias('isCalculationRelevantWagesAndSalaries')\
              ,(when(col('map.isSalaries').isNotNull(), col('map.isSalaries'))\
                       .otherwise(lit(False))).alias('isSalaries')\
              ,(when(col('map.isEmployerShareSocialSecuritySalaries').isNotNull(), col('map.isEmployerShareSocialSecuritySalaries'))\
                       .otherwise(lit(False))).alias('isEmployerShareSocialSecuritySalaries')\
              ,(when(col('map.isWages').isNotNull(), col('map.isWages'))\
                       .otherwise(lit(False))).alias('isWages')\
              ,(when(col('map.isEmployerShareSocialSecurityWages').isNotNull(), col('map.isEmployerShareSocialSecurityWages'))\
                       .otherwise(lit(False))).alias('isEmployerShareSocialSecurityWages')\
              ,(when(col('map.isNetIncomeLoss').isNotNull(), col('map.isNetIncomeLoss'))\
                       .otherwise(lit(False))).alias('isNetIncomeLoss')\
              ,(when(col('map.isIncomeTax').isNotNull(), col('map.isIncomeTax'))\
                       .otherwise(lit(False))).alias('isIncomeTax')\
              ,(when(col('map.isDeferredIncomeTax').isNotNull(), col('map.isDeferredIncomeTax'))\
                       .otherwise(lit(False))).alias('isDeferredIncomeTax')\
              ,(when(col('map.isOtherTax').isNotNull(), col('map.isOtherTax'))\
                       .otherwise(lit(False))).alias('isOtherTax')\
              ,(when(col('map.isOtherOperatingExpenses').isNotNull(), col('map.isOtherOperatingExpenses'))\
                       .otherwise(lit(False))).alias('isOtherOperatingExpenses')\
              ,(when(col('map.isNetInvestmentPropertyPlantAndEquipment').isNotNull(), col('map.isNetInvestmentPropertyPlantAndEquipment'))\
                       .otherwise(lit(False))).alias('isNetInvestmentPropertyPlantAndEquipment')\
              ,(when(col('map.isIntercompanyLoanIncome').isNotNull(), col('map.isIntercompanyLoanIncome'))\
                       .otherwise(lit(False))).alias('isIntercompanyLoanIncome')\
              ,(when(col('map.isIntercompanyLoanExpense').isNotNull(), col('map.isIntercompanyLoanExpense'))\
                       .otherwise(lit(False))).alias('isIntercompanyLoanExpense')\
              ,(when(col('map.isInterestIncomeRelatedCompaniesAtShortNotice').isNotNull(),\
                       col('map.isInterestIncomeRelatedCompaniesAtShortNotice'))\
                       .otherwise(lit(False))).alias('isInterestIncomeRelatedCompaniesAtShortNotice')\
              ,(when(col('map.isInterestExpenseAffiliatedCompaniesAtShortNotice').isNotNull(),\
                        col('map.isInterestExpenseAffiliatedCompaniesAtShortNotice'))\
                       .otherwise(lit(False))).alias('isInterestExpenseAffiliatedCompaniesAtShortNotice')\
              ,(when(col('map.isHistoricalCostsPPEAcquired').isNotNull(), col('map.isHistoricalCostsPPEAcquired'))\
                       .otherwise(lit(False))).alias('isHistoricalCostsPPEAcquired')\
              ,(when(col('map.isPPEAccumulatedDepreciation').isNotNull(), col('map.isPPEAccumulatedDepreciation'))\
                       .otherwise(lit(False))).alias('isPPEAccumulatedDepreciation')\
              ,(when(col('map.isAccumulatedAmortizationIntangible').isNotNull(), col('map.isAccumulatedAmortizationIntangible'))\
                       .otherwise(lit(False))).alias('isAccumulatedAmortizationIntangible')\
              ,(when(col('map.isOtherLiabilityTax').isNotNull(), col('map.isOtherLiabilityTax'))\
                       .otherwise(lit(False))).alias('isOtherLiabilityTax')\
              ,(when(col('map.isFixedAssets').isNotNull(), col('map.isFixedAssets'))\
                       .otherwise(lit(False))).alias('isFixedAssets')\
              ,(when(col('map.isInventories').isNotNull(), col('map.isInventories'))\
                       .otherwise(lit(False))).alias('isInventories')\
              ,(when(col('map.isAdvancePaymentsOnInventories').isNotNull(), col('map.isAdvancePaymentsOnInventories'))\
                       .otherwise(lit(False))).alias('isAdvancePaymentsOnInventories')\
              ,(when(col('map.isOtherInventories').isNotNull(), col('map.isOtherInventories'))\
                       .otherwise(lit(False))).alias('isOtherInventories')\
              ,(when(col('map.isAllowanceforOtherInventories').isNotNull(), col('map.isAllowanceforOtherInventories'))\
                       .otherwise(lit(False))).alias('isAllowanceforOtherInventories')\
              ,(when(col('map.isRawMaterialsExpense').isNotNull(), col('map.isRawMaterialsExpense'))\
                       .otherwise(lit(False))).alias('isRawMaterialsExpense')\
              ,(when(col('map.isChangesInInventoriesFGandWIP').isNotNull(), col('map.isChangesInInventoriesFGandWIP'))\
                       .otherwise(lit(False))).alias('isChangesInInventoriesFGandWIP')\
              ,(when(col('map.isCurrentAsset').isNotNull(), col('map.isCurrentAsset'))\
                       .otherwise(lit(False))).alias('isCurrentAsset')\
              ,(when(col('map.isCurrentLiabilities').isNotNull(), col('map.isCurrentLiabilities'))\
                       .otherwise(lit(False))).alias('isCurrentLiabilities')\
              ,(when(col('map.isNetProfitorLoss').isNotNull(), col('map.isNetProfitorLoss'))\
                       .otherwise(lit(False))).alias('isNetProfitorLoss')\
              ,(when(col('map.isEquity').isNotNull(), col('map.isEquity'))\
                       .otherwise(lit(False))).alias('isEquity')\
              ,(when(col('map.isProfitBeforeInterestAndTaxes').isNotNull(), col('map.isProfitBeforeInterestAndTaxes'))\
                       .otherwise(lit(False))).alias('isProfitBeforeInterestAndTaxes')\
              ,(when(col('map.isProfitBeforeTaxes').isNotNull(), col('map.isProfitBeforeTaxes'))\
                       .otherwise(lit(False))).alias('isProfitBeforeTaxes')\
              ,(when(col('map.isTradeARNet').isNotNull(), col('map.isTradeARNet'))\
                       .otherwise(lit(False))).alias('isTradeARNet')\
              ,(when(col('map.isTradeARIntercompany').isNotNull(), col('map.isTradeARIntercompany'))\
                       .otherwise(lit(False))).alias('isTradeARIntercompany')\
              ,(when(col('map.isTradeAPIntercompany').isNotNull(), col('map.isTradeAPIntercompany'))\
                       .otherwise(lit(False))).alias('isTradeAPIntercompany')\
               ,(when(col('glac2.accountGroup').isNotNull(), col('glac2.accountGroup'))\
                       .otherwise(lit('#NA#'))).alias('accountGroup')\
              )
 
    #Step4

    unknown_list =  [[0, 0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE',1000,'NONE','NONE',1000,'NONE'\
                 ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'\
                 ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE',0,'NONE','NONE','NONE'\
                 ,'NONE','NONE','NONE','NONE',0.00,'Duplicate Account-No','Unique Account-No','Is Scoped Account - No','NONE',True,True\
                 ,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True\
                 ,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True\
                 ,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,True,'NONE']]	
    unknown_df = spark.createDataFrame(unknown_list)
    df_fin_L3_TMP_GLAccount = df_fin_L3_TMP_GLAccount.union(unknown_df)
    
    #Step5
    dr_key= Window.orderBy(lit('A'))

    df_GLAccountCategoryType=knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt0')\
             .filter((col('bdt0.businessDatatype')=="GL Account Category Type")\
                      & (col('bdt0.sourceERPSystemID')==erpSystemIDGeneric)\
                      & (col('bdt0.targetERPSystemID')==erpSystemIDGeneric)\
                      & (col('bdt0.targetLanguageCode')==reportingLanguage)\
                    )\
             .select(((F.row_number().over(dr_key))*-1).alias('glAccountSurrogateKey')\
             ,lit('0').alias('organizationUnitSurrogateKey')\
             ,lit('NONE').alias('accountNumberName')\
             ,lit('NONE').alias('chartOfAccount')\
             ,lit('NONE').alias('AccountCategoryName')\
             ,lit('NONE').alias('AccountCategoryIdentifier')\
             ,lit('NONE').alias('AccountCategorySortOrder')\
             ,lit('NONE').alias('knowledgeAccountName')\
             ,lit('NONE').alias('knowledgeAccountIdentifier')\
             ,lit(1000).alias('knowledgeAccountSortOrder')\
             ,lit('NONE').alias('financialStatementCaptionName')\
             ,lit('NONE').alias('financialStatementCaptionIdentifier')\
             ,lit(1000).alias('financialStatementCaptionSortOrder')\
             ,lit('NONE').alias('accountCategoryL1')\
             ,lit('NONE').alias('accountCategoryL2')\
             ,lit('NONE').alias('accountCategoryL3')\
             ,lit('NONE').alias('accountCategoryL4')\
             ,lit('NONE').alias('accountCategoryL5')\
             ,lit('NONE').alias('accountCategoryL6')\
             ,lit('NONE').alias('accountCategoryL7')\
             ,lit('NONE').alias('accountCategoryL8')\
             ,lit('NONE').alias('accountCategoryL9')\
             ,lit('NONE').alias('accountNumber')\
             ,lit('NONE').alias('accountType')\
             ,lit('NONE').alias('accountCategoryIdentifierL1')\
             ,lit('NONE').alias('accountCategoryIdentifierL2')\
             ,lit('NONE').alias('accountCategoryIdentifierL3')\
             ,lit('NONE').alias('accountCategoryIdentifierL4')\
             ,lit('NONE').alias('accountCategoryIdentifierL5')\
             ,lit('NONE').alias('accountCategoryIdentifierL6')\
             ,lit('NONE').alias('accountCategoryIdentifierL7')\
             ,lit('NONE').alias('accountCategoryIdentifierL8')\
             ,lit('NONE').alias('accountCategoryIdentifierL9')\
             ,lit('NONE').alias('accountCategorySortOrderL1')\
             ,lit('NONE').alias('accountCategorySortOrderL2')\
             ,lit('NONE').alias('accountCategorySortOrderL3')\
             ,lit('NONE').alias('accountCategorySortOrderL4')\
             ,lit('NONE').alias('accountCategorySortOrderL5')\
             ,lit('NONE').alias('accountCategorySortOrderL6')\
             ,lit('NONE').alias('accountCategorySortOrderL7')\
             ,lit('NONE').alias('accountCategorySortOrderL8')\
             ,lit('NONE').alias('accountCategorySortOrderL9')\
             ,lit('NONE').alias('accountCategoryAudit')\
             ,col('bdt0.targetSystemValue').alias('accountCategoryType')\
             ,col('bdt0.targetSystemValueDescription').alias('accountCategoryTypeDescription')\
             ,lit('NONE').alias('interCoFlag')\
             ,lit('NONE').alias('interCoFromAccountMapping')\
             ,lit('NONE').alias('interCoFromMasterData')\
             ,lit('NONE').alias('interCoFromTransactionData')\
             ,lit('NONE').alias('businessPartnerType')\
             ,lit('NONE').alias('accountProcess')\
             ,lit(00.0).alias('accountProcessSortOrder')\
             ,lit('Duplicate Account-No').alias('duplicateAccount')\
             ,lit('Unique Account-No').alias('uniqueAccount')\
             ,lit('Is Scoped Account - No').alias('scopedAccount')\
             ,lit('NONE').alias('allowedPostingType')\
             ,lit(False).alias('isRevenue')\
             ,lit(False).alias('isBank')\
             ,lit(False).alias('isCash')\
             ,lit(False).alias('isTradeAR')\
             ,lit(False).alias('isTradeAP')\
             ,lit(False).alias('isIntercompany')\
             ,lit(False).alias('is3rdParty')\
             ,lit(False).alias('isExpense')\
             ,lit(False).alias('isInterest')\
             ,lit(False).alias('isOtherPayables')\
             ,lit(False).alias('isGRIR')\
             ,lit(False).alias('isIncome')\
             ,lit(False).alias('isLiability')\
             ,lit(False).alias('isAccrued')\
             ,lit(False).alias('isAssets')\
             ,lit(False).alias('isWorkinProgressGross')\
             ,lit(False).alias('isManufacturedFinishedGoodsGross')\
             ,lit(False).alias('isPurchasedFinishedGoodsGross')\
             ,lit(False).alias('isCostofSales')\
             ,lit(False).alias('isRawMaterialsGross')\
             ,lit(False).alias('isAllowanceForWorkInProgress')\
             ,lit(False).alias('isAllowanceForRawMaterialsAndFactorySupplies')\
             ,lit(False).alias('isAllowanceforManufacturedFinishedGoods')\
             ,lit(False).alias('isAllowanceforPurchasedFinishedGoods')\
             ,lit(False).alias('isSalesDiscountsandVolumeRebates')\
             ,lit(False).alias('isRevenueFromSalesOfGoods')\
             ,lit(False).alias('isServicesRevenue')\
             ,lit(False).alias('isSalesReturns')\
             ,lit(False).alias('isBadDebtExpense')\
             ,lit(False).alias('isAllowanceforDoubtfulAccounts')\
             ,lit(False).alias('isTradeReceivablesNet')\
             ,lit(False).alias('isPropertyPlantandEquipmentNet')\
             ,lit(False).alias('isIntangibleAssetsAcquired')\
             ,lit(False).alias('isAmortizationExpense')\
             ,lit(False).alias('isDepreciationExpense')\
             ,lit(False).alias('isHistoricalCostsIntangibleAcquired')\
             ,lit(False).alias('isNetInvestmentPPE')\
             ,lit(False).alias('isPersonnelExpense')\
             ,lit(False).alias('isRepairAndMaintenanceExpense')\
             ,lit(False).alias('isCalculationRelevantWagesAndSalaries')\
             ,lit(False).alias('isSalaries')\
             ,lit(False).alias('isEmployerShareSocialSecuritySalaries')\
             ,lit(False).alias('isWages')\
             ,lit(False).alias('isEmployerShareSocialSecurityWages')\
             ,lit(False).alias('isNetIncomeLoss')\
             ,lit(False).alias('isIncomeTax')\
             ,lit(False).alias('isDeferredIncomeTax')\
             ,lit(False).alias('isOtherTax')\
             ,lit(False).alias('isOtherOperatingExpenses')\
             ,lit(False).alias('isNetInvestmentPropertyPlantAndEquipment')\
             ,lit(False).alias('isIntercompanyLoanIncome')\
             ,lit(False).alias('isIntercompanyLoanExpense')\
             ,lit(False).alias('isInterestIncomeRelatedCompaniesAtShortNotice')\
             ,lit(False).alias('isInterestExpenseAffiliatedCompaniesAtShortNotice')\
             ,lit(False).alias('isHistoricalCostsPPEAcquired')\
             ,lit(False).alias('isPPEAccumulatedDepreciation')\
             ,lit(False).alias('isAccumulatedAmortizationIntangible')\
             ,lit(False).alias('isOtherLiabilityTax')\
             ,lit(False).alias('isFixedAssets')\
             ,lit(False).alias('isInventories')\
             ,lit(False).alias('isAdvancePaymentsOnInventories')\
             ,lit(False).alias('isOtherInventories')\
             ,lit(False).alias('isAllowanceforOtherInventories')\
             ,lit(False).alias('isRawMaterialsExpense')\
             ,lit(False).alias('isChangesInInventoriesFGandWIP')\
             ,lit(False).alias('isCurrentAsset')\
             ,lit(False).alias('isCurrentLiabilities')\
             ,lit(False).alias('isNetProfitorLoss')\
             ,lit(False).alias('isEquity')\
             ,lit(False).alias('isProfitBeforeInterestAndTaxes')\
             ,lit(False).alias('isProfitBeforeTaxes')\
             ,lit(False).alias('isTradeARNet')\
             ,lit(False).alias('isTradeARIntercompany')\
             ,lit(False).alias('isTradeAPIntercompany')\
             ,lit('NONE').alias('accountGroup')\
              )
    
    fin_L3_STG_GLAccount = df_fin_L3_TMP_GLAccount.union(df_GLAccountCategoryType)
    #fin_L3_STG_GLAccount.display()
   
    dwh_vw_DIM_GLAccount=fin_L3_STG_GLAccount.alias('gla3')\
            .select(col('organizationUnitSurrogateKey')
            ,col('glAccountSurrogateKey')\
            ,col('accountNumberName')\
            ,col('chartOfAccount')\
            ,col('accountCategoryName')\
            ,col('accountCategoryIdentifier')\
            ,col('accountCategorySortOrder')\
            ,col('knowledgeAccountName')\
            ,col('knowledgeAccountIdentifier')\
            ,col('knowledgeAccountSortOrder')\
            ,col('financialStatementCaptionName')\
            ,col('financialStatementCaptionIdentifier')\
            ,col('financialStatementCaptionSortOrder')\
            ,col('accountCategoryL1')\
            ,col('accountCategoryL2')\
            ,col('accountCategoryL3')\
            ,col('accountCategoryL4')\
            ,col('accountCategoryL5')\
            ,col('accountCategoryL6')\
            ,col('accountCategoryL7')\
            ,col('accountCategoryL8')\
            ,col('accountCategoryL9')\
            ,col('accountNumber')\
            ,col('accountType')\
            ,col('accountCategoryIdentifierL1')\
            ,col('accountCategoryIdentifierL2')\
            ,col('accountCategoryIdentifierL3')\
            ,col('accountCategoryIdentifierL4')\
            ,col('accountCategoryIdentifierL5')\
            ,col('accountCategoryIdentifierL6')\
            ,col('accountCategoryIdentifierL7')\
            ,col('accountCategoryIdentifierL8')\
            ,col('accountCategoryIdentifierL9')\
            ,col('accountCategorySortOrderL1')\
            ,col('accountCategorySortOrderL2')\
            ,col('accountCategorySortOrderL3')\
            ,col('accountCategorySortOrderL4')\
            ,col('accountCategorySortOrderL5')\
            ,col('accountCategorySortOrderL6')\
            ,col('accountCategorySortOrderL7')\
            ,col('accountCategorySortOrderL8')\
            ,col('accountCategorySortOrderL9')\
            ,col('interCoFromAccountMapping')\
            ,col('interCoFromMasterData')\
            ,col('interCoFromTransactionData')\
            ,col('businessPartnerType')\
            ,col('accountProcess')\
            ,col('accountProcessSortOrder')\
            ,col('duplicateAccount')\
            ,col('uniqueAccount')\
            ,col('scopedAccount')\
            ,col('isRevenue')\
            ,col('isBank')\
            ,col('isCash')\
            ,col('isTradeAR')\
            ,col('isTradeAP')\
            ,col('isIntercompany')\
            ,col('is3rdParty')\
            ,col('isExpense')\
            ,col('isInterest')\
            ,col('isOtherPayables')\
            ,col('isGRIR')\
            ,col('isIncome')\
            ,col('isLiability')\
            ,col('isAccrued')\
            ,col('isAssets')\
            ,col('isWorkinProgressGross')\
            ,col('isManufacturedFinishedGoodsGross')\
            ,col('isPurchasedFinishedGoodsGross')\
            ,col('isCostofSales')\
            ,col('isRawMaterialsGross')\
            ,col('isAllowanceForWorkInProgress')\
            ,col('isAllowanceForRawMaterialsAndFactorySupplies')\
            ,col('isAllowanceforManufacturedFinishedGoods')\
            ,col('isAllowanceforPurchasedFinishedGoods')\
            ,col('isSalesDiscountsandVolumeRebates')\
            ,col('isRevenueFromSalesOfGoods')\
            ,col('isServicesRevenue')\
            ,col('isSalesReturns')\
            ,col('isBadDebtExpense')\
            ,col('isAllowanceforDoubtfulAccounts')\
            ,col('isTradeReceivablesNet')\
            ,col('isPropertyPlantandEquipmentNet')\
            ,col('isIntangibleAssetsAcquired')\
            ,col('isAmortizationExpense')\
            ,col('isDepreciationExpense')\
            ,col('isHistoricalCostsIntangibleAcquired')\
            ,col('isNetInvestmentPPE')\
            ,col('isPersonnelExpense')\
            ,col('isRepairAndMaintenanceExpense')\
            ,col('isCalculationRelevantWagesAndSalaries')\
            ,col('isSalaries')\
            ,col('isEmployerShareSocialSecuritySalaries')\
            ,col('isWages')\
            ,col('isEmployerShareSocialSecurityWages')\
            ,col('isNetIncomeLoss')\
            ,col('isIncomeTax')\
            ,col('isDeferredIncomeTax')\
            ,col('isOtherTax')\
            ,col('isOtherOperatingExpenses')\
            ,col('isNetInvestmentPropertyPlantAndEquipment')\
            ,col('isIntercompanyLoanIncome')\
            ,col('isIntercompanyLoanExpense')\
            ,col('isInterestIncomeRelatedCompaniesAtShortNotice')\
            ,col('isInterestExpenseAffiliatedCompaniesAtShortNotice')\
            ,col('isHistoricalCostsPPEAcquired')\
            ,col('isPPEAccumulatedDepreciation')\
            ,col('isAccumulatedAmortizationIntangible')\
            ,col('isOtherLiabilityTax')\
            ,col('isFixedAssets')\
            ,col('isInventories')\
            ,col('isAdvancePaymentsOnInventories')\
            ,col('isOtherInventories')\
            ,col('isAllowanceforOtherInventories')\
            ,col('isRawMaterialsExpense')\
            ,col('isChangesInInventoriesFGandWIP')\
            ,col('isCurrentAsset')\
            ,col('isCurrentLiabilities')\
            ,col('isNetProfitorLoss')\
            ,col('isEquity')\
            ,col('isProfitBeforeInterestAndTaxes')\
            ,col('isProfitBeforeTaxes')\
            ,col('isTradeARNet')
            ,col('isTradeARIntercompany')\
            ,col('isTradeAPIntercompany')\
            ,col('accountGroup'))

    
    
    #dwh_vw_DIM_GLAccount.display()
    
    #fin_L3_STG_GLAccount.display()
    dwh_vw_DIM_GLAccount = objDataTransformation.gen_convertToCDMStructure_generate(\
                    dwh_vw_DIM_GLAccount,'dwh','vw_DIM_GLAccount',isIncludeAnalysisID = True, isSqlFormat = True)[0]
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_GLAccount,gl_CDMLayer2Path + "fin_L2_DIM_GLAccount.parquet" )

    executionStatus = "L3_STG_GLAccount populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

 
  except Exception as e:
     executionStatus = objGenHelper.gen_exceptionDetails_log()       
     executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
     return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
