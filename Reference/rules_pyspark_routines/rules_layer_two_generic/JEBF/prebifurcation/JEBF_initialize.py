# Databricks notebook source
import sys
import traceback


def JEBF_initialize():     
  """L1_JEBifurcation_00_Initialize_populate"""
  
  try:
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    
    
    objGenHelper.gen_Directory_clear(gl_bifurcationPath)
    objGenHelper.gen_Directory_clear(gl_preBifurcationPath)
    objGenHelper.gen_Directory_clear(gl_postBifurcationPath)

    global fin_L1_TD_Journal
    if fin_L1_TD_Journal is None or fin_L1_TD_Journal.rdd.isEmpty():
        raise ValueError("Journal is empty, JEBF algorithms not executed.")

    startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    bifurcationToExtractionDate = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'BIFURCATION_TO_EXTRACTION_DATE')  
    
    if bifurcationToExtractionDate is None:
      bifurcationToExtractionDate = 0
            
    if bifurcationToExtractionDate == 1:
      endDate = parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'EXTRACTION_DATE')).date()
            
    else:
      endDate = parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
      
                
    periodStart = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START'))
    
    
    if periodStart is None:
       periodStart = 0
        
          
    periodEnd = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END'))
    
    if periodEnd is None:
      periodEnd = 0
      


    col_lineCountGroup = when(col('cnt_lines').between(lit(0),lit(200)), lit('A') ) \
                              .when(col('cnt_lines').between(lit(201),lit(400)), lit('B') )\
                              .when(col('cnt_lines').between(lit(401),lit(600)), lit('C') )\
                              .when(col('cnt_lines').between(lit(601),lit(800)), lit('D') )\
                              .otherwise(lit('E'))

    if knw_LK_CD_knowledgeAccountScopingDetail.first() is None:
        L1_TMP_JEBifurcation_Scoped_Journal = fin_L1_TD_Journal.alias('jou2')\
                           .filter((col("documentStatus").isin('N',''))\
                              & (col("postingDate").cast("date").between(lit(startDate),lit(endDate)))\
                              & ((lit(bifurcationToExtractionDate)==1) | (col("financialPeriod").cast("int")\
                                                        .between(lit(periodStart),lit(periodEnd)))))\
                           .select(col('companyCode'),col('documentNumber'),col('fiscalYear'))\
                           .distinct()
    else:
        L1_TMP_JEBifurcation_Scoped_Journal = fin_L1_TD_Journal.alias('jou2')\
                            .join(fin_L1_STG_GLAccountMapping.alias('glam1'),\
                                 ((col('jou2.accountNumber')== col('glam1.accountNumber'))\
                                 &(col('glam1.isScopedforBifurcation')== True)),how = 'inner')\
                           .filter((col("documentStatus").isin('N',''))\
                              & (col("postingDate").cast("date").between(lit(startDate),lit(endDate)))\
                              & ((lit(bifurcationToExtractionDate)==1) | (col("financialPeriod").cast("int")\
                                                        .between(lit(periodStart),lit(periodEnd)))))\
                           .select(col('companyCode'),col('documentNumber'),col('fiscalYear'))\
                           .distinct()
      
    L1_TMP_JEBifurcation_Scoped_Journal.createOrReplaceTempView("L1_TMP_JEBifurcation_Scoped_Journal")
    sqlContext.cacheTable("L1_TMP_JEBifurcation_Scoped_Journal")
      

    headerDescription = when(col('jou1.headerDescription').isNull(),lit(''))\
                      .otherwise(col('jou1.headerDescription'))

    lineDescription = when(col('jou1.lineDescription').isNull(),lit(''))\
                      .otherwise(col('jou1.lineDescription'))

    debitAmount = when(col('jou1.debitCreditIndicator') == 'D', col('amountLC'))\
                .otherwise(lit(0))

    creditAmount = when(col('jou1.debitCreditIndicator') == 'C', col('amountLC'))\
                .otherwise(lit(0))

    isClearing = when((col('clearingDocumentNumber') != lit('#NA#'))\
                      & (col('clearingDocumentNumber').isNotNull()) \
                      & (col('clearingDocumentNumber')!= ''), lit(1))\
                .otherwise(lit(0))
    
    df_maxlength = fin_L1_TD_Journal.withColumn("len_lineitem",length(col("lineItem")))\
                       .groupBy().max("len_lineitem")
    maxLength=df_maxlength.first()[0]
    
    dr_transactionLineIDKey= Window.partitionBy("jou1.documentNumber","jou1.companyCode","jou1.fiscalYear")\
                            .orderBy(lpad(col("lineItem"), maxLength, '0'),"jou1.debitCreditIndicator")
    
    
    fin_L1_STG_JEBifurcation_00_Initialize = fin_L1_TD_Journal.alias('jou1')\
                .join(knw_LK_CD_ReportingSetup.alias('rep1'),\
                      ((col('jou1.companyCode')== col('rep1.companyCode'))),how = 'inner')\
                .join(L1_TMP_JEBifurcation_Scoped_Journal.alias('jsj'),\
                     ((col('jou1.companyCode')== col('jsj.companyCode'))\
                        & (col('jou1.documentNumber') ==  col('jsj.documentNumber'))\
                        & (col('jou1.fiscalYear') ==  col('jsj.fiscalYear'))\
                      ),how = 'inner')\
                .filter(col("jou1.documentStatus").isin('N','')\
                        & (col("postingDate").cast("date").between(lit(startDate),lit(endDate)))\
                        & ((lit(bifurcationToExtractionDate)==1) | (col("jou1.financialPeriod").cast("int")\
                           .between(lit(periodStart),lit(periodEnd)))))\
                .select(col('jou1.journalSurrogateKey')\
                        ,col('jou1.transactionIDbyJEDocumnetNumber')\
                        ,col('jou1.transactionIDbyPrimaryKey')\
                        ,F.row_number().over(dr_transactionLineIDKey).alias("transactionLineIDKey")\
                        ,col('jou1.lineItem')\
                        ,substring(concat(lit(headerDescription),lit(' '),lit(lineDescription)),1,4000)\
                                    .alias('transactionDescription')\
                        ,col('jou1.documentNumber').alias('journalId')\
                        ,lit('').alias('journalCode')\
                        ,lit('').alias('journalName')\
                        ,col('jou1.financialPeriod').alias('period')\
                        ,col('jou1.fiscalYear')\
                        ,col('jou1.postingDate')\
                        ,col('jou1.creationDate').alias('journalDate')\
                        ,col('jou1.accountNumber').alias('accountID')\
                        ,lit(debitAmount).alias('debitAmount')\
                        ,lit(creditAmount).alias('creditAmount')\
                        ,col('jou1.debitCreditIndicator').alias('debitCreditCode')\
                        ,lit(0).alias('isManual'),lit(isClearing).alias('isClearing'),col('jou1.createdBy').alias('user')\
                        ,when(col('jou1.productNumber') == '', lit('#NA#'))\
                              .when(col('jou1.productNumber').isNull(), lit('#NA#'))\
                              .otherwise(col('jou1.productNumber')).alias('productNo')\
                        ,when(col('jou1.customerNumber') == '', lit('#NA#'))\
                             .when(col('jou1.customerNumber').isNull(), lit('#NA#'))\
                             .otherwise(col('jou1.customerNumber')).alias('customerNumber')\
                        ,when(col('jou1.vendorNumber') == '', lit('#NA#'))\
                              .when(col('jou1.vendorNumber').isNull(), lit('#NA#'))\
                              .otherwise(col('jou1.vendorNumber')).alias('vendorNumber')\
                        ,when(col('jou1.referenceSubledgerDocumentNumber') == '', lit('#NA#'))\
                             .when(col('jou1.referenceSubledgerDocumentNumber').isNull(), lit('#NA#'))\
                             .otherwise(col('jou1.referenceSubledgerDocumentNumber')).alias('purchDoc')\
                        ,when(col('jou1.debitCreditIndicator') == lit('D'), col('amountLC'))\
                              .otherwise(col('amountLC')*(-1)).alias('postingAmount')\
                        ,col('jou1.companyCode').alias('companyCode')\
                      )
    objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_00_Initialize,\
               filenamepath = gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_00_Initialize.delta")
    executionStatus = "fin_L1_STG_JEBifurcation_00_Initialize populated successfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)

    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

  
  finally:
      
      spark.sql("UNCACHE TABLE  IF EXISTS L1_TMP_JEBifurcation_Scoped_Journal")
  
