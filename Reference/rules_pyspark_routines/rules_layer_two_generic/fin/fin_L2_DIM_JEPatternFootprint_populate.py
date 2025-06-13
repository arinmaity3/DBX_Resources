# Databricks notebook source
from pyspark.sql.window import Window
import sys
import traceback
from pyspark.sql.functions import when,col,concat,concat_ws,collect_set,max,array_sort,struct,collect_list,dense_rank

def fin_L2_DIM_JEPatternFootPrint_populate():
    """Populate global dataframe and SQL tempview for fin_L2_DIM_JEPatternFootPrint """
    try:

        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION) 
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()

        global fin_L2_DIM_JEPatternFootPrint
        global fin_L1_STG_JEAccountFootPrint
        
        w = Window.partitionBy("companyCode","documentNumber","fiscalYear","debitCreditIndicator")
        
        fin_L1_TMP_AccountKnowledgeFootprint = spark.sql("Select	companyCode \
			       ,GKL.accountNumber as accountNumber \
			       ,case when isnull(kna.displayName) \
                        then case when isnull(kna.knowledgeAccountName) then 'UnAssigned' else kna.knowledgeAccountName end \
                        else kna.displayName end as KnowledgeAccount \
			       ,case when isnull(fsc.displayName)\
                        then case when isnull(fsc.knowledgeAccountName) then 'UnAssigned' else fsc.knowledgeAccountName end \
                        else fsc.displayName end as FSCAccount \
			      ,accountType	\
            from fin_L1_MD_GLAccount GKL	\
            left join fin_L1_STG_GLAccountMapping GAM	on GAM.accountnumber =   GKL.accountnumber \
            left join fin_L1_STG_GLAccountFlattenHierarchy FGH	on FGH.leafID =   GAM.accountCategoryID \
            left join knw_LK_CD_KnowledgeAccountSnapshot kna \
            on kna.knowledgeAccountID IN  (case when ISNULL(FGH.identifier_01) then '' else FGH.identifier_01 end \
                                            ,case when ISNULL(FGH.identifier_02) then '' else FGH.identifier_02 end \
                                            ,case when ISNULL(FGH.identifier_03) then '' else FGH.identifier_03 end \
                                            ,case when ISNULL(FGH.identifier_04) then '' else FGH.identifier_04 end \
                                            ,case when ISNULL(FGH.identifier_05) then '' else FGH.identifier_05 end \
                                            ,case when ISNULL(FGH.identifier_06) then '' else FGH.identifier_06 end \
                                            ,case when ISNULL(FGH.identifier_07) then '' else FGH.identifier_07 end \
                                            ,case when ISNULL(FGH.identifier_08) then '' else FGH.identifier_08 end \
                                            ,case when ISNULL(FGH.identifier_09) then '' else FGH.identifier_09 end) \
                                        and kna.knowledgeAccountType = 'Account' \
            left join knw_LK_CD_FinancialStructureDetailSnapshot fsd on kna.knowledgeAccountID	= fsd.knowledgeAccountNodeID \
            left join knw_LK_CD_KnowledgeAccountSnapshot fsc on fsd.knowledgeAccountParentID	=  fsc.knowledgeAccountID")
        fin_L1_TMP_AccountKnowledgeFootprint.createOrReplaceTempView("fin_L1_TMP_AccountKnowledgeFootprint")
        fin_L1_TMP_JEPatternFootPrintDetail= spark.sql("SELECT '' as clientID,JE.companyCode \
               , JE.fiscalYear \
               , JE.documentNumber \
               , JE.debitCreditIndicator \
               , case when GKL.KnowledgeAccount is null then 'UnAssigned' else GKL.KnowledgeAccount end as accountCategory_Level5 \
               , SUM(CASE WHEN JE.debitCreditIndicator	='D' THEN 1 ELSE 0 END ) as debitCount \
               , SUM(CASE WHEN JE.debitCreditIndicator	='C' THEN 1 ELSE 0 END ) as creditCount \
               , SUM(CASE WHEN JE.debitCreditIndicator	='D' THEN JE.amountLC ELSE 0 END) as debitAmount \
               , SUM(CASE WHEN JE.debitCreditIndicator	='C' THEN JE.amountLC ELSE 0 END) as creditAmount \
               , GKL.accountType \
           FROM fin_L1_TD_Journal JE \
           LEFT JOIN fin_L1_TMP_AccountKnowledgeFootprint GKL	  ON \
                                                   ( \
                                                       JE.accountNumber	= GKL.accountNumber		AND \
                                                       JE.companyCode		= GKL.companyCode \
                                                   )	\
           WHERE JE.documentStatus ='N' \
           GROUP BY \
                 JE.companyCode \
               , JE.fiscalYear \
               , JE.documentNumber \
               , JE.debitCreditIndicator \
               , case when GKL.KnowledgeAccount is null then 'UnAssigned' else GKL.KnowledgeAccount end \
               , GKL.accountType ")
        
        
        fin_L1_TMP_JEPatternFootPrintDetail_FinancialStatementCaption= spark.sql("SELECT'' as clientID,JE.companyCode \
               , JE.fiscalYear \
               , JE.documentNumber \
               , JE.debitCreditIndicator \
               , case when GKL.FSCAccount is null then 'UnAssigned' else GKL.FSCAccount end as accountCategory_Level3 \
               , SUM(CASE WHEN JE.debitCreditIndicator	='D' THEN 1 ELSE 0 END ) as debitCount \
               , SUM(CASE WHEN JE.debitCreditIndicator	='C' THEN 1 ELSE 0 END ) as creditCount \
               , SUM(CASE WHEN JE.debitCreditIndicator	='D' THEN JE.amountLC ELSE 0 END) as debitAmount \
               , SUM(CASE WHEN JE.debitCreditIndicator	='C' THEN JE.amountLC ELSE 0 END) as creditAmount \
               , GKL.accountType \
           FROM fin_L1_TD_Journal JE \
           LEFT JOIN fin_L1_TMP_AccountKnowledgeFootprint GKL	  ON \
                                                   ( \
                                                       JE.accountNumber	= GKL.accountNumber		AND \
                                                       JE.companyCode		= GKL.companyCode \
                                                   )	\
           WHERE JE.documentStatus ='N' \
           GROUP BY \
                 JE.companyCode \
               , JE.fiscalYear \
               , JE.documentNumber \
               , JE.debitCreditIndicator \
               , case when GKL.FSCAccount is null then 'UnAssigned' else GKL.FSCAccount end \
               , GKL.accountType ")
        
        fin_L1_TMP_JEPatternFootPrintDetail_GLAccount= spark.sql("SELECT '' as clientID,JE.companyCode \
                        , JE.fiscalYear \
                        , JE.documentNumber \
                        , concat(case when isnull(GKL.accountNumber) \
                                      then case when isnull(JE.accountNumber) then '#NA#' else JE.accountNumber end \
                                      else GKL.accountNumber end ,' (',case when GKL.accountName is null \
                                         then '#NA#' else GKL.accountName end,')') as accountNumberName \
                        ,JE.debitCreditIndicator \
                    FROM fin_L1_TD_Journal JE \
                    LEFT JOIN fin_L1_MD_GLAccount GKL	  ON \
                                                            ( \
                                                                JE.accountNumber	= GKL.accountNumber		AND \
                                                                JE.companyCode		= GKL.companyCode \
                                                            )	\
                    WHERE JE.documentStatus ='N' \
                    GROUP BY \
                          JE.companyCode \
                        , JE.fiscalYear \
                        , JE.documentNumber \
                         ,concat(case when isnull(GKL.accountNumber) \
                                      then case when isnull(JE.accountNumber) then '#NA#' else JE.accountNumber end \
                                      else GKL.accountNumber end ,' (',case when GKL.accountName is null \
                                         then '#NA#' else GKL.accountName end,')') \
                        , JE.debitCreditIndicator ")
        
        w = Window.partitionBy("companyCode","documentNumber","fiscalYear","debitCreditIndicator")
        fin_L1_TMP_JEAccountFootPrintAccountPattern=fin_L1_TMP_JEPatternFootPrintDetail_GLAccount\
                .withColumn("debitGLAccountFootprint",when (col("debitCreditIndicator")=='D'\
                                                            ,concat(lit("D:  | "),concat_ws(' | ',array_sort(collect_set("accountNumberName").over(w))),lit(" | ")))\
                                                            .otherwise(lit(None)))\
                .withColumn("creditGLAccountFootprint",when (col("debitCreditIndicator")=='C'\
                                                            ,concat(lit("C:  | "),concat_ws(' | ',array_sort(collect_set("accountNumberName").over(w))),lit(" | ")))\
                                                            .otherwise(lit(None)))\
                .groupBy("companyCode","documentNumber","fiscalYear")\
                .agg(max("debitGLAccountFootprint").alias("debitGLAccountFootprint")\
                    ,max("creditGLAccountFootprint").alias("creditGLAccountFootprint")\
                    )
        fin_L1_TMP_JEAccountFootPrintL5AccountCatPattern=fin_L1_TMP_JEPatternFootPrintDetail\
                .withColumn("DebitL5AccountCatFootprint_Struct",struct("accountType","accountCategory_Level5","debitCount","creditCount"))\
                .withColumn("DebitL5AccountCatFootprint_StructSorted",when (col("debitCreditIndicator")=='D'\
                                                                            ,array_sort(collect_list("DebitL5AccountCatFootprint_Struct").over(w)))\
                                                                            .otherwise(lit(None)))\
                .withColumn("DebitL5AccountCatFootprint",when((concat_ws(' | ',"DebitL5AccountCatFootprint_StructSorted.accountCategory_Level5")).isNull()| \
                            concat_ws(' | ',"DebitL5AccountCatFootprint_StructSorted.accountCategory_Level5").isin('')\
                            ,lit(None))\
                                .otherwise(concat(lit("D:  | ")\
                            ,concat_ws(' | ',"DebitL5AccountCatFootprint_StructSorted.accountCategory_Level5"),lit(" | "))))\
                \
                .withColumn("CreditL5AccountCatFootprint_Struct",struct("accountType","accountCategory_Level5","debitCount","creditCount"))\
                .withColumn("CreditL5AccountCatFootprint_StructSorted",when (col("debitCreditIndicator")=='C'\
                                                                            ,array_sort(collect_list("CreditL5AccountCatFootprint_Struct").over(w)))\
                                                                            .otherwise(lit(None)))\
                .withColumn("CreditL5AccountCatFootprint",when((concat_ws(' | ',"CreditL5AccountCatFootprint_StructSorted.accountCategory_Level5")).isNull()| \
                                concat_ws(' | ',"CreditL5AccountCatFootprint_StructSorted.accountCategory_Level5").isin('')\
                                ,lit(None))\
                            .otherwise(concat(lit("C:  | ")\
                                ,concat_ws(' | ',"CreditL5AccountCatFootprint_StructSorted.accountCategory_Level5"),lit(" | "))))\
                .groupBy("clientID","companyCode","documentNumber","fiscalYear")\
                .agg(max("DebitL5AccountCatFootprint").alias("DebitL5AccountCatFootprint")\
                    ,max("CreditL5AccountCatFootprint").alias("CreditL5AccountCatFootprint")\
                    )
        fin_L1_TMP_JEAccountFootPrintL3AccountCatPattern=fin_L1_TMP_JEPatternFootPrintDetail_FinancialStatementCaption\
               .withColumn("debitL3AccountCatFootprint_Struct",struct("accountType","accountCategory_Level3","debitCount","creditCount"))\
                .withColumn("debitL3AccountCatFootprint_StructSorted",when (col("debitCreditIndicator")=='D'\
                                                                           ,array_sort(collect_list("debitL3AccountCatFootprint_Struct").over(w)))\
                                                                           .otherwise(lit(None)))\
                .withColumn("debitL3AccountCatFootprint",when((concat_ws(' | ',"debitL3AccountCatFootprint_StructSorted.accountCategory_Level3")).isNull()| \
                            concat_ws(' | ',"debitL3AccountCatFootprint_StructSorted.accountCategory_Level3").isin('')\
                            ,lit(None))\
                            .otherwise(concat(lit("D:  | ")\
                            ,concat_ws(' | ',"debitL3AccountCatFootprint_StructSorted.accountCategory_Level3"),lit(" | "))))\
                \
                .withColumn("creditL3AccountCatFootprint_Struct",struct("accountType","accountCategory_Level3","debitCount","creditCount"))\
                .withColumn("creditL3AccountCatFootprint_StructSorted",when (col("debitCreditIndicator")=='C'\
                                                                            ,array_sort(collect_list("creditL3AccountCatFootprint_Struct").over(w)))\
                                                                            .otherwise(lit(None)))\
                .withColumn("creditL3AccountCatFootprint",when((concat_ws(' | ',"creditL3AccountCatFootprint_StructSorted.accountCategory_Level3")).isNull()| \
                            concat_ws(' | ',"creditL3AccountCatFootprint_StructSorted.accountCategory_Level3").isin('')\
                            ,lit(None))\
                            .otherwise(concat(lit("C:  | ")\
                            ,concat_ws(' | ',"creditL3AccountCatFootprint_StructSorted.accountCategory_Level3"),lit(" | "))))\
                .groupBy("clientID","companyCode","documentNumber","fiscalYear")\
                .agg(max("debitL3AccountCatFootprint").alias("debitL3AccountCatFootprint")\
                    ,max("creditL3AccountCatFootprint").alias("creditL3AccountCatFootprint")\
                    )
        fin_L1_STG_JEAccountFootPrint = fin_L1_TMP_JEAccountFootPrintAccountPattern.alias('gla')\
                                        .join(fin_L1_TMP_JEAccountFootPrintL5AccountCatPattern.alias('catl5'),\
                                         ((col('gla.companyCode')== col('catl5.companyCode'))\
                                              & (col('gla.documentNumber') ==  col('catl5.documentNumber'))\
                                              & (col('gla.fiscalYear') ==  col('catl5.fiscalYear'))\
                                             ),how = 'inner')\
                                        .join(fin_L1_TMP_JEAccountFootPrintL3AccountCatPattern.alias('catl3'),\
                                         ((col('gla.companyCode')== col('catl3.companyCode'))\
                                              & (col('gla.documentNumber') ==  col('catl3.documentNumber'))\
                                              & (col('gla.fiscalYear') ==  col('catl3.fiscalYear'))\
                                             ),how = 'inner')\
                                        .select(lit('').alias('clientID'),col('gla.companyCode')\
                                               ,col('gla.fiscalYear'), \
                                                col('gla.documentNumber'),\
                                                col('catl5.CreditL5AccountCatFootprint'),\
                                                col('catl5.DebitL5AccountCatFootprint'),\
                                                col('catl3.creditL3AccountCatFootprint'),\
                                                col('catl3.debitL3AccountCatFootprint'),\
                                                col('gla.debitGLAccountFootprint'),\
                                                col('gla.creditGLAccountFootprint')\
                                                )
        fin_L1_STG_JEAccountFootPrint = fin_L1_STG_JEAccountFootPrint\
                  .withColumn("DCL5AccountCatFootprint",when (col("DebitL5AccountCatFootprint").isNotNull() \
                                   & col("CreditL5AccountCatFootprint").isNotNull()\
                                   ,concat(col("DebitL5AccountCatFootprint"),lit(" -- "),col("CreditL5AccountCatFootprint")))\
                                   .otherwise(when (col("DebitL5AccountCatFootprint").isNotNull()\
                                                   ,col("DebitL5AccountCatFootprint")).otherwise(col("CreditL5AccountCatFootprint"))))\
                  .withColumn("DCL3AccountCatFootprint",when (col("DebitL3AccountCatFootprint").isNotNull()\
                                  & col("CreditL3AccountCatFootprint").isNotNull()\
                                  ,concat(col("DebitL3AccountCatFootprint"),lit(" -- "),col("CreditL3AccountCatFootprint")))\
                                  .otherwise(when (col("DebitL3AccountCatFootprint").isNotNull()\
                                                  ,col("DebitL3AccountCatFootprint")).otherwise(col("CreditL3AccountCatFootprint"))))\
                  .withColumn("patternID",dense_rank().over(Window.orderBy("DebitL5AccountCatFootprint",\
                              "CreditL5AccountCatFootprint","DebitGLAccountFootprint"\
                              ,"CreditGLAccountFootprint")))
                
        fin_L1_STG_JEAccountFootPrint  = objDataTransformation.gen_convertToCDMandCache \
            (fin_L1_STG_JEAccountFootPrint,'fin','L1_STG_JEAccountFootPrint',False)

        fin_L2_DIM_JEPatternFootPrint= spark.sql("select distinct patternID,dCL5AccountCatFootprint\
                                               ,DebitL5AccountCatFootprint,CreditL5AccountCatFootprint\
                                               ,DCL3AccountCatFootprint\
                                               ,debitL3AccountCatFootprint,\
                                               creditL3AccountCatFootprint,\
                                               DebitGLAccountFootprint,\
                                               CreditGLAccountFootprint\
                                          from fin_L1_STG_JEAccountFootPrint")
        fin_L2_DIM_JEPatternFootPrint  = objDataTransformation.gen_convertToCDMandCache \
            (fin_L2_DIM_JEPatternFootPrint,'fin','L2_DIM_JEPatternFootPrint',True)

        vw_DIM_JEPatternFootPrint = spark.sql("SELECT \
          analysisID AS analysisID \
          ,patternID AS	patternID \
          ,CASE WHEN LENGTH(dCL5AccountCatFootprint)>2048 \
           THEN CONCAT(LEFT(dCL5AccountCatFootprint,2045),'...') \
           ELSE dCL5AccountCatFootprint END AS	dcL5AccountCatFootprint \
          ,CASE WHEN LENGTH(DebitL5AccountCatFootprint)>2048	\
           THEN CONCAT(LEFT(DebitL5AccountCatFootprint,2045),'...') \
           ELSE	DebitL5AccountCatFootprint END AS debAccountCatFootprint \
          ,CASE WHEN LENGTH(CreditL5AccountCatFootprint)>2048 \
           THEN CONCAT(LEFT(CreditL5AccountCatFootprint,2045),'...') \
           ELSE	CreditL5AccountCatFootprint   END  AS credAccountCatFootprint \
          ,CASE WHEN LENGTH(DCL3AccountCatFootprint)>2048 \
           THEN CONCAT(LEFT(DCL3AccountCatFootprint,2045),'...') \
           ELSE DCL3AccountCatFootprint END AS	DCL3AccountCatFootprint \
          ,CASE WHEN LENGTH(debitL3AccountCatFootprint)>2048 \
		   THEN CONCAT(LEFT(debitL3AccountCatFootprint,2045),'...')  \
		   ELSE	debitL3AccountCatFootprint END AS debL3AccountCatFootprint \
		  ,CASE WHEN LENGTH(creditL3AccountCatFootprint)>2048 \
		   THEN CONCAT(LEFT(creditL3AccountCatFootprint,2045),'...') \
		   ELSE	creditL3AccountCatFootprint   END  AS credL3AccountCatFootprint	\
		  ,CASE WHEN LENGTH(DebitGLAccountFootprint)>2048 \
		   THEN CONCAT(LEFT(DebitGLAccountFootprint,2045),'...') \
		   ELSE DebitGLAccountFootprint END	AS  debGLAccountFootprint \
		  ,CASE WHEN LENGTH(CreditGLAccountFootprint)>2048 \
           THEN CONCAT(LEFT(CreditGLAccountFootprint,2045),'...') \
		   ELSE CreditGLAccountFootprint END  AS  credGLAccountFootprint \
		  ,DENSE_RANK() OVER(ORDER BY CASE WHEN LENGTH(dCL5AccountCatFootprint)>2048 \
                THEN LEFT(dCL5AccountCatFootprint,2045) \
				ELSE dCL5AccountCatFootprint END) as dcL5AccountCatFootprintKey \
	      ,DENSE_RANK() OVER(ORDER BY CASE WHEN LENGTH(DebitL5AccountCatFootprint)>2048 \
                THEN LEFT(DebitL5AccountCatFootprint,2045) \
				ELSE DebitL5AccountCatFootprint END) AS debAccountCatFootprintKey \
	      ,DENSE_RANK() OVER(ORDER BY CASE WHEN LENGTH(CreditL5AccountCatFootprint)>2048 \
                THEN LEFT(CreditL5AccountCatFootprint,2045) \
				ELSE CreditL5AccountCatFootprint   END)  AS credAccountCatFootprintKey \
          ,DENSE_RANK() OVER(ORDER BY CASE WHEN LENGTH(DebitGLAccountFootprint)>2048 \
                THEN LEFT(DebitGLAccountFootprint,2045) \
				ELSE DebitGLAccountFootprint END)	AS  debGLAccountFootprintKey \
	      ,DENSE_RANK() OVER(ORDER BY CASE WHEN LENGTH(CreditGLAccountFootprint)>2048 \
                THEN LEFT(CreditGLAccountFootprint,2045) \
				ELSE CreditGLAccountFootprint END)  AS  credGLAccountFootprintKey \
          ,CASE WHEN ISNULL(countOfCustomers) THEN 0 ELSE countOfCustomers END  AS countOfCustomers \
		  ,CASE WHEN ISNULL(isCustomerAvailable) THEN 'No - customer not available' ELSE isCustomerAvailable END	\
                      AS isCustomerAvailable \
	      ,CASE WHEN ISNULL(CASE WHEN LENGTH(customerPattern) > 2048 THEN CONCAT(LEFT(customerPattern, 2045),'...') ELSE customerPattern END) \
           THEN'#NA#' \
           ELSE (CASE WHEN LENGTH(customerPattern) > 2048 THEN CONCAT(LEFT(customerPattern, 2045),'...') ELSE customerPattern END) END\
                     AS customerPattern \
	      ,CASE WHEN ISNULL(customerCountryName) THEN '#NA#' ELSE customerCountryName END \
                   AS customerCountryName \
	      ,CASE WHEN ISNULL(highestCustomer) THEN '#NA#'ELSE highestCustomer END AS highestCustomer \
	      ,CASE WHEN ISNULL(highestCustomerCountry) THEN'#NA#' ELSE highestCustomerCountry END\
                  AS highestCustomerCountry \
	      ,CASE WHEN ISNULL(highestCustomerIsInterCompany) THEN '#NA#' ELSE highestCustomerIsInterCompany END\
                 AS highestCustomerIsInterCompany \
	      ,CASE WHEN ISNULL(countOfVendors) THEN 0 ELSE countOfVendors END	AS countOfVendors \
	      ,CASE WHEN ISNULL(isVendorAvailable) THEN 'No - vendor not available' ELSE isVendorAvailable END \
                AS isVendorAvailable \
	      ,CASE WHEN ISNULL(highestVendor) THEN '#NA#' ELSE highestVendor END AS highestVendor \
	      ,CASE WHEN ISNULL(highestVendorCountry) THEN '#NA#' ELSE highestVendorCountry END \
               AS highestVendorCountry \
	      ,CASE WHEN ISNULL(CASE WHEN LENGTH(vendorPattern) > 2048 THEN CONCAT(LEFT(vendorPattern, 2045),'...') ELSE vendorPattern END) \
           THEN '#NA#' \
           ELSE (CASE WHEN LENGTH(vendorPattern) > 2048 THEN CONCAT(LEFT(vendorPattern, 2045),'...') ELSE vendorPattern END)END \
              AS vendorPattern \
	      ,CASE WHEN ISNULL(vendorCountryName) THEN '#NA#' ELSE vendorCountryName END AS vendorCountryName \
	      ,CASE WHEN ISNULL(highestVendorIsInterCompany) THEN'#NA#' ELSE highestVendorIsInterCompany END \
              AS highestVendorIsInterCompany \
	      ,CASE WHEN LENGTH(DebitGLAccountFootprint) >2048 AND LENGTH(CreditGLAccountFootprint)>2048 \
		   THEN '3 - Debit&Credit Footprint Truncated' \
		   WHEN LENGTH(DebitGLAccountFootprint) >2048 \
           THEN	'1 - Debit Footprint Truncated'  \
		   WHEN LENGTH(CreditGLAccountFootprint)>2048 \
		   THEN	'2 - Credit Footprint Truncated' \
		   ELSE '0 - No Truncation' END AS isFootprintTruncated \
		  ,CASE	WHEN LENGTH(DebitL5AccountCatFootprint)>2048 AND LENGTH(CreditL5AccountCatFootprint) >2048 \
		   THEN '1 - Debit&Credit Category Footprint Truncated' \
		   WHEN LENGTH(DebitL5AccountCatFootprint) > 2048 \
		   THEN '2 - Debit Category Footprint Truncated' \
	 	   WHEN LENGTH(CreditL5AccountCatFootprint)	>2048 \
		   THEN '3 - Credit Category Footprint Truncated' \
		   ELSE	'0 - No Truncation' END AS isCategoryFootprintTruncated \
		  ,CASE	WHEN LENGTH(creditL3AccountCatFootprint) >2048 AND LENGTH(debitL3AccountCatFootprint) >2048 \
		   THEN '1 - Debit&Credit FinStmtCptn Footprint Truncated' \
	 	   WHEN LENGTH(debitL3AccountCatFootprint) >2048 \
		   THEN '2 - Debit FinStmtCptn Footprint Truncated' \
		   WHEN LENGTH(creditL3AccountCatFootprint)	> 2048	\
		   THEN '3 - Credit FinStmtCptn Footprint Truncated' \
		   ELSE	'0 - No Truncation' END AS isL3CategoryFootprintTruncated \
		  ,CASE	WHEN LENGTH(customerPattern) >2048 \
		   THEN '1 - Customer Pattern Footprint Truncated' \
		   ELSE	'0 - No Truncation' END AS isCustomerFootprintTruncated \
		  ,CASE	WHEN LENGTH(vendorPattern) >2048	\
		   THEN '1- Vendor Pattern Footprint Truncated' \
	       ELSE	'0 - No Truncation' END AS isVendorFootprintTruncated \
        FROM fin_L2_DIM_JEPatternFootPrint")
        
        dwh_vw_DIM_JEPatternFootPrint = objDataTransformation.gen_convertToCDMStructure_generate(\
                 vw_DIM_JEPatternFootPrint,'dwh','vw_DIM_JEPatternFootPrint',False)[0]
        keysAndValues = {'patternID':0,'dcL5AccountCatFootprintKey':0,'debAccountCatFootprintKey' :0 ,'credAccountCatFootprintKey':0, 'debGLAccountFootprintKey':0 \
                        ,'credGLAccountFootprintKey':0 ,'countOfCustomers':0, 'isCustomerAvailable': 'No - customer not available' \
                        ,'countOfVendors':0, 'isVendorAvailable': 'No - vendor not available'}
        dwh_vw_DIM_JEPatternFootPrint = objGenHelper.gen_placeHolderRecords_populate(\
                  'dwh.vw_DIM_JEPatternFootPrint',keysAndValues,dwh_vw_DIM_JEPatternFootPrint)
        objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_JEPatternFootPrint,gl_CDMLayer2Path + "fin_L2_DIM_JEPatternFootPrint.parquet" ) 
        
        executionStatus = "fin_L2_DIM_JEPatternFootPrint populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L2_DIM_JEPatternFootPrint")
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_STG_JEAccountFootPrint")
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
