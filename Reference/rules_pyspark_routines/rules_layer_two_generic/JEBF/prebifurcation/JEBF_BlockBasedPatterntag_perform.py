# Databricks notebook source
import sys
import traceback
import inspect

def JEBF_BlockBasedPatterntag_perform(BlockType):
        """Populate fin_usp_L1_JEBifurcation_Block_Based_Pattern_tag"""
        try:

            objGenHelper = gen_genericHelper()
            
            patternSource=""
            if BlockType==1:
              patternSource = '3-Block Building Pattern'
            elif BlockType==2:
              patternSource = '4-Shifted Block Building Pattern'
            elif BlockType==3:
              patternSource = '5-Combined Pattern'
            else:
              patternSource = '1-Transaction Based Pattern'
            dfInputTransactions_Filter=fin_L1_TMP_JEBifurcation.filter((col("patternID") == 0))
            dfInputTransactions_Filter.createOrReplaceTempView("fin_InputTransactions_Filter")
            dfInputTransactions_Filter=dfInputTransactions_Filter\
                         .filter(~((F.col("debitAmount")== 0.00 )&(F.col("creditAmount")== 0.00 )))

            spark.sql("SELECT transactionID\
				           ,ntile(2) over (partition by TransactionId order by transactionLineIDKey)\
                                     as ntileRowNumber\
				           , debitCreditCode\
				           , transactionLineIDKey \
				       FROM fin_InputTransactions_Filter \
				      ").createOrReplaceTempView("actlResult") 
        
            spark.sql("SELECT transactionID,ntileRowNumber,debitCreditCode,\
			             SUM(CASE WHEN debitCreditCode = 'C' then 1 else 0 END) cnt_cr_all_block,\
			             SUM(CASE WHEN debitCreditCode = 'D' then 1 else 0 END) cnt_dr_all_block,\
			             Count(1) as lineCount,\
			             CASE WHEN	(MAX(debitCreditCode) OVER (PARTITION BY transactionid,ntileRowNumber) =\
						        MIN(debitCreditCode) OVER (PARTITION BY transactionid,ntileRowNumber))\
					          THEN	1\
					          ELSE	0\
					    END	isSymmetric\
			         FROM \
			            actlResult\
				    GROUP BY transactionID,ntileRowNumber,debitCreditCode")\
                             .createOrReplaceTempView("fin_L1_TMP_BlockSymmetricCheck")
        
            fin_L1_TMP_CrDrBlockTransaction=spark.sql("SELECT TransactionId,1 as crdrBlockFlg\
		                          FROM fin_L1_TMP_BlockSymmetricCheck\
		                          GROUP BY TransactionId having \
                                    MIN(isSymmetric) =1 \
                                    and SUM(lineCount)=SUM(cnt_cr_all_block)+SUM(cnt_dr_all_block) \
                                    and SUM(cnt_cr_all_block) = SUM(cnt_dr_all_block)")
        
            dfTransactions = dfInputTransactions_Filter\
                .groupBy("transactionID","isIrregularTransaction") \
                .agg(count("debitCreditCode").cast('int').alias("lineCount") ,\
                     count(F.when((F.col("debitCreditCode") == 'C'),1)).alias("cnt_cr_lines"), \
                     count(F.when((F.col("debitCreditCode") == 'D'),1)).alias("cnt_dr_lines"), \
                     countDistinct(F.when((F.col("debitCreditCode") == 'D'),F.col("debitAmount"))\
                                           .otherwise(F.col("creditAmount"))).alias("cnt_amount"),\
                     countDistinct(F.when((F.col("debitCreditCode") == 'D'),F.col("debitAmount"))).alias("cnt_dr_amount"), \
                     countDistinct(F.when((F.col("debitCreditCode") == 'D'),F.col("accountID"))).alias("cnt_dr_account"),\
                     countDistinct(F.when((F.col("debitCreditCode") == 'C'),F.col("creditAmount"))).alias("cnt_cr_amount"), \
                     countDistinct(F.when((F.col("debitCreditCode") == 'C'),F.col("accountID"))).alias("cnt_cr_account")
                 )
        
            dfTransactions = dfTransactions.alias("inp")\
                             .join(fin_L1_TMP_CrDrBlockTransaction.alias("flg"), \
                                  col("inp.transactionID") == col("flg.transactionID"),"left") \
                             .select(col("inp.*"),col("flg.crdrBlockFlg"))
            dfTransactions = dfTransactions.fillna({'crdrBlockFlg':0})
        

            fin_L1_JEBifurcation_Block_Based_Pattern_tag = dfTransactions.selectExpr("transactionID","CASE" \
                                             " WHEN cnt_dr_lines=1 " \
                                                 " AND cnt_cr_lines=1" \
                                                 " AND "+str(BlockType)+" in (0)	" \
                                                 " AND isIrregularTransaction = 0"\
                                                 " THEN 1 " \

                                             " WHEN cnt_dr_lines=1 " \
                                                 " AND cnt_cr_lines>1" \
                                                 " AND "+str(BlockType)+" in (0)" \
                                                 " AND isIrregularTransaction = 0"\
                                                 " THEN 2  " \

                                             "  WHEN cnt_dr_lines>1 " \
                                                  " AND cnt_cr_lines=1" \
                                                  " AND "+str(BlockType)+"  in (0)	" \
                                                  " AND isIrregularTransaction = 0"\
                                                  " THEN 3" \
                                             " WHEN cnt_dr_lines=1 " \
                                                 " AND cnt_cr_lines=1" \
                                                 " AND "+str(BlockType)+"  in (1,2,3)		THEN 11" \
                                             " WHEN cnt_dr_lines=1 " \
                                                 " AND cnt_cr_lines>1 " \
                                                 " AND "+str(BlockType)+"  in (1,2,3)		THEN 12" \

                                             " WHEN cnt_dr_lines>1 " \
                                                 " AND cnt_cr_lines=1" \
                                                 " AND "+str(BlockType)+"   in (1,2,3)		THEN 13" \

                                             " WHEN cnt_dr_lines>1" \
                                                 " AND cnt_cr_lines>1" \
                                                 " AND cnt_dr_lines=cnt_cr_lines" \
                                                 " AND (cnt_dr_account=1" \
                                                 " OR cnt_cr_account=1	)" \
                                                 " AND	cnt_dr_amount=cnt_cr_amount" \
                                                 " AND	cnt_dr_amount=cnt_amount" \
                                                 " AND crdrBlockFlg = 1			THEN 14" \

                                             " WHEN cnt_dr_lines>1" \
                                                 " AND cnt_cr_lines>1" \
                                                 " AND cnt_dr_lines=cnt_cr_lines" \
                                                 " AND cnt_dr_amount=cnt_cr_amount" \
                                                 " AND cnt_dr_amount=cnt_dr_lines" \
                                                 " AND cnt_cr_amount=cnt_cr_lines" \
                                                 " AND ((cnt_dr_account>1" \
                                                 " AND cnt_cr_account>1))" \
                                                 " AND cnt_dr_amount=cnt_amount" \
                                                 " AND crdrBlockFlg = 1			THEN 15" \

                                             " WHEN cnt_dr_lines>1 " \
                                                 " AND cnt_cr_lines>1 " \
                                                 " AND cnt_dr_lines=cnt_cr_lines " \
                                                 " AND cnt_dr_amount=cnt_cr_amount " \
                                                 " AND ((cnt_dr_account>1 " \
                                                 " AND cnt_cr_account>1))" \
                                                 " AND cnt_dr_amount=cnt_amount " \
                                                 " AND crdrBlockFlg = 1			THEN 16" \

                                             " WHEN cnt_dr_lines>1" \
                                                 " AND cnt_cr_lines>1 " \
                                                 " AND cnt_dr_lines=cnt_cr_lines" \
                                                 " AND cnt_dr_account=1 " \
                                                 " AND cnt_cr_account=1" \
                                                 " AND cnt_dr_amount=cnt_dr_lines" \
                                                 " AND cnt_cr_amount=cnt_cr_lines" \
                                                 " AND cnt_dr_amount=cnt_cr_amount" \
                                                 " AND cnt_dr_amount=cnt_amount" \
                                                 " AND "+str(BlockType)+"  in (3)			THEN 4" \

                                             " WHEN cnt_dr_lines>1 " \
                                                 " AND cnt_cr_lines>1 " \
                                                 " AND cnt_dr_lines=cnt_cr_lines " \
                                                 " AND cnt_dr_amount=cnt_cr_amount" \
                                                 " AND cnt_dr_amount<>cnt_dr_lines" \
                                                 " AND cnt_cr_amount<>cnt_cr_lines" \
                                                 " AND ((cnt_dr_account>1 " \
                                                 " AND cnt_cr_account=1)" \
                                                 " OR (cnt_dr_account=1 " \
                                                 " AND cnt_cr_account>1))" \
                                                 " AND cnt_dr_amount=cnt_amount	" \
                                                 " AND  "+str(BlockType)+"  in (3)			THEN 7" \

                                             " WHEN cnt_dr_lines>1 " \
                                                 " AND cnt_cr_lines>1 " \
                                                 " AND cnt_dr_lines=cnt_cr_lines" \
                                                 " AND cnt_dr_account=1 " \
                                                 " AND cnt_cr_account=1" \
                                                 " AND cnt_dr_amount<>cnt_dr_lines " \
                                                 " AND cnt_cr_amount<>cnt_cr_lines " \
                                                 " AND cnt_dr_amount=cnt_cr_amount" \
                                                 " AND cnt_dr_amount=cnt_amount	" \
                                                 " AND "+str(BlockType)+" in (3)			THEN 5" \

                                             " WHEN cnt_dr_lines>1"\
                                                 " AND cnt_cr_lines>1" \
                                                 " AND cnt_dr_lines=cnt_cr_lines " \
                                                 " AND cnt_dr_amount=cnt_cr_amount" \
                                                 " AND cnt_dr_amount=cnt_dr_lines" \
                                                 " AND cnt_cr_amount=cnt_cr_lines" \
                                                 " AND ((cnt_dr_account>1 " \
                                                 " AND cnt_cr_account=1) " \
                                                 " OR (cnt_dr_account=1 " \
                                                 " AND cnt_cr_account>1))" \
                                                 " AND	cnt_dr_amount=cnt_amount	" \
                                                 " AND "+str(BlockType)+" in (3)			THEN 6" \

                                             " WHEN cnt_dr_lines>1 " \
                                                 " AND cnt_cr_lines>1 " \
                                                 " AND cnt_dr_lines=cnt_cr_lines " \
                                                 " AND cnt_dr_amount=cnt_cr_amount" \
                                                 " AND cnt_dr_amount=cnt_dr_lines" \
                                                 " AND cnt_cr_amount=cnt_cr_lines" \
                                                 " AND ((cnt_dr_account>1 " \
                                                 "     AND cnt_cr_account>1))" \
                                                 " AND cnt_dr_amount=cnt_amount" \
                                                 " AND  "+str(BlockType)+" in (3)			THEN 8" \

                                             " WHEN cnt_dr_lines>1 AND cnt_cr_lines>1 AND " \
                                                 " cnt_dr_lines=cnt_cr_lines AND " \
                                                 " cnt_dr_amount=cnt_cr_amount AND" \
                                                 " cnt_dr_amount<>cnt_dr_lines AND" \
                                                 " cnt_cr_amount<>cnt_cr_lines AND" \
                                                 " cnt_dr_account>1 AND " \
                                                 " cnt_cr_account>1 AND" \
                                                 " cnt_dr_amount=cnt_amount" \
                                                 " AND "+str(BlockType)+" in (3)		THEN 9" \

                                             " WHEN cnt_dr_lines>1 AND cnt_cr_lines>1 AND" \
                                                 " (cnt_dr_account =1 OR cnt_cr_account=1) AND" \
                                                 " ( cnt_dr_amount <> cnt_cr_amount )" \
                                                 " AND "+str(BlockType)+" in (3)			THEN  10" \


                                        " 	ELSE 0 END as JE_Pattern_ID"\
                                        ) 
        
            dfInput = DeltaTable.forPath(spark, gl_preBifurcationPath+"fin_L1_TMP_JEBifurcation.delta")
            dfInput.alias("inp")\
               .merge(source=fin_L1_JEBifurcation_Block_Based_Pattern_tag\
                      .filter(col("JE_Pattern_ID")!=lit(0)).alias("pat"),condition="inp.transactionID = pat.transactionID") \
               .whenMatchedUpdate(set = {"patternID": "pat.JE_Pattern_ID","patternSource": lit(patternSource)}).execute() 
            
        except Exception as err:
           raise
