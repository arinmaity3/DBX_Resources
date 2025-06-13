# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import ceil, col,length,format_number,row_number,lit, when, concat
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame

def gen_L2_DIM_Bracket_populate():
  """ Populate L2_DIM_Bracket """
  try:  
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))   
        #spark.sql("UNCACHE TABLE  IF EXISTS gen_L2_DIM_Bracket")
        #spark.sql("UNCACHE TABLE  IF EXISTS gen_L2_DIM_BracketText")
        global gen_L2_DIM_Bracket
        global gen_L2_DIM_BracketText
  
        knw_LK_GD_Bracket = objGenHelper.gen_readFromFile_perform(\
                                gl_knowledgePath + "knw_LK_GD_Bracket.delta")
        df1 =  spark.sql('Select fiscalYear,companyCode,documentNumber, '
            ' SUM(CASE WHEN debitCreditIndicator = "D" THEN amountLC ELSE 0.00 END) debitAmount'
			' FROM fin_L1_TD_Journal'
			' GROUP BY fiscalYear'
					',companyCode'
					',documentNumber')
  
        #Required variables 
        minAmount=str(df1.select(ceil(min('debitAmount')).alias("minAmount")).rdd.max()[0])
        maxAmount=str(df1.select(ceil(max('debitAmount')).alias("maxAmount")).rdd.max()[0])
        if minAmount == 'None':
            minAmount = '0'
        if maxAmount == 'None':
            maxAmount = '1000'
        maxlength= str(len(maxAmount))
        maxLimit = "9" * int(maxlength)
        languageCode = knw_LK_CD_ReportingSetup.select(col("KPMGDataReportingLanguage")).collect()[0][0]
        if int(maxAmount[0])>=5:
            firstAmount = "5" + "0" * (int(maxlength)-1)

        else:
            firstAmount = "1" + "0" * (int(maxlength)-1)

        firstAmount = int(firstAmount)
        ls_id = [1,2,3,4,5,6,7,8]
        ls_prc = [100,50,25,12,6,4,2,1]
        ls_char = ['A','B','C','D','E','F','G','H']
        ls_char.sort(reverse=True)
        ls_amountfrom = [int(firstAmount*i/100) if i != 1 else 0 for i in ls_prc]
        ls_amountTO = [ls_amountfrom[ls_amountfrom.index(i)-1] if ls_amountfrom.index(i)!=0 \
                                                            else int(maxAmount) for i in ls_amountfrom]
        ls_id.sort(reverse=True)
        a = list(zip(ls_id,ls_amountfrom,ls_amountTO,ls_char,ls_prc))
        schema = StructType([
          StructField('ID', IntegerType(), True),
          StructField('lowerLimit', LongType(), True),
          StructField('upperLimit', LongType(), True),
          StructField('bracketSize',StringType(),True),
          StructField('calcPercentage', IntegerType(), True)
        ])

       # Convert list to RDD
        rdd = spark.sparkContext.parallelize(a)

       # Create data frame
        df = spark.createDataFrame(a,schema)

        df = df.withColumn("upperlmt", regexp_replace(format_number("upperLimit",0), ",", ","))
        df = df.withColumn("lowerlmt", regexp_replace(format_number("lowerLimit",0), ",", ","))

        column_bracketTest = when(col("ID")== 1 , concat_ws(" - ",lit("lower").cast("String"),col('upperlmt')))\
                        .when(col("ID")== 8 , concat_ws(" - ",col("lowerlmt").cast("String"),lit('Above')))\
                        .otherwise(concat_ws(" - ",col('lowerlmt'),col("upperlmt").cast("String")))

        df2 = df.withColumn("bracketText", column_bracketTest)

        df2 = df.withColumn("bracketText", column_bracketTest)\
           .withColumn('bracketType',lit('JE_AMOUNT_RANGE'))\
           .withColumn('languageCode',lit('EN')) 

        df_JE_BRACKET = df2.select(col('lowerLimit'),col('upperLimit'),col('bracketText'),\
                        lit("#NA#").alias('bracketTextSubGroup'),lit("#NA#").alias('bracketTextGroup'),\
                        col('bracketSize'),col('bracketType'),col('languageCode'))
   
        df_LK_GD_Bracket = knw_LK_GD_Bracket.select(col('lowerLimit'),col('upperLimit'),\
                           col('bracketText'),lit("#NA#").alias('bracketTextSubGroup'),\
                           lit("#NA#").alias('bracketTextGroup'),col('bracketSize'),\
                            col('bracketType'),col('languageCode'))
  
        dfs = [df_JE_BRACKET,df_LK_GD_Bracket]
        JE_BRACKET = reduce(DataFrame.union, dfs)
        JE_BRACKET.createOrReplaceTempView("knw_JE_BRACKET")
        sqlContext.cacheTable('knw_JE_BRACKET') 
  
        gen_L2_DIM_Bracket = spark.sql(" select distinct lowerLimit,upperlimit,0 as phaseid,0 as sortid \
                                FROM knw_JE_BRACKET GROUP BY upperlimit, lowerLimit \
		                        ORDER BY upperlimit, lowerLimit ")
        w = Window().orderBy(lit('bracketSurrogateKey'))
        gen_L2_DIM_Bracket = gen_L2_DIM_Bracket.withColumn("bracketSurrogateKey", row_number().over(w))\
                                               .withColumn("analysisID",lit(analysisid))
        gen_L2_DIM_Bracket = objDataTransformation.gen_convertToCDMandCache \
            (gen_L2_DIM_Bracket,'gen','L2_DIM_Bracket',True)
    
        gen_L2_DIM_BracketText = spark.sql("SELECT \
				  bracketSurrogateKey\
                 ,brck.bracketType\
				 ,brck.bracketText\
                 ,if(brck.bracketTextSubGroup IS NULL,brck.bracketText,brck.bracketTextSubGroup)\
                         as bracketTextSubGroup\
                 ,if(brck.bracketTextGroup IS NULL,brck.bracketText,brck.bracketTextGroup) \
                         as bracketTextGroup\
                 ,bracketSize\
                 ,if('" + languageCode + "' IS NULL,brck.languageCode,'" + languageCode + "')\
                         as languageKey\
                 ,0 as phaseID\
            FROM knw_JE_BRACKET brck\
            JOIN	gen_L2_DIM_Bracket	brk2\
                ON	brk2.lowerLimit 		=	brck.lowerLimit\
                AND	brk2.upperLimit			=   brck.upperLimit")

        w = Window().orderBy(lit('bracketTextSurrogateKey'))
        gen_L2_DIM_BracketText = gen_L2_DIM_BracketText.withColumn("bracketTextSurrogateKey", row_number().over(w))\
                                                       .withColumn("analysisID",lit(analysisid))
        
        gen_L2_DIM_BracketText = objDataTransformation.gen_convertToCDMandCache \
            (gen_L2_DIM_BracketText,'gen','L2_DIM_BracketText',True)
        
        #Write to parquet file based on dwh schema
        dwh_vw_DIM_Bracket = gen_L2_DIM_Bracket.alias('brck')\
                .join(gen_L2_DIM_BracketText.alias('brckText'), \
                           (col('brck.bracketSurrogateKey') == col('brckText.bracketSurrogateKey')),'inner')\
                .select(lit(analysisid).alias("analysisID"),\
                        col('brck.bracketSurrogateKey').alias('bracketSurrogateKey')\
                        ,col('brckText.bracketType').alias('bracketType')\
                        ,col('brckText.bracketText').alias('bracketText')\
                        ,col('brckText.bracketSize').alias('bracketSize')\
                        ,when(col('brck.sortID').isNotNull(),col('brck.sortID')).otherwise(lit(0)).alias('sortID')\
                       ) \
                 .filter(col("brckText.bracketType").isin('JE_LINE_RANGE','JE_AMOUNT_RANGE'))
        objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_Bracket,gl_CDMLayer2Path +"gen_L2_DIM_Bracket.parquet")
               
        executionStatus = "L2_DIM_Bracket populated sucessfully."
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        
  except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

  finally:
    spark.sql("UNCACHE TABLE  IF EXISTS knw_JE_BRACKET")

                       
