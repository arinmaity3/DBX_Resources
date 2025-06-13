# Databricks notebook source
def updateInputDeltaFile(aggr=0):
        try:
           
            if aggr==0:
               inputfilepath=gl_bifurcationPath+"fin_L1_TMP_JEBifurcation.delta"
            else:
               inputfilepath=gl_bifurcationPath+"fin_L1_TMP_JEBifurcation_Accountwise_Agg.delta"
        
            fin_L1_STG_JEBifurcation_Link = spark.read.format("delta")\
                .load(gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")\
                .filter(col('dataType') == lit('O')).select(col("journalSurrogateKey"))\
                .distinct()
            df_input = DeltaTable.forPath(spark, inputfilepath)
            df_input.alias("inp")\
                .merge(source=fin_L1_STG_JEBifurcation_Link.alias("link"),condition="inp.journalSurrogateKey = link.journalSurrogateKey") \
                .whenMatchedDelete(condition = lit(True)).execute() 
        except Exception as err:
             raise



