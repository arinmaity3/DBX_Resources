# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import col 
from pyspark.sql.window import Window 
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,DateType,ShortType,IntegerType,DoubleType

def fin_L1_STG_GLAccountCategoryMappingHierarchy_populate():
  """Populate GL account category, hierarchy"""
  try:
    
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    w = Window().orderBy('sortOrder2')   
    
    global fin_L1_STG_GLAccountCategoryMaster
    global fin_L1_STG_GLAccountFlattenHierarchy
    global fin_L1_STG_GLAccountMapping
    
    knw_LK_GD_DAFlagFieldMapping = objGenHelper.gen_readFromFile_perform(\
                                gl_knowledgePath + "knw_LK_GD_DAFlagFieldMapping.delta")

    scopedAnalytics=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'SCOPED_ANALYTICS') #
                  
    #populate GL account category master
    fin_L1_STG_GLAccountCategoryMaster = knw_LK_CD_KnowledgeAccountSnapshot.alias("KA").\
             join(knw_LK_CD_FinancialStructureDetailSnapshot.alias("FSD"),\
             [col("FSD.knowledgeAccountNodeID") == col("KA.knowledgeAccountID")],how='inner').\
             select(col("KA.knowledgeAccountID").alias("identifier"),\
             coalesce(col("KA.displayName"),col("KA.knowledgeAccountName")).alias("accountCategory"),\
             col("KA.financialStructureType"),col("FSD.sortOrder").cast("double").alias("sortOrder2"))\
             .distinct().\
             withColumn("glAccountCategoryMasterSurrogateKey", row_number().over(w)).\
             withColumn("sortOrder",lpad(row_number().over(w),7,'0')).drop("sortOrder2") 
    
    #populate GL TMP hierarchy
    w = Window().orderBy('accountCategory')
    w2 = Window().orderBy('sortOrder2')
      
    fin_L1_TMP_GLAccountHierarchy = knw_LK_CD_FinancialStructureDetailSnapshot.alias("FSD").\
            join(fin_L1_STG_GLAccountCategoryMaster.alias("GLCM"),\
            [col("FSD.knowledgeAccountNodeID") == col("GLCM.identifier")],how="inner").\
            join(fin_L1_STG_GLAccountCategoryMaster.alias("GLCM2").\
            select(col("GLCM2.identifier").alias("identifier2"),\
            col("GLCM2.glAccountCategoryMasterSurrogateKey").alias("parentID2")),\
            [col("FSD.knowledgeAccountParentID").eqNullSafe(col("identifier2"))],how="left").\
            select((col("GLCM.glAccountCategoryMasterSurrogateKey")).alias("nodeID"),\
            col("GLCM.identifier"),col("GLCM.accountCategory"),col("parentID2"),\
            col("GLCM.financialStructureType"),col("FSD.sortOrder").alias("sortOrder2").cast("double")).\
            withColumn("ID",row_number().over(w)).\
            withColumn("sortOrder",lpad(row_number().over(w2),7,'0')).drop("sortOrder2") 
    
    
    
    #adding place holder to hierarchy
    if ((not(knw_LK_CD_KnowledgeAccountMapping.rdd.isEmpty())) & \
        (fin_L1_TMP_GLAccountHierarchy.filter(col("financialStructureType") == 0).rdd.isEmpty())):
      
      maxNodeID = fin_L1_TMP_GLAccountHierarchy.agg({"nodeID": "max"}).collect()[0][0]  
      fin_L1_TMP_GLAccountHierarchy = fin_L1_TMP_GLAccountHierarchy.withColumn("parentID", \
                      when((col("parentID2").isNull() & \
                           (col("financialStructureType").isNotNull())) == True,\
                      col("financialStructureType") + lit(maxNodeID)).\
                      otherwise(col("parentID2")).cast("int")).drop("parentID2")
  
      maxIDs = fin_L1_TMP_GLAccountHierarchy.agg({"nodeID": "max","ID" : "max"}).rdd.collect()[0]
      ID = maxIDs[0] 
      nodeID = maxIDs[1] 
      
      glAccountHierarchySchema =  StructType([StructField("nodeID",ShortType(),True),
                                       StructField("identifier",StringType(),True),
                                       StructField("accountCategory",StringType(),True),
                                       StructField("financialStructureType",IntegerType(),True),
                                       StructField("ID",IntegerType(),True),
                                       StructField("sortOrder",StringType(),True),
                                       StructField("parentID",IntegerType(),True)])
      
      lstCaptions = [[1 + nodeID,1,'Balance Sheet',1,1 + ID,'0000001',None],
                     [2 + nodeID,2,'Income Statement',2, 2 + ID,'0000002',None],
                     [3 + nodeID,3,'Disclosures',3,3 + ID,'0000003',None]]
  
      fin_L1_TMP_GLAccountHierarchy = fin_L1_TMP_GLAccountHierarchy.\
                                      unionAll\
            (spark.createDataFrame(data = lstCaptions,schema = glAccountHierarchySchema))
    
    #creating flattern hierachy
    iscolexists=fin_L1_TMP_GLAccountHierarchy.schema.simpleString().find('parentID:')
    if(iscolexists) <0:
      fin_L1_TMP_GLAccountHierarchy=fin_L1_TMP_GLAccountHierarchy.withColumn("parentID",col("parentID2"))

    colsList = ['level1','level2','level3','level4','level5','level6','level7','level8','level9']    
    expression = '+'.join(colsList)
    
    fin_L1_TMP_GLAccountHierarchy.createOrReplaceTempView('GLAH')        
    fin_L1_STG_GLAccountFlattenHierarchy = spark.sql("SELECT L1.accountCategory	AS accountName_01\
           ,L2.accountCategory		AS accountName_02\
           ,L3.accountCategory		AS accountName_03\
           ,L4.accountCategory		AS accountName_04\
           ,L5.accountCategory		AS accountName_05\
           ,L6.accountCategory		AS accountName_06\
           ,L7.accountCategory		AS accountName_07\
           ,L8.accountCategory		AS accountName_08\
           ,L9.accountCategory		AS accountName_09\
           ,CASE WHEN L1.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level1\
           ,CASE WHEN L2.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level2\
           ,CASE WHEN L3.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level3\
           ,CASE WHEN L4.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level4\
           ,CASE WHEN L5.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level5\
           ,CASE WHEN L6.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level6\
           ,CASE WHEN L7.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level7\
           ,CASE WHEN L8.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level8\
           ,CASE WHEN L9.accountCategory IS NOT NULL THEN 1 ELSE 0 END AS level9\
           ,L1.identifier           AS Identifier_01\
           ,L2.identifier           AS Identifier_02\
           ,L3.identifier           AS Identifier_03\
           ,L4.identifier           AS Identifier_04\
           ,L5.identifier           AS Identifier_05\
           ,L6.identifier           AS Identifier_06\
           ,L7.identifier           AS Identifier_07\
           ,L8.identifier           AS Identifier_08\
           ,L9.identifier           AS Identifier_09\
           ,L1.sortOrder            AS sortOrder_01\
           ,L2.sortOrder            AS sortOrder_02\
           ,L3.sortOrder            AS sortOrder_03\
           ,L4.sortOrder            AS sortOrder_04\
           ,L5.sortOrder            AS sortOrder_05\
           ,L6.sortOrder            AS sortOrder_06\
           ,L7.sortOrder            AS sortOrder_07\
           ,L8.sortOrder            AS sortOrder_08\
           ,L9.sortOrder            AS sortOrder_09\
           ,L1.nodeID 			    AS nodeID_01\
           ,L2.nodeID 			    AS nodeID_02\
           ,L3.nodeID 			    AS nodeID_03\
           ,L4.nodeID 			    AS nodeID_04\
           ,L5.nodeID 			    AS nodeID_05\
           ,L6.nodeID 			    AS nodeID_06\
           ,L7.nodeID 			    AS nodeID_07\
           ,L8.nodeID 			    AS nodeID_08\
           ,L1.nodeID               AS LeafID\
           FROM GLAH L1 \
                 LEFT JOIN GLAH L2 ON L1.parentID = L2.nodeID \
                 LEFT JOIN GLAH L3 ON L2.parentID = L3.nodeID \
                 LEFT JOIN GLAH L4 ON L3.parentID = L4.nodeID \
                 LEFT JOIN GLAH L5 ON L4.parentID = L5.nodeID \
                 LEFT JOIN GLAH L6 ON L5.parentID = L6.nodeID \
                 LEFT JOIN GLAH L7 ON L6.parentID = L7.nodeID \
                 LEFT JOIN GLAH L8 ON L7.parentID = L8.nodeID \
                 LEFT JOIN GLAH L9 ON L8.parentID = L9.nodeID")

    fin_L1_STG_GLAccountFlattenHierarchy = fin_L1_STG_GLAccountFlattenHierarchy.\
                                           withColumn("level",expr(expression)).drop(*colsList)
    
  
    #update hierachy level
    colMasterList = list()
    colMasterList = ["accountName_","Identifier_","sortOrder_"]
    expression = "CASE $ WHEN level THEN  {0}  WHEN level-1 THEN {1}  WHEN level-2 THEN {2} WHEN level-3 THEN {3}\
                  WHEN level-4  THEN {4} WHEN level-5 THEN {5} WHEN level-6 THEN {6} WHEN level-7 THEN {7} \
                  WHEN level-8 THEN {8} END"
    oldColumnList = list()
    
    for i in range(0,3):
      colName = list()  
      [colName.append(c)for c in fin_L1_STG_GLAccountFlattenHierarchy.columns if c.startswith(colMasterList[i])]    
      for j in range(0,9):
        oldColumnList.append(colName[j])
        colExpr = expression.format(*colName).replace('$',str(j+1))
        fin_L1_STG_GLAccountFlattenHierarchy = fin_L1_STG_GLAccountFlattenHierarchy.\
                                               withColumn(colName[j]+ "_new",expr(colExpr))    

    fin_L1_STG_GLAccountFlattenHierarchy = fin_L1_STG_GLAccountFlattenHierarchy.drop(*oldColumnList)
    
    for i in oldColumnList:
      fin_L1_STG_GLAccountFlattenHierarchy = fin_L1_STG_GLAccountFlattenHierarchy.withColumnRenamed(i+ "_new",i)

    fin_L1_STG_GLAccountFlattenHierarchy  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_GLAccountFlattenHierarchy,'fin','L1_STG_GLAccountFlattenHierarchy',False)
    
    #populate DA flags
    
    w = Window().orderBy('accountCategoryID')
    fin_L1_STG_GLAccountMapping = knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot.alias("FSDTOGL").\
                  join(knw_LK_CD_FinancialStructureDetailSnapshot.alias("FSD"),\
                  [col("FSDTOGL.financialStructureDetailID") == col("FSD.financialStructureDetailID")],how='inner').\
                  join(knw_LK_CD_KnowledgeAccountSnapshot.alias("KA"),\
                  [col("KA.knowledgeAccountID") == col("FSD.knowledgeAccountNodeID")],how='inner').\
                  join(fin_L1_STG_GLAccountCategoryMaster.alias("GLCM"),\
                  [col("GLCM.identifier").eqNullSafe(col("KA.knowledgeAccountID"))],how='left').\
                  select(col("glAccountNumber").alias("accountNumber"),\
                  col("glAccountCategoryMasterSurrogateKey").alias("accountCategoryID")).\
                  distinct().\
                  withColumn("glAccountCategoryMappingSurrogateKey",row_number().over(w)).\
                  withColumn("isScopedforBifurcation",lit(True))

    
    dfFlagsMapped = fin_L1_STG_GLAccountMapping.alias("GLAM").\
                join(fin_L1_STG_GLAccountCategoryMaster.alias("GLCM"),\
                [col("GLAM.accountCategoryID") == col("GLCM.glAccountCategoryMasterSurrogateKey")],how='inner').\
                join(fin_L1_STG_GLAccountFlattenHierarchy.alias("FH"),\
                [col("FH.LeafID") == col("GLAM.accountCategoryID")],how='inner').\
                join(knw_LK_CD_KnowledgeAccountMapping.alias("KAM"),\
                [col("KAM.KnowledgeUniqueID").isin(col("Identifier_01"),col("Identifier_02"),\
                             col("Identifier_03"),col("Identifier_03"),\
                             col('Identifier_04'),col('Identifier_05'),\
                             col('Identifier_06'),col('Identifier_07'),\
                             col('Identifier_08'),col('Identifier_09'))]).\
                join(knw_LK_GD_DAFlagKnowledgeAccountMappingDetail.alias("DAFLAG"),\
                [col("DAFLAG.knowledgeID").eqNullSafe(col("KAM.KnowledgeID"))],how='left').\
                select(col("flagName"),col("KnowledgeUniqueID").alias("knowledgeAccountID")).\
                filter((col("KAM.KnowledgeID") != 0) & (col("KAM.KnowledgeID").isNotNull())).distinct()                    
    
    dfDAFlagMaster =  knw_LK_GD_DAFlagFieldMapping.join(dfFlagsMapped,\
                      [dfFlagsMapped.flagName == knw_LK_GD_DAFlagFieldMapping.DAFlag],how='left')
    
    dfDAFlagMaster = dfDAFlagMaster.groupBy("DAFlagFieldMapping").\
                     agg(collect_set('knowledgeAccountID').alias('knowledgeAccountID'))  


    if (knw_LK_CD_knowledgeAccountScopingDetail.first() is None):
        fin_L1_STG_GLAccountMapping =  fin_L1_STG_GLAccountMapping.alias("GLAM").\
                                       join(fin_L1_STG_GLAccountCategoryMaster.alias("GLCM"),\
                                       [col("GLAM.accountCategoryID") == col("GLCM.glAccountCategoryMasterSurrogateKey")],how='inner').\
                                       select(col("glAccountCategoryMappingSurrogateKey"),\
                                              col("accountNumber"),\
                                              col("accountCategoryID"),\
                                              col("isScopedforBifurcation"),\
                                              col("identifier"))
    else:
        dfScopedAccounts =  fin_L1_STG_GLAccountMapping.alias("GLAM").\
                             join(fin_L1_STG_GLAccountCategoryMaster.alias("GLCM"),\
                             [col("GLAM.accountCategoryID") == col("GLCM.glAccountCategoryMasterSurrogateKey")],how='inner').\
                             join(fin_L1_STG_GLAccountFlattenHierarchy.alias("GLAFH"),\
                             [col("GLAFH.leafID").eqNullSafe(col("GLAM.accountCategoryID"))],how='left').\
                             crossJoin(knw_LK_CD_knowledgeAccountScopingDetail.alias("SCD")).\
                             filter((col("SCD.isScoped")==1) & (col("SCD.knowledgeAccountID").isin(col("Identifier_01"),col("Identifier_02"),\
                                         col("Identifier_03"),col("Identifier_03"),\
                                         col('Identifier_04'),col('Identifier_05'),\
                                         col('Identifier_06'),col('Identifier_07'),\
                                         col('Identifier_08'),col('Identifier_09')))).\
                             select(col("glAccountCategoryMappingSurrogateKey")).distinct()
        
        fin_L1_STG_GLAccountMapping = fin_L1_STG_GLAccountMapping.alias("GLAM").\
                                     join(fin_L1_STG_GLAccountCategoryMaster.alias("GLCM"),\
                                     [col("GLAM.accountCategoryID") == col("GLCM.glAccountCategoryMasterSurrogateKey")],how='inner').\
                                     join(dfScopedAccounts.alias("SA"),\
                                     [col("SA.glAccountCategoryMappingSurrogateKey").\
                                     eqNullSafe(col("GLAM.glAccountCategoryMappingSurrogateKey"))],how='left').\
                                     select(col("GLAM.glAccountCategoryMappingSurrogateKey"),\
                                            col("GLAM.accountNumber"),\
                                            col("GLAM.accountCategoryID"),\
                                            expr("CASE WHEN SA.glAccountCategoryMappingSurrogateKey IS NULL \
                                                       THEN False \
                                                       ELSE True END").alias("isScopedforBifurcation"),\
                                            col("GLCM.identifier"))


    
    for flag in dfDAFlagMaster.rdd.collect():    
      knowledgeIDs = ''  
      if (len(flag[1]) != 0):
        knowledgeIDs = str(flag[1]).replace('[','(').replace(']',')')
        expression = 'CASE WHEN identifier in ' + knowledgeIDs +  'THEN 1 ELSE 0 END'
        fin_L1_STG_GLAccountMapping = fin_L1_STG_GLAccountMapping.withColumn(flag[0],expr(expression))
      else:
        fin_L1_STG_GLAccountMapping = fin_L1_STG_GLAccountMapping.withColumn(flag[0],lit(0))
    
    fin_L1_STG_GLAccountMapping  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_GLAccountMapping,'fin','L1_STG_GLAccountMapping',False)
    
    fin_L1_STG_GLAccountCategoryMaster  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_GLAccountCategoryMaster,'fin','L1_STG_GLAccountCategoryMaster',False)

    executionStatus = "L1_STG_GLAccountCategoryMappingHierarchy populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
  
