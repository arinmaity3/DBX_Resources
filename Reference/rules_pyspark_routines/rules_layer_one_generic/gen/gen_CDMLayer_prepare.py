# Databricks notebook source
from pyspark.sql.functions import expr, concat, col, trim,when
from pyspark.sql.types import StringType,DateType,ShortType
from pyspark.sql.window import Window 
import uuid
  
class gen_CDMLayer_prepare():   
  def gen_convertToCDMLayer1_prepare(self):    
    try:
       
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      global fin_L1_MD_GLAccount,fin_L1_TD_Journal,fin_L1_TD_GLBalance,fin_L1_TD_GLBalancePeriodic
      
      print('CDM conversion process started...')

      CDMFileType = ['GLA','GLAB','JET','GLTB']
      gl_metadataDictionary["dic_ddic_column"]\
          .select(col("fileType"),col("tableName2"),col("columnName"),\
                  col("dataType"),col("fieldLength"),\
                  col("position"),col("sparkDataType"),\
                  col("schemaName"),col("tableName"))\
          .filter(col("fileType").isin(CDMFileType))\
          .createOrReplaceTempView("dic_column")
      
      if(PROCESS_STAGE.IMPORT in gl_processStage): #Normal flow
          for sourceFile in gl_lstOfSourceFiles:
            for fileType,dfRawFile in gl_lstOfSourceFiles[sourceFile].items():          
              gl_parameterDictionary["SourceTargetColumnMapping"].\
                              filter(upper(col("fileID")) == sourceFile).createOrReplaceTempView("colMapping")
              lsRawFilecols = ', '.join(["'{}'".format(eachcol.strip().upper()) for eachcol in dfRawFile.columns])
              dfMapping = spark.sql("SELECT A.tableName2,A.columnName,A.dataType,A.fieldLength,A.position,\
                                   B.sourceColumn,B.targetColumn,A.sparkDataType, \
                                    CASE WHEN UPPER(B.targetColumn) IN ("+lsRawFilecols+") THEN 1 ELSE 0 END AS isTargetColInRawfile\
                                    FROM dic_column A \
                                    LEFT JOIN colMapping B ON A.columnName = B.targetColumn \
                                    WHERE A.fileType = " + "'" + fileType + "' ORDER BY B.sourceColumn DESC,A.position")          
              if(fileType == "JET" or fileType == 'GLAB' or fileType == 'GLA' or fileType == 'GLTB'):
                for row in dfMapping.rdd.collect():
                  if row["targetColumn"] != None:                
                    if ((row["columnName"].strip() == 'companyCode') |((row["columnName"] == 'documentStatus') and fileType == "JET")):
                      dfRawFile = self.__gen_deriveColumn_perform(row["columnName"],dfRawFile,row,fileType)
                    elif row["sourceColumn"].strip().upper()!=row["targetColumn"].strip().upper() and row["isTargetColInRawfile"]==1:
                           dfRawFile = dfRawFile.withColumnRenamed(row["sourceColumn"],row["columnName"]+"_CONVERTD")
                    else:
                           dfRawFile = dfRawFile.withColumnRenamed(row["sourceColumn"],row["columnName"])
                  elif row["targetColumn"] == None:
                    if ((row["columnName"] == 'companyCode') |((row["columnName"] == 'documentStatus') and fileType == "JET")):
                      dfRawFile = self.__gen_deriveColumn_perform(row["columnName"],dfRawFile,row,fileType,False)
                    else:
                      dfRawFile = dfRawFile.withColumn(row["columnName"],trim(lit(None)))  
              for dfrawFilecol in  dfRawFile.columns:
                  if dfrawFilecol.endswith("_CONVERTD"):
                   dfRawFile = dfRawFile.withColumn(dfrawFilecol.replace("_CONVERTD",''),col(dfrawFilecol))\
                                        .drop(dfrawFilecol)

            dfFile =  gl_parameterDictionary['InputParams'].filter(upper(col("fileID"))==sourceFile)\
                        .select(col("fileName"),col("fileType")).first()
            
            fileName = dfFile[0]
            fileType = dfFile[1]
           
            tab = spark.sql("select distinct schemaName,tableName \
                    from dic_column where upper(fileType) = '" + fileType.upper() + "'").first()
         
            schemaName = tab[0]
            tableName = tab[1]
            
            #gen_CDMDataTypeValidation_perform -- part of max data length validation
            dtStatus = gen_CDMDataTypeValidation_perform(dfRawFile,
                                                         fileName,
                                                         sourceFile,
                                                         fileType,
                                                         gl_ERPSystemID,
                                                         schemaName,tableName,isCDM=True)
            
            dfRawFile.createOrReplaceTempView("cdm_staging")           
            CDMcols = objDataTransformation.gen_convertToCDMStructure_get(dfMapping.orderBy(col("position")))
        
            sqlQuery = "SELECT " + CDMcols + " FROM cdm_staging"            
            dfCDML1 = spark.sql(sqlQuery)                 
            self.__gen_CDMLayer1_popuate(dfCDML1,fileType)

      else: #dataflow         
         self.__dataFlowTransformation_perform()         
                                    
      if(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'BALANCE_TYPE') == 'GLTB'):
        fin_L1_TD_GLBalance = self.__gen_transformGLTBFile_perform('GLAB')     
        fin_L1_MD_GLAccount = self.__gen_transformGLTBFile_perform('GLA')     
      if(fin_L1_TD_Journal.count() > 0):        
        self.__gen_L1_TD_Journal_derive()        
      if(fin_L1_TD_GLBalance.count() > 0):
        self.__gen_L1_TD_GLBalance_derive()   
                                      
              
      fin_L1_MD_GLAccount = objDataTransformation.gen_CDMColumnOrder_get(fin_L1_MD_GLAccount,'fin','L1_MD_GLAccount')
      fin_L1_TD_GLBalance = objDataTransformation.gen_CDMColumnOrder_get(fin_L1_TD_GLBalance,'fin','L1_TD_GLBalance')      
      fin_L1_TD_Journal = objDataTransformation.gen_CDMColumnOrder_get(fin_L1_TD_Journal,'fin','L1_TD_Journal')
                                    
      del objGenHelper 
      print('CDM conversion process completed...')
    except Exception as err:
          raise 
  
  def __gen_transformGLTBFile_perform(self,fType):
    try:
      global fin_L1_TD_GLBalancePeriodic,fin_L1_MD_GLAccount
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      
      if(fType =='GLAB'):
        unpivotExpr = "stack(17, '-1', openingBalance, '1', period01, '2', period02, '3', period03,'4', period04, '5',period05,'6',\
                      period06, '7',period07,'8',	period08,'9',period09,'10',period10,'11',period11,'12',\
                      period12,'13',period13,'14',period14,'15',period15,'16',period16)as (financialPeriod,endingBalanceLC)"
        
        dfGLTBTransformed = fin_L1_TD_GLBalancePeriodic.select("companyCode","accountNumber","fiscalYear","debitCreditIndicator", expr(unpivotExpr)) 
                            #\.where("endingBalanceLC is not null")
        dfGLTBTransformed = dfGLTBTransformed.withColumn("financialPeriod",col("financialPeriod").cast("short"))
        for cols in gl_metadataDictionary["dic_ddic_column"].select(col("columnName")). \
                  filter((col("fileType") == 'GLAB') &  (col("columnName").isin(dfGLTBTransformed.columns) == False)).rdd.collect():
          dfGLTBTransformed = dfGLTBTransformed.withColumn(cols.columnName,trim(lit("")))
          
      elif(fType == 'GLA'):
        dfGLTBTransformed = fin_L1_TD_GLBalancePeriodic.select(col("companyCode"),col("accountNumber"),col("accountName"),\
                                                   col("accountGroup"),col("accountType")).distinct()
        
        for cols in gl_metadataDictionary["dic_ddic_column"].select(col("columnName"),col("sparkDataType")). \
                  filter((col("fileType") == 'GLA') &  (col("columnName").isin(dfGLTBTransformed.columns) == False)).rdd.collect():
          dfGLTBTransformed = dfGLTBTransformed.withColumn(cols.columnName,trim(lit(None)).cast(cols.sparkDataType))
          
        dfGLTBTransformed = objDataTransformation.gen_CDMColumnOrder_get(dfGLTBTransformed,'fin','L1_MD_GLAccount')
        fin_L1_MD_GLAccount = objDataTransformation.gen_CDMColumnOrder_get(fin_L1_MD_GLAccount,'fin','L1_MD_GLAccount')        
        dfGLTBTransformed = fin_L1_MD_GLAccount.union(dfGLTBTransformed)   
        
      return dfGLTBTransformed
    except Exception as err:
      raise

  def __gen_deriveColumn_perform(self,columnName,dfRawFile,row,fileType,isColExists = True):
    try:
      colDerived = uuid.uuid4().hex
      if(isColExists == True):        
        sourceColumn = row["sourceColumn"]        
        if(columnName == 'companyCode'):                    
          df = dfRawFile.filter((col(row["sourceColumn"]) == "") | (col(row["sourceColumn"]).isNull())).first()          
          if((df != None) or (sourceColumn != columnName)):            
            dfRawFile = dfRawFile.withColumn(colDerived,when(trim(col(row["sourceColumn"])) == "","N/A")
                                                           .when(col(row["sourceColumn"]).isNull(),"N/A")
                                                           .otherwise(col(row["sourceColumn"])))    
            dfRawFile = dfRawFile.drop(sourceColumn)
            dfRawFile = dfRawFile.withColumnRenamed(colDerived,'companyCode')            
        elif(columnName == 'documentStatus' and fileType == "JET"):
          df = dfRawFile.filter((col(row["sourceColumn"]) == "") | (col(row["sourceColumn"]).isNull())).first() 
          if((df != None) or (sourceColumn != columnName)):
            dfRawFile  = dfRawFile.withColumn(colDerived,when(trim(col(row["sourceColumn"])) == "","N")
                                                             .when(col(row["sourceColumn"]).isNull(),"N")
                                                             .when(col(row["sourceColumn"])=='null',"N")
                                                             .otherwise(col(row["sourceColumn"]))) 
            dfRawFile = dfRawFile.drop(sourceColumn)
            dfRawFile = dfRawFile.withColumnRenamed(colDerived,'documentStatus')
      else:
          defaultValue = ''
          if(row["columnName"] == 'companyCode'):
            defaultValue = 'N/A'
          elif (row["columnName"] == 'documentStatus'):
            defaultValue = 'N'
          dfRawFile = dfRawFile.withColumn(row["columnName"],lit(defaultValue))
      return dfRawFile
    except Exception as err:
      raise    
  
  def __gen_CDMLayer1_popuate(self,df,fileType):
    try:        
      global fin_L1_MD_GLAccount,fin_L1_TD_Journal,fin_L1_TD_GLBalance,fin_L1_TD_GLBalancePeriodic  
      if (fileType == 'JET'):
        fin_L1_TD_Journal = fin_L1_TD_Journal.union(df)
      elif (fileType == 'GLA'):        
        fin_L1_MD_GLAccount = fin_L1_MD_GLAccount.union(df)
      elif (fileType == 'GLAB'):
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.union(df)        
      elif (fileType == 'GLTB'):
          fin_L1_TD_GLBalancePeriodic = fin_L1_TD_GLBalancePeriodic.union(df)
    except Exception as err:
      raise
      
  def __gen_L1_TD_Journal_derive(self):
    try:
      global fin_L1_TD_Journal
      objDataTransformation = gen_dataTransformation()
      
      #check debit credit indicator
      df = fin_L1_TD_Journal.filter((fin_L1_TD_Journal.debitCreditIndicator.isNull()) |(trim(fin_L1_TD_Journal.debitCreditIndicator) == "")).first()
      if(df != None):
        colDerived = uuid.uuid4().hex
        fin_L1_TD_Journal = fin_L1_TD_Journal\
          .withColumn(colDerived,when((col("amountLC") < 0) & ((col("debitCreditIndicator").isNull()) | (trim(col("debitCreditIndicator")) == "")),"C")
          .when((col("amountLC") >= 0) & ((col("debitCreditIndicator").isNull()) | (trim(col("debitCreditIndicator")) == "")),"D")
          .otherwise(col("debitCreditIndicator")))
        fin_L1_TD_Journal = fin_L1_TD_Journal.drop("debitCreditIndicator")
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumnRenamed(colDerived,"debitCreditIndicator")
      
      #convert to uppercase if needed
      df = fin_L1_TD_Journal.filter((col("debitCreditIndicator") == 'c') | (col("debitCreditIndicator") == 'd')).first()
      if(df != None):
          colDerived = uuid.uuid4().hex
          fin_L1_TD_Journal = fin_L1_TD_Journal.withColumn(colDerived,when((col("debitCreditIndicator") == 'c'), "C")\
                              .when((col("debitCreditIndicator") == 'd'),"D")\
                              .otherwise(col("debitCreditIndicator")))
          fin_L1_TD_Journal = fin_L1_TD_Journal.drop("debitCreditIndicator")
          fin_L1_TD_Journal = fin_L1_TD_Journal.withColumnRenamed(colDerived,"debitCreditIndicator")

      #Creation date
      df = fin_L1_TD_Journal.filter((fin_L1_TD_Journal.creationDate.isNull()) |(trim(fin_L1_TD_Journal.creationDate) == "")).first()
      if(df != None):
        colDerived = uuid.uuid4().hex
        fin_L1_TD_Journal = fin_L1_TD_Journal\
           .withColumn(colDerived,when(((col("postingDate").isNull()) | (trim(col("postingDate")) ==""))\
                                       & ((col("creationDate").isNull()) | (trim(col("creationDate")) == "")),"1900-01-01")
           .when(((col("postingDate").isNotNull()) | (trim(col("postingDate")) != ""))\
                                      & ((col("creationDate").isNull()) | (trim(col("creationDate")) == "")),col("postingDate"))
          .otherwise(col("creationDate")))
        fin_L1_TD_Journal = fin_L1_TD_Journal.drop("creationDate")
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumnRenamed(colDerived,"creationDate")
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumn("creationDate",col("creationDate").cast("Date"))
      
      #amountLC, amountDC
      drCrChange = objDataTransformation.gen_usf_DebitCreditIndicator_Change('JET',fin_L1_TD_Journal)        
      if(drCrChange == "0"):
        colDerived = uuid.uuid4().hex
        fin_L1_TD_Journal = fin_L1_TD_Journal\
                              .withColumn(colDerived,(when((col("debitCreditIndicator") == 'C'), col("amountLC")*-1)
                              .otherwise(col("amountLC"))).cast(DecimalType(32,6)))
        fin_L1_TD_Journal = fin_L1_TD_Journal.drop("amountLC")
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumnRenamed(colDerived,"amountLC")
          
        colDerived = uuid.uuid4().hex
        fin_L1_TD_Journal = fin_L1_TD_Journal\
                                .withColumn(colDerived,(when((col("debitCreditIndicator") == 'C'), col("amountDC")*-1)
                                .otherwise(col("amountDC"))).cast(DecimalType(32,6)))
        fin_L1_TD_Journal = fin_L1_TD_Journal.drop("amountDC")
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumnRenamed(colDerived,"amountDC")
          
      #amountDC
      df = fin_L1_TD_Journal.filter((fin_L1_TD_Journal.amountDC.isNull()) & (fin_L1_TD_Journal.amountLC.isNotNull())).first()
      if(df != None):
        colDerived = uuid.uuid4().hex
        fin_L1_TD_Journal = fin_L1_TD_Journal\
                              .withColumn(colDerived,when((col("amountDC").isNull()) & (col("amountLC").isNotNull()),col("amountLC"))
                              .otherwise(col("amountDC")))
        fin_L1_TD_Journal = fin_L1_TD_Journal.drop("amountDC")
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumnRenamed(colDerived,"amountDC")                                                 
      
      #lineItem
      df_jetline = fin_L1_TD_Journal.filter((fin_L1_TD_Journal.lineItem.isNull()) | (trim(fin_L1_TD_Journal.lineItem) == "")).first()
      if(df_jetline != None):
        colDerived = uuid.uuid4().hex
        windowPartition = Window.partitionBy("companyCode","documentNumber","fiscalYear").orderBy(lit('A'))
        lineItemRow_number=row_number().over(windowPartition).cast(StringType())
        lineItemMax=when(max(col('lineItem').cast(IntegerType())).over(windowPartition).isNull(),0)\
                       .otherwise(max(col('lineItem').cast(IntegerType())).over(windowPartition)+1)
        lineItemNew=concat(lineItemRow_number,lineItemMax) 
        
        lineItem1 =when((col("lineItem").isNull()) | (trim(col("lineItem")) == ""),lineItemNew)\
                             .otherwise(col("lineItem"))
        fin_L1_TD_Journal = fin_L1_TD_Journal\
                                  .withColumn(colDerived,lineItem1)
                                
        fin_L1_TD_Journal = fin_L1_TD_Journal.drop("lineItem")
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumnRenamed(colDerived,"lineItem")
    except Exception as err:
      raise
      
  def __gen_L1_TD_GLBalance_derive(self):
    try:
      global fin_L1_TD_GLBalance 
      objDataTransformation = gen_dataTransformation()
    
      #debit credit indicator 
      df = fin_L1_TD_GLBalance\
            .filter((fin_L1_TD_GLBalance.debitCreditIndicator.isNull()) |(trim(fin_L1_TD_GLBalance.debitCreditIndicator) == "")).first()
      if(df != None):
        colDerived = uuid.uuid4().hex
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance\
            .withColumn(colDerived,when((col("endingBalanceLC") < 0) & \
                                        ((col("debitCreditIndicator").isNull()) | (trim(col("debitCreditIndicator")) == "")),"C")
            .when((col("endingBalanceLC") >= 0) & ((col("debitCreditIndicator").isNull()) | (trim(col("debitCreditIndicator")) == "")),"D")
            .otherwise(col("debitCreditIndicator")))
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.drop("debitCreditIndicator")
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.withColumnRenamed(colDerived,"debitCreditIndicator")
    
      #convert to uppercase if needed
      df = fin_L1_TD_GLBalance.filter((col("debitCreditIndicator") == 'c') | (col("debitCreditIndicator") == 'd')).first()
      if(df != None):
          colDerived = uuid.uuid4().hex
          fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.withColumn(colDerived,when((col("debitCreditIndicator") == 'c'), "C")\
                              .when((col("debitCreditIndicator") == 'd'),"D")\
                              .otherwise(col("debitCreditIndicator")))
          fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.drop("debitCreditIndicator")
          fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.withColumnRenamed(colDerived,"debitCreditIndicator")

      #endingBalanceLC, endingBalanceDC
      drCrChange = objDataTransformation.gen_usf_DebitCreditIndicator_Change('GLAB',fin_L1_TD_GLBalance)
      if(drCrChange == "1"):
        colDerived = uuid.uuid4().hex
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance\
                                .withColumn(colDerived,when((col("debitCreditIndicator") == 'C'), col("endingBalanceLC")*-1)
                                .otherwise(col("endingBalanceLC")))
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.drop("endingBalanceLC")
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.withColumnRenamed(colDerived,"endingBalanceLC")

        colDerived = uuid.uuid4().hex
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance\
                              .withColumn(colDerived,when((col("debitCreditIndicator") == 'C'), col("endingBalanceDC")*-1)
                              .otherwise(col("endingBalanceDC")))
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.drop("endingBalanceDC")
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.withColumnRenamed(colDerived,"endingBalanceDC")

      #endingBalanceDC
      df = fin_L1_TD_GLBalance.filter(((fin_L1_TD_GLBalance.endingBalanceDC.isNull())| \
              (trim(fin_L1_TD_GLBalance.endingBalanceDC) == "")) & (fin_L1_TD_GLBalance.endingBalanceLC.isNotNull())).first()   
      if(df != None):      
        colDerived = uuid.uuid4().hex
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.withColumn(colDerived
                                            ,when(((col("endingBalanceDC").isNull()) | (trim(col("endingBalanceDC")) == "")) \
                                                  & (col("endingBalanceLC").isNotNull()),col("endingBalanceLC"))
                                            .otherwise(col("endingBalanceDC")))
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.drop("endingBalanceDC")
        fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.withColumnRenamed(colDerived,"endingBalanceDC")   
    except Exception as err:
      raise
      
  def __dataFlowTransformation_perform(self):
      try:

          global fin_L1_MD_GLAccount,fin_L1_TD_Journal,fin_L1_TD_GLBalance,fin_L1_TD_GLBalancePeriodic
                                    
          for sourceFile in gl_lstOfSourceFiles:
            for fileType,dfRawFile in gl_lstOfSourceFiles[sourceFile].items():
                dfSource = dfRawFile 
                #compnayCode
                df = dfSource.filter((col("companyCode").isNull()) | (trim(col("companyCode")) == "")).first()
                if(df != None):
                    colDerived = uuid.uuid4().hex
                    dfSource = dfSource.\
                               withColumn(colDerived,when(((col("companyCode").isNull()) | (trim(col("companyCode")) == "")),"N/A").\
                               otherwise(col("companyCode")))
                    dfSource = dfSource.drop("companyCode")
                    dfSource = dfSource.withColumnRenamed(colDerived,"companyCode")

                dfFile =  gl_parameterDictionary['InputParams'].filter(upper(col("fileID"))==sourceFile)\
                        .select(col("fileName"),col("fileType")).first()
                fileName = dfFile[0]

                if(fileType == 'JET'):                  
                    #documentStatus
                    df = dfSource.filter((col("documentStatus").isNull()) | (trim(col("documentStatus")) == "")).first()                    
                    if(df != None):                        
                        colDerived = uuid.uuid4().hex
                        dfSource = dfSource\
                                  .withColumn(colDerived,when(((col("documentStatus").isNull()) | (trim(col("documentStatus")) == "")),"N").\
                                  otherwise(col("documentStatus")))
                        dfSource = dfSource.drop("documentStatus")
                        dfSource = dfSource.withColumnRenamed(colDerived,"documentStatus")

                    dtStatus = gen_CDMDataTypeValidation_perform(dfSource,
                                                         fileName,
                                                         sourceFile,
                                                         fileType,
                                                         gl_ERPSystemID,
                                                         schemaName = 'fin',
                                                         tableName = 'L1_TD_Journal',
                                                         isCDM=True)
                    fin_L1_TD_Journal = dfSource
                                    
                    for cols in gl_metadataDictionary['dic_ddic_column'].\
                                      filter((col('schemaName')=='fin') & (col('tableName') == 'L1_TD_Journal') & 
                                      (~col("columnName").isin(fin_L1_TD_Journal.columns))).\
                                      select(col("columnName")).collect():
                                fin_L1_TD_Journal = fin_L1_TD_Journal.withColumn(cols['columnName'],lit(""))
                                    
                elif(fileType == 'GLAB'):
                    dtStatus = gen_CDMDataTypeValidation_perform(dfSource,
                                                         fileName,
                                                         sourceFile,
                                                         fileType,
                                                         gl_ERPSystemID,
                                                         schemaName = 'fin',
                                                         tableName = 'L1_TD_GLBalance',
                                                         isCDM=True)
                    fin_L1_TD_GLBalance = dfSource
                elif(fileType == 'GLA'):
                    dtStatus = gen_CDMDataTypeValidation_perform(dfSource,
                                                         fileName,
                                                         sourceFile,
                                                         fileType,
                                                         gl_ERPSystemID,
                                                         schemaName = 'fin',
                                                         tableName = 'L1_MD_GLAccount',
                                                         isCDM=True)
                    fin_L1_MD_GLAccount = dfSource
                elif(fileType == 'GLTB'):
                    dtStatus = gen_CDMDataTypeValidation_perform(dfSource,
                                                         fileName,
                                                         sourceFile,
                                                         fileType,
                                                         gl_ERPSystemID,
                                                         schemaName = 'fin',
                                                         tableName = 'L1_TD_GLBalancePeriodic',
                                                         isCDM=True)
                    fin_L1_TD_GLBalancePeriodic = dfSource    
      except Exception as err:
          raise
   
