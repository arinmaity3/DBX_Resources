# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import concat,expr,col,when,lit


def _SAP_OrganizationSetup_validate():
  try:
        
    objDataTransformation = gen_dataTransformation()
    objGenHelper =  gen_genericHelper()
    executionStatus = ""
    executionStatusID = LOG_EXECUTION_STATUS.STARTED

    lstOfTables  = list()
    lstOfExistngTables = list()
    isPrepareReportingSetup = False
    
    global knw_LK_CD_ReportingSetup
    knw_LK_CD_ReportingSetup = None
    erp_T001 = None
    erp_BKPF = None
    
    #check whether reporting setup is already populated or not
    try:
      dbutils.fs.ls(gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv")      
    except Exception as e:      
      if 'java.io.FileNotFoundException' in str(e):
        isPrepareReportingSetup = True
        pass
        
    if(isPrepareReportingSetup == True):
      [lstOfTables.append([t.tableName.split('_')[1],t.netRecordCount]) \
      for t in gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT].\
                    filter(col("tableName").isin(['erp_T001','erp_BKPF'])).\
                    select(col("tableName"),col("netRecordCount")).collect()]

      lstOfExistngTables=[x[0] for x in lstOfTables]

      
      if((len(lstOfTables) == 2) and (len((list(itertools.filterfalse(lambda status : status[1] != 0, lstOfTables)))) != 0) == False):
        if(erp_BKPF == None):          
          erp_BKPF = objGenHelper.gen_readFromFile_perform(gl_layer0Staging + "erp_BKPF.delta")             
        if(erp_T001 == None):
          erp_T001 = objGenHelper.gen_readFromFile_perform(gl_layer0Staging + "erp_T001.delta")

        knw_LK_CD_ReportingSetup = erp_BKPF.alias("B").join(erp_T001.alias("T"),\
                            [(col("B.MANDT") == col("T.MANDT")) &  \
                            (col("B.BUKRS") == col("T.BUKRS"))],how='inner').\
                            filter((col("B.BUKRS")!= '') & (col("T.BUKRS") != '')).\
                            select(col("T.MANDT").alias("clientCode"),\
                                   col("T.BUTXT").alias("clientName"),\
                                   col("T.BUKRS").alias("companyCode"),\
                                   col("T.BUKRS").alias("reportingGroup"),\
                                   col("T.WAERS").alias("reportingCurrencyCode"),\
                                   col("T.WAERS").alias("localCurrencyCode"),\
                                   col("T.SPRAS"),\
                                   col("T.KTOPL").alias("chartOfaccount"),\
                                   lit('EN').alias("KPMGDataReportingLanguage")).distinct()

        knw_LK_CD_ReportingSetup = knw_LK_CD_ReportingSetup.alias("RS").join(\
                          gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].alias('RL'),
                                               [ (col("RL.businessDatatype") == lit('Language Code')) &
                                                (col("RL.sourceERPSystemID") == lit(10)) &
                                                (col("RL.targetERPSystemID") == lit(10)) &
                                               (col("RL.sourceSystemValue") == (col("RS.SPRAS")))],how='left').\
                         join(gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].alias('CRL'),
                                               [ (col("CRL.businessDatatype") == lit('Language Code')) &
                                                (col("CRL.sourceERPSystemID") == lit(10)) &
                                                (col("CRL.targetERPSystemID") == lit(10)) &
                                               (col("CRL.sourceSystemValue").eqNullSafe(col("RS.SPRAS")))],how='left').\
                         join(gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].alias('CSL'),
                                               [ (col("CSL.businessDatatype") == lit('Language Code')) &
                                                (col("CSL.sourceERPSystemID") == lit(10)) &
                                                (col("CSL.targetERPSystemID") == lit(10)) &
                                               (col("CSL.sourceSystemValue").eqNullSafe(col("RS.SPRAS")))],how='left').\
                        select(("RS.*"),\
                               col("RL.targetSystemValue").alias("reportingLanguageCode"),\
                               col("CRL.targetSystemValue").alias("clientDataReportingLanguage"),\
                               col("CSL.targetSystemValue").alias("clientDataSystemReportingLanguage"))

        knw_LK_CD_ReportingSetup  = objDataTransformation.\
                           gen_convertToCDMandCache(knw_LK_CD_ReportingSetup,'knw','LK_CD_ReportingSetup',False,gl_commonParameterPath)
        
        objGenHelper.gen_writeSingleCsvFile_perform(df = knw_LK_CD_ReportingSetup,\
                                                    targetFile = gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv")
         
      
      
      else:    
        lstOfMandatoryFiles = list()
        MandatoryFiles = ['T001','BKPF']
        MissingFiles = list(set(MandatoryFiles) - set(lstOfExistngTables))
        [lstOfMandatoryFiles.append(t) for t in MissingFiles]
        executionStatus = "The files (" + ",".join(lstOfMandatoryFiles) + ") are " \
                           "required to setup Organization units which are empty\\blank."
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
        return [executionStatusID,executionStatus]
    
    if(erp_T001 is None):
      erp_T001 = objGenHelper.gen_readFromFile_perform(gl_layer0Staging + "erp_T001.delta")
    if(knw_LK_CD_ReportingSetup is None):      
      knw_LK_CD_ReportingSetup = objGenHelper.gen_readFromFile_perform(gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv")
    
    #Check scoped data matched with source data
    lstOfCompanyCode = list()  
    [lstOfCompanyCode.append([c.clientCode,
                          c.companyCode,
                          c.chartOfAccount,
                          c.localCurrencyCode])
      for c in knw_LK_CD_ReportingSetup.alias('R').join(erp_T001.alias('T'),\
                                       [(upper(col("R.clientCode")) == upper(col("T.MANDT"))) & \
                                       (upper(col("R.companyCode")) == upper(col("T.BUKRS"))) & \
                                       (upper(col("R.chartOfAccount")) == upper(col("T.KTOPL"))) & \
                                       (upper(col("R.localCurrencyCode")) == upper(col("T.WAERS")))],how='left')\
                                       .filter((col("T.MANDT").isNull()) | (col("T.BUKRS").isNull()) |
                                              (col("T.KTOPL").isNull()) | (col("T.WAERS").isNull()))\
                                       .select(upper(col("R.clientCode")).alias('clientCode'),\
                                              upper(col("R.companyCode")).alias('companyCode'),
                                              upper(col("R.chartOfAccount")).alias('chartOfAccount'),
                                              upper(col("R.localCurrencyCode")).alias('localCurrencyCode')).distinct().collect()] 
              
    if(len(lstOfCompanyCode)!=0):
      res = "|".join([str(i or '')  for i in lstOfCompanyCode])
      executionStatus = "Scoped organization setup does not matched with source data"\
                       "[clientCode,companyCode,chartOfAccount,localCurrencyCode] " + res  
      executionStatusID = LOG_EXECUTION_STATUS.FAILED
      return [executionStatusID,executionStatus]
    
    #check duplicate client, company code populated or not
    lstOfKeys = list()
    [lstOfKeys.append([c.clientCode,c.companyCode])\
     for c in knw_LK_CD_ReportingSetup.select(upper(col("clientCode")).alias('clientCode'),upper(col("companyCode")).alias('companyCode')).collect()]
    
    lstOfClientCodes,lstOfCompanyCodes = map(list, zip(*lstOfKeys)) 
    
    if(len(set(lstOfClientCodes)) != 1):
      executionStatus = "You have scoped organizational units across multiple client codes; this is not supported." \
                         "Please ensure that only organizational units within one client code are present within a" \
                         "single analysis [clientCode] - [" + ",".join(lstOfClientCodes) + "]"
      executionStatusID = LOG_EXECUTION_STATUS.FAILED  
      return [executionStatusID,executionStatus]
    else:
#       duplicateCompanyCode = [x for i, x in enumerate(lstOfCompanyCodes) if x in lstOfCompanyCodes[:i]]
      if(len(lstOfCompanyCodes) != len(set(lstOfCompanyCodes))) :
        executionStatus = "You have scoped a duplicate organizational unit within one client;" \
                         " this is not supported. Please ensure that organizational units are only" \
                         "scoped once within a single analysis [companyCode] - [" + ",".join(lstOfCompanyCodes) + "]"
         
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
        return [executionStatusID,executionStatus]
    
    if(isPrepareReportingSetup == False):      
      knw_LK_CD_ReportingSetup =  knw_LK_CD_ReportingSetup.alias("R")\
                          .join(erp_T001.alias("T"),\
                          [(upper(col("R.clientCode")) == upper(col("T.MANDT"))) & \
                          (upper(col("R.companyCode")) == upper(col("T.BUKRS")))],how='inner')\
                          .join(gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].alias('RL'),
                                               [ (col("RL.businessDatatype") == lit('Language Code')) &
                                                (col("RL.sourceERPSystemID") == lit(10)) &
                                                (col("RL.targetERPSystemID") == lit(10)) &
                                               (col("RL.sourceSystemValue") == (col("T.SPRAS"))) &
                                               (upper(col("RL.targetSystemValue")) == upper(col('R.reportingLanguageCode')))],how='left')\
                         .join(gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].alias('CRL'),
                                               [ (col("CRL.businessDatatype") == lit('Language Code')) &
                                                (col("CRL.sourceERPSystemID") == lit(10)) &
                                                (col("CRL.targetERPSystemID") == lit(10)) &
                                               (col("CRL.sourceSystemValue").eqNullSafe(col("T.SPRAS"))) &
                                               (upper(col("CRL.targetSystemValue")) == upper(col('R.clientDataReportingLanguage')))],how='left')\
                         .join(gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].alias('CSL'),
                                               [ (col("CSL.businessDatatype") == lit('Language Code')) &
                                                (col("CSL.sourceERPSystemID") == lit(10)) &
                                                (col("CSL.targetERPSystemID") == lit(10)) &
                                               (col("CSL.sourceSystemValue").eqNullSafe(col("T.SPRAS"))) &
                                               (upper(col("CSL.targetSystemValue")) == upper(col('R.clientDataSystemReportingLanguage')))],how='left')\
                          .select(col("T.MANDT").alias('clientCode')
                                 ,upper(col('R.clientName')).alias('clientName')
                                 ,col("T.BUKRS").alias('companyCode')
                                 ,upper(col('R.reportingGroup')).alias('reportingGroup')
                                 ,col("T.WAERS").alias('reportingCurrencyCode')
                                 ,col("T.WAERS").alias('localCurrencyCode')
                                 ,col('RL.targetSystemValue').alias('reportingLanguageCode')
                                 ,col("T.KTOPL").alias('chartOfAccount')
                                 ,col('CRL.targetSystemValue').alias('clientDataReportingLanguage')
                                 ,col('CSL.targetSystemValue').alias('clientDataSystemReportingLanguage')
                                 ,upper(col('R.KPMGDataReportingLanguage')).alias('KPMGDataReportingLanguage')
                                 ,upper(col('R.clientTerritoryLanguage')).alias('clientTerritoryLanguage')
                                 ,upper(col('R.secondCurrencyCode')).alias('secondCurrencyCode')
                                 ,upper(col('R.thirdCurrencyCode')).alias('thirdCurrencyCode')
                                 ,upper(col('R.ledgerID')).alias('ledgerID')
                                 ,upper(col('R.balancingSegment')).alias('balancingSegment')
                                 ,(when(((col("R.clientName").isNull()) | (col("R.clientName") == '')),col("T.BUTXT")).
                                 otherwise(col("R.clientName"))).alias("clientNameNew"))\
                          .drop("clientName").withColumnRenamed("clientNameNew","clientName")
            
      knw_LK_CD_ReportingSetup  = objDataTransformation.\
                           gen_convertToCDMandCache(knw_LK_CD_ReportingSetup,'knw','LK_CD_ReportingSetup',False,gl_commonParameterPath)
      
      objGenHelper.gen_writeSingleCsvFile_perform(df = knw_LK_CD_ReportingSetup,\
                                                  targetFile = gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv")        
    
    __knw_LK_CD_ReportingLanguage_get()

    executionStatus = "Organization setup validation completed successfully."
    executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
    return [executionStatusID,executionStatus]  
  
  except Exception as err: 
    raise


def __knw_LK_CD_ReportingLanguage_get():
        """Creates a table with distinct reportinglanguage details for each company and client.
        This table can be looked up based on the languagetype to get distinct values of that language type.
        """
        try:
            df = spark.sql("SELECT DISTINCT \
            										 clientCode			\
            										,companyCode		\
            										,clientDataReportingLanguage	AS languageCode		\
            										,'clientDataReportingLanguage'	AS languageCodeColumn	\
            					         FROM knw_LK_CD_ReportingSetup	\
            						     UNION ALL						\
            						     SELECT DISTINCT				\
            						     				 clientCode		\
            						     				,companyCode	\
            						     				,clientDataSystemReportingLanguage	AS languageCode	\
            						     				,'clientDataSystemReportingLanguage' AS languageCodeColumn	\
            						     		FROM knw_LK_CD_ReportingSetup \
            						     UNION ALL							\
            						     SELECT DISTINCT					\
            										 clientCode			\
            										,companyCode		\
            										,KPMGDataReportingLanguage		AS languageCode		\
            										,'KPMGDataReportingLanguage'     AS languageCodeColumn	\
            							 FROM knw_LK_CD_ReportingSetup	")

            df.createOrReplaceTempView("knw_LK_CD_ReportingLanguage")            

        except Exception as e:
            raise  

#ORA OrganizationSetUp validate

def _ORA_OrganizationSetup_validate():
  try:
    objDataTransformation = gen_dataTransformation()
    objGenHelper =  gen_genericHelper()

    global knw_LK_CD_ReportingSetup
    global erp_GL_LEDGER_NORM_SEG_VALS,erp_GL_LEDGERS
    
    isPrepareReportingSetup=False
    executionStatus = ""
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    #check whether reporting setup is already populated or not
    try:
      dbutils.fs.ls(gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv")      
    except Exception as e:      
      if 'java.io.FileNotFoundException' in str(e):
        isPrepareReportingSetup = True
        pass
    if(erp_GL_LEDGER_NORM_SEG_VALS is None):          
          erp_GL_LEDGER_NORM_SEG_VALS = objGenHelper.gen_readFromFile_perform(gl_layer0Staging + "erp_GL_LEDGER_NORM_SEG_VALS.delta")             
    if(erp_GL_LEDGERS is None):
          erp_GL_LEDGERS = objGenHelper.gen_readFromFile_perform(gl_layer0Staging + "erp_GL_LEDGERS.delta")

    if isPrepareReportingSetup:
        knw_LK_CD_ReportingSetup=\
          erp_GL_LEDGER_NORM_SEG_VALS.alias("a")\
          .join(erp_GL_LEDGERS.alias("b"),\
          expr("a.ledger_id= b.ledger_id"),"inner")\
          .filter(expr("a.segment_type_code ='B'"))\
           .selectExpr("concat(LTRIM(RTRIM(CAST(a.ledger_id AS varchar(20)))) \
                      , '-' , RTRIM(LTRIM(a.segment_value))) as companyCode"\
                      ,"concat(LTRIM(RTRIM(CAST(a.ledger_id AS varchar(20)))) \
                      , '-' , RTRIM(LTRIM(a.segment_value))) as reportingGroup"\
                      ,"''   as clientCode"\
                      ,"''   as clientName"\
                      ,"b.currency_code   as reportingCurrencyCode"\
                      ,"b.currency_code   as localCurrencyCode"\
                      ,"'EN'     as reportingLanguageCode"\
                      ,"b.chart_of_accounts_id as chartOfAccount"\
                      ,"'EN' as clientDataReportingLanguage"\
                      ,"'EN' as clientDataSystemReportingLanguage"\
                      ,"'EN' as KPMGDataReportingLanguage"\
                      ,"LTRIM(RTRIM(a.ledger_id)) as ledgerID"\
                      ,"LTRIM(RTRIM(a.segment_value)) as balancingSegment").distinct()
    else:
      knw_LK_CD_ReportingSetup = objGenHelper.gen_readFromFile_perform(gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv")

    clientNameOrCodeEmpty=(col("R.clientName").isNull())|(col("R.clientName")=='')\
					  |(col("R.clientCode").isNull())|(col("R.clientCode")=='')

    legalEntityIdentifierEmpty = when(((col("X.LEGAL_ENTITY_IDENTIFIER").isNull())|(col("X.LEGAL_ENTITY_IDENTIFIER")=='')),lit('#NA#'))\
                 .otherwise(col("X.LEGAL_ENTITY_IDENTIFIER"))
    nameEmpty = when(((col("X.NAME").isNull())|(col("X.NAME")=='')),lit('#NA#'))\
                 .otherwise(col("X.NAME"))

    knw_LK_CD_ReportingSetup = knw_LK_CD_ReportingSetup.alias("R")\
        .join(erp_GL_LEDGER_NORM_SEG_VALS.alias("G"),\
        (upper(col("G.SEGMENT_VALUE")).eqNullSafe(upper(col("R.balancingSegment"))))\
        &(upper(col("G.LEDGER_ID")).eqNullSafe(upper(col("R.ledgerID")))),"left")\
        .join(erp_XLE_ENTITY_PROFILES.alias("X"),\
        upper(col("G.LEGAL_ENTITY_ID")).eqNullSafe(upper(col("X.LEGAL_ENTITY_ID"))),"left")\
        .withColumn("clientCode_upd",when(lit(clientNameOrCodeEmpty),(when(col("X.LEGAL_ENTITY_ID").isNotNull()\
                                     ,lit(legalEntityIdentifierEmpty)).otherwise(lit('#NA#'))))\
                                    .otherwise(col("R.clientCode")))\
        .withColumn("clientName_upd",when(lit(clientNameOrCodeEmpty),(when(col("X.LEGAL_ENTITY_ID").isNotNull()\
                                     ,lit(nameEmpty)).otherwise(lit('#NA#'))))\
                                    .otherwise(col("R.clientName")))\
    	.drop(col("R.clientCode"))\
    	.drop(col("R.clientName"))\
    	.withColumnRenamed("clientCode_upd","clientCode")\
    	.withColumnRenamed("clientName_upd","clientName")

    if(isPrepareReportingSetup == False):
       knw_LK_CD_ReportingSetup = knw_LK_CD_ReportingSetup.alias("R")\
           .join(erp_GL_LEDGER_NORM_SEG_VALS.alias("G"),\
           (upper(col("G.SEGMENT_VALUE")).eqNullSafe(upper(col("R.balancingSegment"))))\
           &(upper(col("G.LEDGER_ID")).eqNullSafe(upper(col("R.ledgerID")))),"left")\
           .join(erp_GL_LEDGERS.alias("L"),\
           (upper(col("L.LEDGER_ID")).eqNullSafe(upper(col("R.ledgerID"))))\
           &(upper(col("L.CHART_OF_ACCOUNTS_ID")).eqNullSafe(upper(col("R.chartOfAccount")))),"left")\
           .select(col("R.clientCode").alias('clientCode')
                  ,col('R.clientName').alias('clientName')
                  ,when(col('G.LEDGER_ID').isNotNull(),when(upper(concat(col('G.LEDGER_ID'),lit('-'),\
                        col('G.SEGMENT_VALUE'))) == upper(col('R.companyCode'))\
                        ,concat(col('G.LEDGER_ID'),lit('-'),col('G.SEGMENT_VALUE')))\
                        .otherwise(upper(col('R.companyCode'))))\
                        .otherwise(upper(col('R.companyCode'))).alias('companyCode')\
                  ,when(col('G.LEDGER_ID').isNotNull(),when(upper(concat(col('G.LEDGER_ID'),lit('-')\
                       ,col('G.SEGMENT_VALUE'))) == upper(col('R.reportingGroup'))\
                       ,concat(col('G.LEDGER_ID'),lit('-'),col('G.SEGMENT_VALUE')))\
                       .otherwise(upper(col('R.reportingGroup'))))\
                       .otherwise(upper(col('R.reportingGroup'))).alias('reportingGroup')\
                  ,when(col('L.LEDGER_ID').isNotNull(),col('L.CURRENCY_CODE'))\
                       .otherwise(col('R.reportingCurrencyCode')).alias('reportingCurrencyCode')\
                  ,when(col('L.LEDGER_ID').isNotNull(),col('L.CURRENCY_CODE'))\
                       .otherwise(col('R.localCurrencyCode')).alias('localCurrencyCode')\
                  ,upper(col('R.reportingLanguageCode')).alias('reportingLanguageCode')\
                  ,when(col('L.LEDGER_ID').isNotNull(),col("L.CHART_OF_ACCOUNTS_ID"))\
                       .otherwise(col('R.chartOfAccount')).alias('chartOfAccount')\
                  ,upper(col('R.clientDataReportingLanguage')).alias('clientDataReportingLanguage')
                  ,upper(col('R.clientDataSystemReportingLanguage')).alias('clientDataSystemReportingLanguage')
                  ,upper(col('R.KPMGDataReportingLanguage')).alias('KPMGDataReportingLanguage')
                  ,upper(col('R.clientTerritoryLanguage')).alias('clientTerritoryLanguage')
                  ,upper(col('R.secondCurrencyCode')).alias('secondCurrencyCode')
                  ,upper(col('R.thirdCurrencyCode')).alias('thirdCurrencyCode')
                  ,when(col('G.LEDGER_ID').isNotNull(),col('G.LEDGER_ID'))\
                      .otherwise(col('R.ledgerID')).alias('ledgerID')\
                  ,when(col('G.LEDGER_ID').isNotNull(),col('G.SEGMENT_VALUE'))\
                      .otherwise(col('R.balancingSegment')).alias('balancingSegment'))

    knw_LK_CD_ReportingSetup  = objDataTransformation.gen_convertToCDMandCache\
        (knw_LK_CD_ReportingSetup,'knw','LK_CD_ReportingSetup',False,gl_commonParameterPath)

    objGenHelper.gen_writeSingleCsvFile_perform(df = knw_LK_CD_ReportingSetup,\
                           targetFile = gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv")
    
    recd_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.companyCode').isNull(),lit(''))\
                        .otherwise(col('rps.companyCode')).cast("string")!="").count()

    if (recd_count == 0):
      executionStatus = "Company Code column is empty in '[knw].[LK_CD_ReportingSetup].[companyCode]'"  
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    df_duplicate_companyCode = knw_LK_CD_ReportingSetup.select(col('companyCode'))\
                           .groupBy("companyCode").count()\
                           .filter(col('count')>1)
    missingparameter = df_duplicate_companyCode.select(col('companyCode')).rdd.flatMap(lambda x: x).collect()
    
    if (len(missingparameter)>0):
      Joinstring = ','.join(missingparameter)
      executionStatus = "Duplicate company code found in Organization Setup ('[knw].[LK_CD_ReportingSetup].[companyCode]')" + str(Joinstring)
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
    
    executionStatus="OrganizationSetup Validation successful"    
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()           
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


def app_organizationSetup_validate(executionID = ""):
  try:
    if(gl_ERPSystemID == 12):
        VALIDATION_ID_ORG = VALIDATION_ID.ORA_ORGANIZATION_SETUP
    else:
        VALIDATION_ID_ORG = VALIDATION_ID.ORGANIZATION_SETUP


    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID_ORG,
                              executionID = executionID)
    
    if(gl_ERPSystemID == 1):      
      executionStatus =  _SAP_OrganizationSetup_validate()
      executionLog.add(executionStatus[0],logID,executionStatus[1])
      return [executionStatus[0],executionStatus[1]]
    elif(gl_ERPSystemID == 12):
        executionStatus =  _ORA_OrganizationSetup_validate()
        executionLog.add(executionStatus[0],logID,executionStatus[1])
        return [executionStatus[0],executionStatus[1]]
              
  except Exception as err: 
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]   
    
