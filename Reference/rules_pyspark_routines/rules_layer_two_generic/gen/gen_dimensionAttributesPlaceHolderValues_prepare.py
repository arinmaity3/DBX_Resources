# Databricks notebook source
def __gen_defaultValue_prepare(orgUnit,schemaName,defaultValue = ""):
      try:       
        defaultCollection = globals()[schemaName].names.copy()     
        if(defaultValue == ""):
            defaultValue = "#NA#"
        if(schemaName == "financailPerodSchemaCollect"):
          defaultCollection[0] = ''.join(orgUnit)
          defaultCollection[1] = '1900'
          defaultCollection[2] = '1900-01-01'
          defaultCollection[3] = '1'
          defaultCollection[4] = 'orgUnit'
        else:               
          for i in range(len(defaultCollection)):
            if(defaultCollection[i] == "companyCode"):
              defaultCollection[i] = ''.join(orgUnit)
            elif (defaultCollection[i] == "key"):
              defaultCollection[i] = "orgUnit"
            else:
              defaultCollection[i] = defaultValue
                      
        return defaultCollection
      except Exception as err:
        raise
        
def gen_dimensionAttributesPlaceHolderValues_prepare():
    try:    
        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

        lstOfDefaultOrgUnit = gen_L2_DIM_Organization.\
                   select("companyCode")\
                   .filter( ~(col("companyCode").isin('#NA#','')) & (col("companyCode").isNotNull()) )\
                   .distinct()\
                   .rdd.map(lambda row : [row[0]]).collect()  
        funcs = [__gen_defaultValue_prepare]    
        for orgUnit in lstOfDefaultOrgUnit:
          for lstName,schemaName in  dimensionSchemCollection.items():
              if(schemaName in ["vendorSchemaCollect","customerSchemaCollect"]):
                  for i in ['#NA#','MANUAL']:
                      lstDefaultValue = list(map(lambda x: x(orgUnit,schemaName,i),funcs))
                      globals()[lstName].append(lstDefaultValue[0])      
              else:
                   lstDefaultValue = list(map(lambda x: x(orgUnit,schemaName,""),funcs))
                   globals()[lstName].append(lstDefaultValue[0])      
        
        executionStatus = "Palce holder values for attributes populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:        
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
