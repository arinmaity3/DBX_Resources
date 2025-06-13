# Databricks notebook source
from pyspark.sql.functions import expr, format_number 
from pyspark.sql.functions import row_number,lit ,col, when, concat, regexp_extract,trim
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType,LongType,DecimalType,FloatType
import sys
import traceback

def check_numberformat(dfinput,columnname,decimalseparator_str,thousandseparator_str):
    try:
        metacharacters_list = ['.','*', '+','[',']','(',')','^','$','{','}','|','?']
        # Escape the separator string if it happens to be a regex metacharacter
        if decimalseparator_str in metacharacters_list:
            decimalseparator = '\\'+decimalseparator_str
        else:
            decimalseparator = decimalseparator_str
        if thousandseparator_str in metacharacters_list:
            thousandseparator = '\\'+thousandseparator_str
        else:
            thousandseparator = thousandseparator_str
        error_patterns_list = [ decimalseparator+'.*'+thousandseparator, # Eg: 34.,5 or 34.66,05
                                 thousandseparator+thousandseparator, # Eg: 34,,5
                                 thousandseparator+decimalseparator,  # Eg: 34,.5
                                 decimalseparator+'.*'+decimalseparator, # Eg: 34..5
                                 '\+'+thousandseparator, # Eg: 34+,5  +,5
                                 '-'+thousandseparator,  # Eg: -,
                                 '\+'+decimalseparator+'$',  #  +.
                                 '-'+decimalseparator+'$',  #  -.
                                 thousandseparator+'$',  # Ends with thousandseparator. Eg: 34.5,
                                 '.+-',     # -sign anywhere other than at the beginning. Eg: 34-5 
                                 '.+\+',    # +sign anywhere other than at the beginning. Eg: 34+5 
                                 '^-$',     # -
                                 '^\+$',    # +
                                 '^'+decimalseparator+'$',  # .
                                 '^'+thousandseparator+'$'  # ,
                               ]
                                                          # If the value matches with any of the above list of invalid patterns, return True
        dferror = dfinput.withColumn("invalidpattern",((concat(*[regexp_extract(columnname, val, 0) for val in error_patterns_list]) != "") | \
                                                         # or if the stripped value contains any character other than 0-9, return True     
                                                         (regexp_extract(trim(col(columnname+"_trimmed")),'[^0-9]',0) != "" )))  \
                           .filter(col("invalidpattern") == True) 
        return dferror
    except Exception as err:
        raise

def app_numberFormat_validate(fileName,fileID,fileType,executionID,decimalSeperator,thousandSeperator, dfSource):
    """Accept a source-dataframe and its file details as input,
    Lookup against mapping information in parameter dictionary to get the target columnnames of the passed fileid,
    Lookup against the ddic information in metadata dictionary to get the numeric columns for the passed fileType,
    For the list of mapped numeric columns, check if the values in the source dataframe are valid numerics,
    In case of error, prepare an unpivoted detail output and append that to a global error list.
    """
    try:
        lstOfValidationResult=None
        dferrordetail = None
        cleanedColumnList=[]
        logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.NUMBER_FORMAT,executionID = executionID) 
        objGenHelper = gen_genericHelper()  
        validationId = VALIDATION_ID.NUMBER_FORMAT.value
            
        vw_app_numberFormat_validate_source = 'vw_app_numberFormat_validate_source_' + fileID.replace('-','_').lower()
        vw_app_numberFormat_validate_colMapping = 'vw_app_numberFormat_validate_colMapping_' + fileID.replace('-','_').lower()
        vw_app_numberFormat_validate_dic_column = 'vw_app_numberFormat_validate_dic_column_' + fileID.replace('-','_').lower()
        vw_app_numberFormat_validate_error = 'vw_app_numberFormat_validate_error_' + fileID.replace('-','_').lower()
        app_numberFormat_validate_tmpresult = 'app_numberFormat_validate_tmpresult_' + fileID.replace('-','_').lower()

        # Set decimalSeperator and thousandSeperator to default values in case they are NULL:
        if decimalSeperator is None:
            decimalSeperator = "."

        if thousandSeperator is None:
            thousandSeperator = ","

        # Create the SQL temp views:

        # 1) Capture the raw source data into a table after appending a rownumber column:
        dfSource.withColumn("rowNumber", row_number().over(Window().orderBy(lit('id')))).createOrReplaceTempView(vw_app_numberFormat_validate_source)
        
        # 2) Capture the column mapping information for the given file into a table:
        gl_parameterDictionary["SourceTargetColumnMapping"].filter((col("fileID") == fileID)).createOrReplaceTempView(vw_app_numberFormat_validate_colMapping)
            
        # 3) Capture the ddic column information of all numeric columns for the given file-type into a table:
        gl_metadataDictionary["dic_ddic_column"].select(col("columnName")).filter((col("fileType") == fileType) & (col("dataType") == "numeric")).createOrReplaceTempView(vw_app_numberFormat_validate_dic_column)

        # Create an empty dataframe to which the validation results will be later appended:
        dferror = spark.sql("select "+ str(validationId) +" as validationid, '' as filetype, rowNumber,'' as Column,'' as value, '0' as validationstatus \
                                 from "+ vw_app_numberFormat_validate_source +" where 1 = 2")

        # Get the list of columns that need to be validated:
        column_names_list = spark.sql("SELECT B.sourceColumn \
                                          FROM "+ vw_app_numberFormat_validate_dic_column +" A \
                                          INNER JOIN "+vw_app_numberFormat_validate_colMapping +" B \
                                          ON A.columnName = B.targetColumn").collect()

        cleanedColumnList = [row_of_columnname[0].replace('[', '').replace(']', '') for row_of_columnname in column_names_list]
            
        # Perform numberformatvalidation and collect the error rows into a dataframe dferror:
        dfStdNumberFormat = dfSource
        for columnname in cleanedColumnList:
            if dict(dfStdNumberFormat.dtypes)[columnname] != "double":
                dfinput = spark.sql("select rowNumber, \
                                               ltrim(rtrim(`"+columnname+"`)) as `"+columnname+"`,  \
                                               replace(replace(replace(replace(`"+columnname+"`,\
                                                                              '"+decimalSeperator+"',''),\
                                                             '"+thousandSeperator+"',''),\
                                                       '+',''),\
                                               '-','') as `"+columnname+"_trimmed` \
                                               from "+ vw_app_numberFormat_validate_source +" \
                                               where nullif(ltrim(rtrim(`"+columnname+"`)),'') is not null")

                check_numberformat(dfinput,columnname,decimalSeperator,thousandSeperator).createOrReplaceTempView(vw_app_numberFormat_validate_error)
                dfeachcolumnresult = spark.sql("select "+ str(validationId) +" as validationid, '" \
                                                   + fileType +"' as filetype, rowNumber,'" \
                                                   + columnname + "' as Column, `" \
                                                   + columnname + "` as value, \
                                                   '0' as validationStatus \
                                                   from "+ vw_app_numberFormat_validate_error + "")
                dferror = dferror.union(dfeachcolumnresult)
            else:
                dfStdNumberFormat=dfStdNumberFormat\
                  .withColumn(columnname,format_number(col(columnname),6))

                
        if dferror.count() == 0:
            executionStatus = "Number format validation succeeded."
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
               
        else:
            executionStatus = "There are columns in the raw file that contain invalid numeric values."
            executionStatusID = LOG_EXECUTION_STATUS.FAILED 
            # Generate a gpslno, then unpivot the validation error details and collect into a dataframe :    
            dferror.limit(gl_maximumNumberOfValidationDetails).createOrReplaceTempView(app_numberFormat_validate_tmpresult)
            dferrordetail = spark.sql("select DENSE_RANK() OVER (ORDER BY rowNumber,Column) as groupSlno, \
                                                 validationid AS validationID,\
                                                 filetype AS validationObject,filetype AS fileType,\
                                                 cast(rowNumber as string) as rowNumber,Column,value, \
                                                 case when validationstatus = 0 then 'Invalid numeric pattern present.'  \
                                                 end as remarks \
                                                 from "+ app_numberFormat_validate_tmpresult +"")
            dferrordetail = dferrordetail.select("groupSlno","validationID","validationObject","fileType",expr("stack(3,'rowNumber',rowNumber,'Column',Column,'value',value) as (resultKey,resultValue)"),"remarks")

    except Exception as e:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED         
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail  = dferrordetail)
        return [executionStatusID,executionStatus,dferrordetail,cleanedColumnList,VALIDATION_ID.NUMBER_FORMAT.value]
