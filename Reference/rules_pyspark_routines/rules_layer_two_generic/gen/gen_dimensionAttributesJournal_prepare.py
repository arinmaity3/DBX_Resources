# Databricks notebook source
from pyspark.sql.functions import expr

class gen_dimensionAttributesJournal_prepare():
    @staticmethod
    def journalAttributes_collect():
        try:
            objGenHelper = gen_genericHelper()
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)

            #region Organization
            #logID = executionLog.init(gl_processID,None,None,None,None)            
            gl_lstOfOrganizationCollect.extend(fin_L1_TD_Journal.\
                select(col("companyCode"),lit('Journal')).\
                filter(col("companyCode") != '#NA#').distinct().\
                rdd.map(lambda row : [row[0],row[1]]).collect())
            #endregion  

            #region Postingkey
            gl_lstOfPostingKeyCollect.extend(fin_L1_TD_Journal.\
                  select(col("companyCode"),col("postingKey"),lit('Journal'))\
                 .filter( ~(col("postingKey").isin('#NA#','')) & (col("postingKey").isNotNull()) )\
                 .distinct()\
                 .rdd.map(lambda row : [row[0],row[1],row[2]]).collect())
            #endregion

            #region Product
            gl_lstOfProductCollect.extend(fin_L1_TD_Journal.\
                      select(col("companyCode"),col("productNumber"),\
                      col("productArtificialID"),lit('Journal')) \
                      .filter((col("productNumber") != "") & (col("productNumber") != "#NA#"))\
                      .rdd.map(lambda row : [row[0],row[1],row[2],row[3]]).collect())
            #endregion

            #region ReferenceTransaction
            gl_lstOfReferenceTransactionCollect.extend(fin_L1_TD_Journal.\
                    select(col("companyCode")\
                   ,col("referenceTransaction"),lit('Journal'))\
                   .filter( ~(col("referenceTransaction").isin('#NA#','')) & (col("referenceTransaction").isNotNull()))\
                   .distinct()\
                   .rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

            #endregion

            #region BusinessTransaction
            gl_lstOfBusinessTransactionCollect.extend(fin_L1_TD_Journal.\
                select(col("companyCode"),col("businessTransactionCode")\
                ,lit('Journal')) \
                .filter((col("businessTransactionCode") != "") & (col("businessTransactionCode") != "#NA#"))\
                .distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

            #endregion

            #region CalendarDate
            dateFields = gl_metadataDictionary['dic_ddic_column'].\
                        select(col("columnName")).\
                        filter((col("tableName") == 'L1_TD_Journal') & \
                       (col("dataType") == "date")).rdd.flatMap(lambda x: x).collect()
            
            for dateCols in dateFields:
                gl_lstOfCalendarDateCollect.extend(fin_L1_TD_Journal.select(col("companyCode")\
                    ,col(dateCols),lit('Journal'))\
                    .filter((col(dateCols).isNotNull()) & (trim(col(dateCols)) != ''))\
                    .distinct()\
                    .rdd.map(lambda row : [row[0],row[1],row[2]]).collect())
            #endregion


            #region Currency
            gl_lstOfCurrencyCollect.extend(fin_L1_TD_Journal.\
                       select(col("companyCode"),col("documentCurrency")\
                       ,lit('Journal'))\
                       .distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

            #endregion

            #region DocType
            gl_lstOfDocTypeCollect.extend(fin_L1_TD_Journal.\
                    select(col("companyCode")\
                    ,col("documentType"),lit('Journal'))\
                    .filter( ~(col("documentType").isin('#NA#','')) & (col("documentType").isNotNull()))\
                    .distinct()\
                    .rdd.map(lambda row : [row[0],row[1],row[2]]).collect())
            #endregion

            #region TransactionType
            gl_lstOfTransactionCollect.extend(fin_L1_TD_Journal.\
                    select(col("companyCode"),col("transactionCode"),\
                    lit('Journal'))\
                    .filter((col("transactionCode") != "") & \
                    (col("transactionCode") != "#NA#")).\
                    distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())
            #endregion

            #region User
            fields = ['createdBy','approvedBy']
            for f in fields:
                gl_lstOfUserCollect.extend(fin_L1_TD_Journal.\
                    select(col("companyCode")\
                    ,col(f),lit('Journal'))\
                    .filter( ~(col(f).isin('#NA#','')) &\
                    (col(f).isNotNull()))\
                    .distinct()\
                    .rdd.map(lambda row : [row[0],row[1],row[2]]).collect())
             
            #endregion

            #region Vendor
            gl_lstOfVendorCollect.extend(fin_L1_TD_Journal.\
                    select(col("companyCode"),col("vendorNumber"),\
                    lit('Journal'))\
                    .filter((col("vendorNumber") != "") & \
                    (col("vendorNumber") != "#NA#")).\
                    rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

            #endregion

            #region Customer
            gl_lstOfCustomerCollect.extend(fin_L1_TD_Journal.\
                     select(col("companyCode"),col("customerNumber"),\
                     lit('Journal'))\
                     .filter((col("customerNumber") != "") & \
                     (col("customerNumber") != "#NA#")).\
                     distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

            #endregion
    
            #region GLAccount
            gl_lstOfGLAccountCollect.extend(fin_L1_TD_Journal.\
                     select(col("companyCode"),col("accountNumber"),
                     lit('Journal'))\
                     .filter(~(col("accountNumber").isin('#NA#','')) &\
                     (col("accountNumber").isNotNull())). \
                     distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

            #endregion


            #region Financial Period
            gl_lstOfFinancialPeriodCollect.extend(fin_L1_TD_Journal.\
                   select(col('companyCode'),col('fiscalYear'),\
                   col('postingDate'),\
                   when (col('financialPeriod') < lit(1), lit(-1)).\
                   otherwise(col('financialPeriod')),\
                   lit('Jouranal'))\
                   .distinct() \
                   .rdd.map(lambda row : [row[0],row[1],row[2],row[3],row[4]]).collect())
            
            #endregion

            executionStatus = "Journal attributes populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log() 
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
        

    


