# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,lit,col,when
import sys
import traceback

def fin_L2_FACT_Journal_01_prepare(): 
  """Populate fin_L2_FACT_Journal_01_prepare """
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION) 
    global fin_L2_STG_Journal_01_prepare
  
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    fin_L2_STG_Journal_01_prepare =spark.sql("SELECT "\
                "    dou2.organizationUnitSurrogateKey					  organizationUnitSurrogateKey"\
				"   ,jou1.journalSurrogateKey							  journalSurrogateKey"\
				"   ,gla2.glAccountSurrogateKey							  glAccountSurrogateKey"\
				"   ,gla2.accountNumber									  accountNumber"\
				"   ,ven2.vendorSurrogateKey							  vendorSurrogateKey"\
				"   ,ven2.vendorNumber									  vendorNumber"\
				"   ,cus2.customerSurrogateKey							  customerSurrogateKey"\
				"   ,cus2.customerNumber								  customerNumber"\
				"   ,doc2.documentTypeSurrogateKey						  documentTypeSurrogateKey"\
				"   ,doc2.documentType									  documentType"\
				"   ,trn2.transactionTypeSurrogateKey					  transactionCodeSurrogateKey"\
				"   ,trn2.transactionType								  transactionType"\
				"   ,case when ISNULL(jePstg.postingSurrogateKey)"\
                    " then 0 else	jePstg.postingSurrogateKey	end 	  postingSurrogateKey"\
				"   ,case when ISNULL(jePstg.postingKey)	"
                     " then '#NA#'	else jePstg.postingKey end 			  postingKey"\
				"   ,bt2.businessTransactionSurrogateKey				  businessTransactionSurrogateKey"\
				"   ,bt2.businessTransactionCode						  businessTransactionCode"\
				"   ,jeRfr.referenceTransactionSurrogateKey				  referenceTransactionSurrogateKey"\
				"   ,jeRfr.referenceTransactionCode						  referenceTransactionCode"\
				"   ,jou1.customAmount									  customAmount"\
				"   ,jou1.customDate									  customDate"\
				"   ,jou1.customText									  customText"\
                "   ,jou1.customText02									  customText02"\
                "   ,jou1.customText03									  customText03"\
                "   ,jou1.customText04									  customText04"\
                "   ,jou1.customText05									  customText05"\
                "   ,jou1.customText06									  customText06"\
                "   ,jou1.customText07									  customText07"\
                "   ,jou1.customText08									  customText08"\
                "   ,jou1.customText09									  customText09"\
                "   ,jou1.customText10									  customText10"\
                "   ,jou1.customText11									  customText11"\
                "   ,jou1.customText12									  customText12"\
                "   ,jou1.customText13									  customText13"\
                "   ,jou1.customText14									  customText14"\
                "   ,jou1.customText15									  customText15"\
                "   ,jou1.customText16									  customText16"\
                "   ,jou1.customText17									  customText17"\
                "   ,jou1.customText18									  customText18"\
                "   ,jou1.customText19									  customText19"\
                "   ,jou1.customText20									  customText20"\
                "   ,jou1.customText21									  customText21"\
                "   ,jou1.customText22									  customText22"\
                "   ,jou1.customText23									  customText23"\
                "   ,jou1.customText24									  customText24"\
                "   ,jou1.customText25									  customText25"\
                "   ,jou1.customText26									  customText26"\
                "   ,jou1.customText27									  customText27"\
                "   ,jou1.customText28									  customText28"\
                "   ,jou1.customText29									  customText29"\
                "   ,jou1.customText30									  customText30"\
                " FROM	fin_L1_TD_Journal				jou1"\
                " INNER JOIN gen_L2_DIM_Organization	dou2 ON "\
                    " dou2.companyCode					= jou1.companyCode"\
                    " AND dou2.companyCode				<> '#NA#'"\
                " INNER JOIN fin_L2_DIM_GLAccount	    gla2 ON "\
                  " gla2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey	"\
                  " AND gla2.accountNumber			= case when isnull(nullif(jou1.accountNumber,'')) then "\
                  "'#NA#' else jou1.accountNumber end "\
                  " AND gla2.accountCategory			= '#NA#'"\

                " INNER JOIN ptp_L2_DIM_Vendor        ven2 ON "\
                  " ven2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey"\
                  " AND ven2.vendorNumber				= case when isnull(nullif(jou1.vendorNumber,'')) \
                                                          then '#NA#' else jou1.vendorNumber end "\

                 " INNER JOIN otc_L2_DIM_Customer      cus2 ON "\
                  " cus2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey"\
                  " AND cus2.customerNumber			= case when isnull(nullif(jou1.customerNumber,'')) \
                                                      then '#NA#' else jou1.customerNumber end "\

                " INNER JOIN gen_L2_DIM_DocType       doc2 ON  "\
                  " doc2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey"\
                  " AND doc2.documentType				= case when isnull(nullif(jou1.documentType,''))\
                                                          then '#NA#' else jou1.documentType end"\

                " INNER JOIN gen_L2_DIM_Transaction	trn2 ON	 "\
                  " trn2.organizationUnitSurrogateKey	= dou2.organizationUnitSurrogateKey"\
                  " AND trn2.transactionType			= case when isnull(nullif(jou1.transactionCode,'')) then "\
                  " '#NA#' else jou1.transactionCode end "\

                " LEFT  JOIN  gen_L2_DIM_BusinessTransaction  bt2  ON  "\
                  " bt2.organizationUnitSurrogateKey   = dou2.organizationUnitSurrogateKey"\
                  " AND bt2.businessTransactionCode	= case when isnull(nullif(jou1.businessTransactionCode,'')) then" 
                  " '#NA#' else jou1.businessTransactionCode end "\

                " LEFT  JOIN gen_L2_DIM_PostingKey		jePstg  ON "\
                  " jePstg.organizationUnitSurrogateKey    = dou2.organizationUnitSurrogateKey"\
                  " AND jePstg.postingKey				     = case when isnull(nullif(cast(jou1.postingKey AS VARCHAR(5)),''))"\
                  " then'#NA#' else cast(jou1.postingKey AS VARCHAR(5)) end "\
                  " AND jePstg.languageCode				   = dou2.reportingLanguage"\

                " LEFT  JOIN gen_L2_DIM_ReferenceTransaction jeRfr ON "\
                  " dou2.organizationUnitSurrogateKey		 = jeRfr.organizationUnitSurrogateKey"\
                  " AND case when isnull(nullif(jou1.referenceTransaction,'')) then '#NA#' else jou1.referenceTransaction end  "\
                  " = jeRfr.referenceTransactionCode")
                               
    
    fin_L2_STG_Journal_01_prepare = objDataTransformation.gen_convertToCDMandCache \
        (fin_L2_STG_Journal_01_prepare,'fin','L2_STG_Journal_01_prepare',True)
        
    journal_01_prepareCount = fin_L2_STG_Journal_01_prepare.count()    
    
    if journal_01_prepareCount != gl_countJET:
        executionStatus = "Number of records in fin_L2_FACT_Journal_01_prepare are \
        not reconciled with number of records in [fin].[L1_TD_Journal]"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        warnings.warn(executionStatus)
        return [False,"fin_L2_FACT_Journal_01_prepare population failed"] 
    
    executionStatus = "fin_L2_FACT_Journal_01_prepare populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
# fin_usp_L2_FACT_Journal_01_prepare()
