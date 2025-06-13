# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import split, explode

class knowledge():
    @staticmethod
    def knowledgeMetadata_load():
        try:

            print('')
            #objGenHelper = gen_genericHelper()
            
            #gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_BusinessDatatypeValueMapping.delta")
            #gl_metadataDictionary['knw_LK_GD_CurrencyShiftingStaticJoins'] =objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_CurrencyShiftingStaticJoins.delta")
            
            #lst_scoped_analytics = list()
            
            #df_SCOPED_ANALYTICS = objGenHelper.gen_readFromFile_perform(gl_commonParameterPath + "knw_LK_CD_Parameter.csv")\
            #.filter(col("name")=="SCOPED_ANALYTICS")
            #lst_scoped_analytics.extend(df_SCOPED_ANALYTICS\
            #.select(explode(split("parameterValue",','))).rdd.map(lambda x:x[0]).collect())

            #if ("RSL" in lst_scoped_analytics)|("CP" in lst_scoped_analytics)|("ARV" in lst_scoped_analytics):
            #  gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentType'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_ClientKPMGDocumentType.delta")
              
            #  gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentTypeClient'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_ClientKPMGDocumentTypeClient.delta")
              
            #  gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentTypeKPMG'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_ClientKPMGDocumentTypeKPMG.delta")
              
            #  gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentTypeKPMGGroup'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_ClientKPMGDocumentTypeKPMGGroup.delta")
              
            #  gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentTypeKPMGShort'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_ClientKPMGDocumentTypeKPMGShort.delta")
              
            #  gl_metadataDictionary['knw_LK_GD_ClientKPMGDocumentTypeKPMGShortToAttribute'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_ClientKPMGDocumentTypeKPMGShortToAttribute.delta")

            #  gl_metadataDictionary['knw_LK_GD_CutOffInterpretation'] =  objGenHelper.\
            #     gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_CutOffInterpretation.delta")
                    
            #  gl_metadataDictionary['knw_LK_GD_CutOffDateComparisonType'] =  objGenHelper.\
            #     gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_CutOffDateComparisonType.delta")

            #  gl_metadataDictionary['knw_LK_GD_CutOffDateComparison'] =  objGenHelper.\
            #     gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_CutOffDateComparison.delta")

            #  gl_metadataDictionary['knw_LK_GD_CutOffDateOneComparison'] =  objGenHelper.\
            #     gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_CutOffDateOneComparison.delta")

            #  gl_metadataDictionary['knw_LK_GD_CutOffDateOnePeriodComparison'] =  objGenHelper.\
            #     gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_CutOffDateOnePeriodComparison.delta")

            #  gl_metadataDictionary['knw_LK_GD_SubBranchDocumentSetClassification'] =  objGenHelper.\
            #     gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_SubBranchDocumentSetClassification.delta")
              
            #  gl_metadataDictionary['knw_LK_GD_InternationalCommercialTerm'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_InternationalCommercialTerm.delta")
              
            #  gl_metadataDictionary['knw_LK_GD_CurrencyConversionMetadata'] =  objGenHelper.\
            #    gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_CurrencyConversionMetadata.delta")
              
        except Exception as err:
            raise

