# Databricks notebook source
class layerZeroProcessingStatus():
    @staticmethod
    def updateExecutionStatus():
        try:
            lstOfImportStatus = list()

            [lstOfImportStatus.append([log["logID"],
                          log["fileID"],
                          log["fileName"],
                          log["fileType"],
                          log["tableName"],
                          PROCESS_ID(log["processID"]).value,
                          log["validationID"],
                          log["routineName"],
                          log["statusID"],
                          log["status"],
                          log["startTime"],
                          log["endTime"],
                          log["comments"]])
                for log in list(gl_executionLog.values())]

            gView = "executionLog_" + fileID.replace('-','_').lower()
            dfExecutionLog =  spark.createDataFrame(schema = gl_logSchema, data = lstOfImportStatus)
            dfExecutionLog.createOrReplaceGlobalTempView(gView)     
        except Exception as err:
            raise
