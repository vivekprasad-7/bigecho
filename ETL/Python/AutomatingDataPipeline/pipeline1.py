from connectionDB import Database
obj = Database()

class pipeline1:
    jobExecutionStatusFlag = None



    def main(self,callStatus=None,minid=None,created_date=None,startdate= None,enddate= None):
        try :
            created_date = str(created_date)
            load_status = 'IN-PROGRESS'
            pipeline_id = 1
            pipeline_name = "ExecuteCorePGMerchantsExtraction"
            if callStatus == 0:
                obj.insertBeforePipeline(pipeline_id,pipeline_name,created_date,load_status)
                lastinsertedid = obj.lastInsertedID()[0][0]


            ##Production code
            #
            maxid = 629
            ##

            if callStatus == 0:
                load_status = 'SUCCEED'
                obj.updateAfterPipeline(maxid, load_status, lastinsertedid)
            if callStatus == 1:
                self.jobExecutionStatusFlag = 'SUCCEED'
                obj.retryAfterPipeline(maxid, pipeline_id, created_date)
        except Exception as e:
            if callStatus == 0:
                load_status = 'FAILED'
                obj.updateAfterFailedPipeline(load_status, lastinsertedid)
            if callStatus == 1:
                self.jobExecutionStatusFlag = 'FAILED'
            print(e)

    def jobExecutionStatus(self):
        return self.jobExecutionStatusFlag
