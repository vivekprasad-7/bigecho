from connectionDB import Database
obj = Database()
jobExecutionStatusFlag = None



def main(callStatus=None,minid=None,created_date=None,startdate= None,enddate= None):
    try :
        load_status = 'IN-PROGRESS'
        pipeline_id = 1
        pipeline_name = "ExecuteCorePGMerchantsExtraction"
        obj.insertBeforePipeline(pipeline_id,pipeline_name,load_status)

          = "select LAST_INSERT_ID()"


        ##Production code
        #
        maxid = 625
        ##

        if callStatus == 0:
            load_status = 'SUCCEED'
            obj.updateAfterPipeline(maxid, load_status, lastinsertedid)
        if callStatus == 1:
            jobExecutionStatusFlag = 'SUCCEED'
            obj.updateAfterPipeline(maxid, pipeline_id, created_date)
    except Exception as e:
        if callStatus == 0:
            load_status = 'FAILED'
            obj.updateAfterPipeline(maxid, load_status, lastinsertedid)


def jobExecutionStatus():
    return jobExecutionStatusFlag
