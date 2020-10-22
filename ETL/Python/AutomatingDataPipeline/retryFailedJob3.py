import mysql.connector
import datetime
import getStartDateEndDate
import Merchant_Dimention as MD
from connectionDB import Database
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
obj = Database()
try:
    latestRunResult = obj.selectRetryResult

    jobStatus = ['Succeed', 'Succeed', 'FAILED']
    executionFlag = 0  # represents whether we should run the pipeline for duplicatd pipeline
    for i, j in enumerate(latestRunResult):
        if i == 0:
            executionFlag = 0
        else:
            for key in range(i, 0, -1):
                if latestRunResult[key - 1][0] == j[0]:
                    if jobStatus[key - 1] == "FAILED":
                        executionFlag = 1
                        break

        #print(executionFlag)
        if executionFlag == 0:
            latestRetryCount = j[2]
            startdate = j[3]
            enddate = j[4]
            if latestRetryCount < 3:
                ### RE RUN Python JOBS
                if j[0] == 4:
                    minID = MD.findMinID(j[0])
                    enddate = getStartDateEndDate.finddate(j[1],j[0])
                    MD.main(callStatus=1, minid=minID, created_date=j[1],enddate=enddate) # naming convention isretry
                ## Some CODE
                latestRunStatus = MD.jobExecutionStatus()
                jobStatus.append(latestRunStatus)
                if latestRunStatus == 'FAILED':
                    latestRetryCount += 1
                    obj.retryAfterPipeline(latestRetryCount, current_time, j[0],j[1])

except  Exception as e:
    print(e)

#steps
#1. select id from tbl_Pipeline_Status where status='FAILED'
# In Loop
#2. run python job for that id1 ; with para lastid,startdate,enddate
#3. select status from tbl_Pipeline_Status where id=id1 ; if status ='Succeed' ;
# fetch retryCount and do retryCount += 1
# update lastRetry= CurrentTime


