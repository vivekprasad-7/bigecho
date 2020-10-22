import mysql.connector
import datetime
import getStartDateEndDate
from getStartDateEndDate import findMinID
from connectionDB import Database
from pipeline1 import pipeline1
from pipeline2 import pipeline2
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
obj = Database()
pipe = pipeline1()
pipe2 = pipeline2()
try:
    latestRunResult = obj.selectRetryResult()

    jobStatus = [] # updating latest run Status
    for i, j in enumerate(latestRunResult):
        executionFlag = 0  # represents whether we should run the pipeline for duplicatd pipeline
        if i == 0:
            executionFlag = 0
        else:
            for key in range(i, 0, -1):
                if latestRunResult[key - 1][0] == j[0]:
                    if jobStatus[key - 1] == "FAILED":
                        executionFlag = 1
                        break

        #print(j[0])
        if executionFlag == 0:
            latestRetryCount = j[2]
            startdate = j[3]
            enddate = j[4]
            if latestRetryCount < 3:
                ### RE RUN Python JOBS
                if j[0] == 2:
                    minID = findMinID(j[0])
                    enddate = getStartDateEndDate.finddate(j[1], j[0])
                    pipe2.main(callStatus=1, minid=minID, created_date=j[1],
                              enddate=enddate)  # naming convention isretry
                if j[0] == 1:
                    minID = findMinID(j[0])
                    enddate = getStartDateEndDate.finddate(j[1],j[0])
                    pipe.main(callStatus=1, minid=minID, created_date=j[1],enddate=enddate) # naming convention isretry
                ## Some CODE
                latestRunStatus = pipe.jobExecutionStatus()
                jobStatus.append(latestRunStatus)
                if latestRunStatus == 'FAILED':
                    latestRetryCount += 1
                    obj.retryAfterFailedPipeline(latestRetryCount, current_time, j[0],j[1])
            else:
                jobStatus.append("FAILED")
        if executionFlag == 1:
            jobStatus.append("FAILED")
        print(":: below execution")
        print(executionFlag)
    print(jobStatus)

except  Exception as e:
    print(e)

