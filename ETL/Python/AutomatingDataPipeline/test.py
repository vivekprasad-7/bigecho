from pipeline1 import pipeline1
from connectionDB import Database
x = pipeline1()
obj = Database()

latestRunResult = obj.selectRetryResult()
jobStatus = [] # updating latest run Status
executionFlag = 0  # represents whether we should run the pipeline for duplicatd pipeline
print(latestRunResult)
for i, j in enumerate(latestRunResult):
    if i == 0:
        executionFlag = 0
        jobStatus.append('FAILED')
    else:

        for key in range(i, 0, -1):

            if latestRunResult[key - 1][0] == j[0]:
                print('here')
                print(key)
                if jobStatus[key - 1] == "FAILED":
                    executionFlag = 1

                    break
        if j == 1:
            jobStatus.append("FAILED")

    #print(executionFlag)