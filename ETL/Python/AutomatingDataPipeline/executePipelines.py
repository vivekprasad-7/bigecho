import Merchant_Dimention as MD
import mysql.connector
import mysql.connector
import datetime
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
from connectionDB import Database

#Function to fetch Min Id
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
obj = Database()

def findMinID(pipeline_ID):
    try:
        latestRunResult = obj.getMinId(pipeline_ID)[0][0]
        return(latestRunResult)

    except Exception as e:
        print(e)



minID = findMinID(1)
MD.main(callStatus=0,minid =minID, created_date=current_time)


'''#checking StartDateEndDate Pipeline
pipeline_id = 6
if pipeline_id in(3,7,8,14,15):
    startdate = GSD.finddate('2020-10-13 10:20:00',pipeline_id)[0]
    enddate = GSD.finddate('2020-10-13 10:20:00', pipeline_id)[1]

if pipeline_id in(4,5,6):
    enddate = GSD.finddate('2020-10-13 10:20:00',pipeline_id)
    startdate = None

print(startdate)
print(enddate)
'''

'''
def ExecuteCorePGMerchantsExtraction():
    pipeline_ID = 1
    minID = findMinID(pipeline_ID)
    minUserID = minID.split('|')[1]
    minTxnID = minID.split('|')[0]
    # Run the main(callStatus=0 , minTxnID) function
    # Run main()
    print(minUserID)
    print(minTxnID)
'''