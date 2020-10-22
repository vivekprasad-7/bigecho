import datetime
from datetime import timedelta
from datetime import datetime
from pytz import timezone
from connectionDB import Database
import pytz
tz_IST = timezone('Asia/Kolkata')  ## take created date while retrying
utc = timezone('UTC')  #
# created_date = '2020-10-13 10:20:00'
# date_time_obj = datetime.strptime(created_date, '%Y-%m-%d %H:%M:%S')
obj = Database()


def finddate(created_date,pipeline_id):
    #created_date = '2020-10-13 10:20:00'
    created_date = str(created_date)
    date_time_obj = datetime.strptime(created_date, '%Y-%m-%d %H:%M:%S')

    if pipeline_id==3: #ExecuteBlazenetExtraction.py


        StartDate = ((date_time_obj- timedelta(days=2)).replace(hour=18, minute=30, second=00,
                                                                       microsecond=0, tzinfo=utc))
        EndDate = ((date_time_obj- timedelta(days=1)).replace(hour=18, minute=29, second=59,
                                                                      microsecond=0, tzinfo=utc))
        #print(StartDate)
        #print(EndDate)
        return (StartDate, EndDate)

    elif pipeline_id in (4,5):

        EndDate = "'"+ (date_time_obj- timedelta(days=1)).replace(hour=18, minute=29, second=59, microsecond=0).strftime('%Y-%m-%d %H:%M:%S.%f')+ "'"
        #print(EndDate)
        return (EndDate)

    elif pipeline_id == 6:
        to_date =(datetime.now(tz_IST) - timedelta(days=1)).replace(hour=18, minute=29, second=59, microsecond=0, tzinfo=utc)
        to_date =  to_date.timestamp()
        to_date = int(to_date)*1000
        #print(to_date)
        return (to_date)
    elif pipeline_id in(7,8,14,15):
        StartDate = (date_time_obj- timedelta(days=1))
        EndDate = (date_time_obj- timedelta(days=1))
        StartDate = "'" + StartDate.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S.%f') + "'"
        EndDate = "'" + EndDate.replace(hour=23, minute=59, second=59, microsecond=0).strftime('%Y-%m-%d %H:%M:%S.%f') + "'"
        #print(StartDate)
        #print(EndDate)
        return (StartDate, EndDate)
    # elif pipeline_id ==10:
    #     local_dt = date_time_obj.replace(tzinfo=pytz.utc).astimezone(tz_IST)
    #     checkpoint_start = "2015-09-25T00:00:00+0530"
    #     start = datetime.strptime(checkpoint_start, "%Y-%m-%dT%H:%M:%S%z")
    #     end = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    #     date_array = (start + timedelta(days=x) for x in range(0, (end - start).days))
    #
    #     return date_array


def findMinID(pipeline_ID):
    try:
        latestRunResult = obj.getMinId(pipeline_ID)
        return(latestRunResult[0][0])

    except Exception as e:
        print(e)
        print("I am here2")

