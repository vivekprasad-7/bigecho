
import mysql.connector
import datetime
class Database:

    def __init__(self):
        self._conn = mysql.connector.connect(user='root', host='127.0.0.1', password='Programming1!', database='arms')
        self._cursor = self._conn.cursor()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor


    def query(self, sql):
        self.cursor.execute(sql)
        return self.fetchall()

    def Insertquery(self, sql):
        self.cursor.execute(sql)
        self._conn.commit()

    def fetchall(self):
        return self.cursor.fetchall()

    def insertBeforePipeline(self,pipeline_id,pipeline_name,created_date,load_status):
        sql = "Insert Into tbl_Pipeline_Status values(%d,'%s',NULL,NULL,NULL,'%s',NULL,'%s',0,NULL,NULL);" % (
            pipeline_id, pipeline_name,created_date, load_status
        )
        return self.Insertquery(sql)

    def updateAfterPipeline(self,maxid,load_status,lastinsertedid):
        sql = "update tbl_Pipeline_Status set lastid='%s',runstatus='%s' where id= %d" % (
                    maxid, load_status, lastinsertedid
                )
        return self.Insertquery(sql)

    def updateAfterFailedPipeline(self, maxid, load_status, lastinsertedid):
        sql = "update tbl_Pipeline_Status set runstatus='%s' where id= %d" % (
             load_status, lastinsertedid
        )
        return self.Insertquery(sql)
    def updateAfterFailedPipelineRetry(self, maxid, load_status, lastinsertedid):
        sql = "update tbl_Pipeline_Status set runstatus='FAILED' where id= %d" % (
             load_status, lastinsertedid
        )
        return self.Insertquery(sql)
    def retryAfterPipeline(self, maxid, pipeline_id, created_date):
        sql = "Updat tbl_Pipeline_Status set lastid = %s, runstatus = 'SUCCEED' where pipeline_id=%d and createddate='%s'" % (
                maxid,pipeline_id,created_date)

        return self.Insertquery(sql)

    def retryAfterPipeline2(self, maxid, pipeline_id, created_date):
        sql = "Update tbl_Pipeline_Status set lastid = %s, runstatus = 'SUCCEED' where pipeline_id=%d and createddate='%s'" % (
            maxid, pipeline_id, created_date)

        return self.Insertquery(sql)

    def retryAfterFailedPipeline(self, latestRetryCount, current_time, pipeline_id, created_date):
        sql = "Update tbl_Pipeline_Status set retrycount = %d, lastretry = '%s' where pipeline_id=%d and createddate='%s'" % (
                latestRetryCount,current_time,pipeline_id,created_date)
        return self.Insertquery(sql)

    def getMinId(self,pipeline_ID):
        sql ="select (lastid) from tbl_Pipeline_Status where  pipeline_id=%d and runstatus='SUCCEED' order by createddate desc limit 1" % (pipeline_ID)
        return self.query(sql)

    def selectRetryResult(self):
        sql ="SELECT pipeline_Id,createddate,retryCount, startdate, enddate FROM tbl_Pipeline_Status where runStatus='FAILED' order by createddate"
        return self.query(sql)
    def lastInsertedID(self):
        sql ="select LAST_INSERT_ID()"
        return self.query(sql)