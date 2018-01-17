#!/usr/bin/env python3

import os
import sys
import cx_Oracle
import datetime
import threading

from pprint import pprint

import config
import commandline

class ODIDB(object):

    def __init__(self):

        print("Establishing connection to database")
        dsn = cx_Oracle.makedsn(host='odiserv.kpno.noao.edu',
                                port=1521, sid='odiprod')
        # print(dsn)

        # establish connection to Oracle-DB
        # connection = odidb.connect('odi@//odiserv.kpno.noao.edu:1521/odiprod/')
        self.connection = cx_Oracle.connect(config.oracle_user, config.oracle_password, dsn)

        self.cursor = self.connection.cursor()

        self.lock = threading.Lock()



    def query_exposures_for_transfer(self, timeframe=7., all=False):

        self.lock.acquire() #blocking=True

        now = datetime.datetime.now()
        delta_t = datetime.timedelta(days=timeframe)
        cutoff_time = now - delta_t

        # select all exposures that have not been marked as
        # - complete (return code=0) yet
        # - tried but failed
        sql = """\
    SELECT  exp.id,exp.exposure,exp.fileaddr
    FROM    EXPOSURES exp
    WHERE   exp.CREATETIME > :cutoff"""

        if (not all):
            sql += """ AND    
    exp.id NOT IN
    (
    SELECT  expid
    FROM    EXPOSURE_EVENT
    WHERE event like '%:: 0%' OR 
          event like 'ppa ingested OK%' OR
          event like 'pyDTS%ERROR%'
    )
    ORDER BY exp.id DESC
    """

        self.cursor.prepare(sql)
        self.cursor.setinputsizes(cutoff=cx_Oracle.TIMESTAMP)
        self.cursor.execute(None, {'cutoff': cutoff_time,})
        #self.cursor.execute(sql)
        results = self.cursor.fetchall()
        # pprint(results)

        self.lock.release()

        return results


    def check_for_exposures(self, last_id):
        self.lock.acquire()
        # sql = "SELECT ID,CREATETIME,EXPOSURE FROM EXPOSURES WHERE ID > %d" % (last_id)

        sql = """select ID,CREATETIME,EXPOSURE from exposures exp where exp.id not in (select expid from exposure_event where event like 'ppa notification OK%') order by exp.createtime  desc
        """
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        self.lock.release()

        return results


    def mark_exposure_archived(self, obsid, event=None, dryrun=False):

        self.lock.acquire() #blocking=True)
        print("Adding completion report to EXPOSURE_EVENT table")
        # convert obsid into expid
        sql = "SELECT ID FROM EXPOSURES WHERE EXPOSURE LIKE '%%%s'" % (obsid)
        # print(sql)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        exposure_id = results[0][0]
        # print("EXPOSURE_ID=",results, exposure_id)

        # start a sequence to auto-increment the ID value
        # try:
        #     self.cursor.execute("CREATE SEQUENCE SEQ_EVENTID INCREMENT BY 1")
        # except cx_Oracle.DatabaseError as e:
        #     # most likely this means ORA-00955: name is already used by an existing object
        #     # so we can safely ignore this
        #     pass
        # self.cursor.execute("SELECT SEQ_EVENTID.nextval FROM EXPOSURE_EVENT")
        # print("SEQUENCE.nextval=",self.cursor.fetchone())

        # self.cursor.execute("SELECT MAX(ID) FROM EXPOSURE_EVENT")
        # print("MAX(ID)=",self.cursor.fetchall())

        # get time-stamp
        event_time = datetime.datetime.now()
        # print(event_time)


        # now mark it as complete
        if (event is None):
            event = "test new DTS transfer complete :: -1"
        # sql = "INSERT INTO EXPOSURE_EVENT (EXPID, EVENT) VALUES (:1, :2)"
        # sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENT) VALUES (SEQ_EVENTID.next_val, :1, :2)"
        # sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENTTIME, EVENT) VALUES ((SELECT MAX(ID) FROM EXPOSURE_EVENT)+1, :1, :2, :3)"
        # sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENTTIME, EVENT) VALUES ((SELECT MAX(ID) FROM EXPOSURE_EVENT)+1, :expid, :eventtime, :event)"
        sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENTTIME, EVENT) VALUES (EXP_EVENTID.nextval, :expid, :eventtime, :event)"

        values = {'expid': exposure_id, 'eventtime':event_time, 'event':event}
        # print(sql)
        # print(values)
        # self.cursor.execute(sql, values)
        if (not dryrun):
            self.cursor.prepare(sql)
            self.cursor.setinputsizes(eventtime=cx_Oracle.TIMESTAMP)
            self.cursor.execute(None, values)
            self.connection.commit()
        else:
            print("DRYRUN: %s (%s)" % (sql, str(values)))

        self.lock.release()

        return

    def get_directory_from_obsid(self, obsid):
        sql = "SELECT FILEADDR FROM EXPOSURES WHERE EXPOSURE='%s'" % (obsid)
        # print(sql)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        if (len(results) != 1):
            raise ValueError("Wrong number of exposures (%d) with OBSID=%s" % (
                len(results), obsid
            ))
        # print(results)
        return results[0][0]


    def __del__(self):
        self.cursor.close()
        self.connection.close()



if __name__ == "__main__":

    db = ODIDB()

    args = commandline.parse()
    exposures = db.query_exposures_for_transfer(timeframe=args.timeframe, all=True)

    print("Found %d exposures to be transferred" % (len(exposures)))
    for n,e in enumerate(exposures): #((id,exposure,path) in exposures):
        if (n%50 == 0):
            print("")
            print("#####  exp-id  ___________________OBSID   ........................... file-location ...........................")
            print("")
        (id,exposure,path) = e
        print("%5d: %6d %25s   %s" % (n+1,id,exposure,path))