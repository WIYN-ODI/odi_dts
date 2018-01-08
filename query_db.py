#!/usr/bin/env python3

import os
import sys
import cx_Oracle
import datetime
import threading

from pprint import pprint

import config

class ODIDB(object):

    def __init__(self):

        dsn = cx_Oracle.makedsn(host='odiserv.kpno.noao.edu',
                                port=1521, sid='odiprod')
        # print(dsn)

        # establish connection to Oracle-DB
        # connection = odidb.connect('odi@//odiserv.kpno.noao.edu:1521/odiprod/')
        self.connection = cx_Oracle.connect(config.oracle_user, config.oracle_password, dsn)

        self.cursor = self.connection.cursor()

        self.lock = threading.Lock()

    def query_exposures_for_transfer(self):

        self.lock.acquire(blocking=True)

        # select all exposures that have not been marked as complete (return code=0) yet
        sql = """\
    SELECT  exp.id,exp.exposure,exp.fileaddr
    FROM    EXPOSURES exp
    WHERE   exp.id NOT IN
    (
    SELECT  expid
    FROM    EXPOSURE_EVENT
    WHERE event like '%:: 0%' OR 
          event like 'ppa ingested OK%'
    )
    """

        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        # pprint(results)

        self.lock.release()

        return results

    def mark_exposure_archived(self, obsid, event=None):

        self.lock.acquire(blocking=True)

        # convert obsid into expid
        sql = "SELECT ID FROM EXPOSURES WHERE EXPOSURE LIKE '%%%s'" % (obsid)
        print(sql)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        exposure_id = results[0][0]
        print("EXPOSURE_ID=",results, exposure_id)

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
        print(event_time)


        # now mark it as complete
        if (event is None):
            event = "test new DTS transfer complete :: -1"
        # sql = "INSERT INTO EXPOSURE_EVENT (EXPID, EVENT) VALUES (:1, :2)"
        # sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENT) VALUES (SEQ_EVENTID.next_val, :1, :2)"
        # sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENTTIME, EVENT) VALUES ((SELECT MAX(ID) FROM EXPOSURE_EVENT)+1, :1, :2, :3)"
        sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENTTIME, EVENT) VALUES ((SELECT MAX(ID) FROM EXPOSURE_EVENT)+1, :expid, :eventtime, :event)"

        values = {'expid': exposure_id, 'eventtime':event_time, 'event':event}
        print(sql)
        print(values)
        # self.cursor.execute(sql, values)
        self.cursor.prepare(sql)
        self.cursor.setinputsizes(eventtime=cx_Oracle.TIMESTAMP)
        self.cursor.execute(None, values)
        self.connection.commit()

        self.lock.release()

        return


    def __del__(self):
        self.cursor.close()
        self.connection.close()



if __name__ == "__main__":

    db = ODIDB()

    exposures = db.query_exposures_for_transfer()
    print("Found %d exposures to be transferred" % (len(exposures)))
    for e in exposures: #((id,exposure,path) in exposures):
        (id,exposure,path) = e
        print("%6d %25s   %s" % (id,exposure,path))