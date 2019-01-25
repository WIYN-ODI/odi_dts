#!/usr/bin/env python3

import os
import sys
import cx_Oracle
import datetime
import threading
import logging

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

        self.logger = logging.getLogger("ODI-DB")
        self.logger.info("Connection established!")

        self.lock = threading.Lock()



    def query_exposures_for_transfer(self, timeframe=7., all=False, include_resends=False):

        self.lock.acquire() #blocking=True

        now = datetime.datetime.now()
        delta_t = datetime.timedelta(days=timeframe)
        cutoff_time = now - delta_t

        # select all exposures that have not been marked as
        # - complete (return code=0) yet
        # - tried but failed
        sql = """\
    SELECT  exp.id,exp.exposure,exp.fileaddr,'' as extra
    FROM    EXPOSURES exp
    WHERE   exp.CREATETIME > :cutoff"""

        if (not all):
            sql += """ AND    
    exp.id NOT IN
    (
    SELECT  expid
    FROM    EXPOSURE_EVENT
    WHERE (event like '%:: 0%' OR 
           event like 'ppa ingested OK%' OR
           event like 'pyDTS%ERROR%') AND NOT (
           event like 'tar cf%' OR
           event like '/home/dts/bin/dtsq')
    )
    ORDER BY exp.id DESC
    """

        self.cursor.prepare(sql)
        self.cursor.setinputsizes(cutoff=cx_Oracle.TIMESTAMP)
        self.cursor.execute(None, {'cutoff': cutoff_time,})
        #self.cursor.execute(sql)
        results = self.cursor.fetchall()
        # pprint(results)

        if (include_resends):
            # print("Checking for files that need re-sending")


            # query multiple re-send attempts
#             sql = """\
# select exp.id, exp.exposure,exp.fileaddr,'resend' as extra
# from exposures exp where exp.id in (
#     select req.req_expid from (
#         select expid as req_expid,count(expid) as resend_requests from exposure_event
#         where event like 'ppa request resend%'
#         group by expid
#         ) REQ
#     JOIN (SElect expid,count(expid) as resend_count from exposure_event
#         where event like 'pyDTS resend%'
#         group by expid
#         ) comp
#     on req.req_expid = comp.expid
#     where req.resend_requests>comp.resend_count
# )
# """

            sql = """\
select exp.id, exp.exposure,exp.fileaddr,'resend' as extra 
from exposures exp where exp.id in (
select expid from (select expid,
sum(case when event like 'ppa request resend%' then 1 else 0 end) as attempts,
sum(case when event like 'pyDTS resend%' then 1 else 0 end) as completed
from exposure_event
group by expid) where attempts > completed)
            """
            self.cursor.execute(sql)
            resend_results = self.cursor.fetchall()
            # print(resend_results)
            if (len(resend_results) > 0):
                self.logger.info("Found %d files to re-send as requested via PPA" % (len(resend_results)))

            results.extend(resend_results)

        self.lock.release()

        return results


    def check_for_exposures(self, include_problematic=False):
        self.lock.acquire()
        # sql = "SELECT ID,CREATETIME,EXPOSURE FROM EXPOSURES WHERE ID > %d" % (last_id)

        if (include_problematic):

            sql = """\
            select ID,CREATETIME,EXPOSURE 
            from exposures exp where exp.id not in (
              select expid from exposure_event where event like 'ppa notification OK%'
              ) 
            order by exp.createtime  desc
            """
        else:
            sql = """\
            select ID,CREATETIME,EXPOSURE 
            from exposures exp where exp.id not in (
              select expid from exposure_event where event like 'ppa notification OK%'
              ) 
            order by exp.createtime  desc
            """

        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        self.lock.release()

        return results


    def exposureid_from_obsid(self, obsid):

        sql = "SELECT ID FROM EXPOSURES WHERE EXPOSURE LIKE '%%%s'" % (obsid)
        # print(sql)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        exposure_id = results[0][0]

        return exposure_id

    def mark_exposure_archived(self, obsid, event=None, dryrun=False,
                               verbose=False, file_problematic=False):

        self.lock.acquire() #blocking=True)
        print("Adding completion report to EXPOSURE_EVENT table")
        exposure_id = self.exposureid_from_obsid(obsid)

        if (verbose):
            print("exposure id = %s" % (exposure_id))

        # convert obsid into expid
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
        # unlike most other cases, this timestamp better be local mountain
        # time to make the event-time in the database correlate better with
        # IDs in the database.
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

    def request_exposure_resend(self, obsid, timestamp=None, reason=None, dryrun=False):

        if (timestamp is None):
            timestamp = datetime.datetime.now()

        if (reason is None):
            event = "ppa request resend (%s)" % (obsid)
        else:
            event = "ppa request resend (%s: %s)" % (obsid, reason)

        # get exposure-id from OBS-ID
        exposure_id = self.exposureid_from_obsid(obsid)
        if (exposure_id is None):
            # TODO: report error back to PPA
            self.logger.critical("Unable to get exposure ID from OBSID %s" % (obsid))
            return False

        sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENTTIME, EVENT) VALUES (EXP_EVENTID.nextval, :expid, :eventtime, :event)"

        values = {'expid': exposure_id, 'eventtime':timestamp, 'event':event}
        # print(sql)
        # print(values)
        # self.cursor.execute(sql, values)
        if (not dryrun):
            self.cursor.prepare(sql)
            self.cursor.setinputsizes(eventtime=cx_Oracle.TIMESTAMP)
            self.cursor.execute(None, values)
            self.connection.commit()
        else:
            self.logger.info("DRYRUN: %s" % (sql))


    def close(self):
        #print ("Closing connection to ODI database")
        if (self.cursor is not None):
            self.cursor.close()
            self.cursor = None
        if (self.connection is not None):
            self.connection.close()
            self.connection = None

    def __del__(self):
        self.close()



if __name__ == "__main__":

    db = ODIDB()

    args = commandline.parse()
    exposures = db.query_exposures_for_transfer(timeframe=args.timeframe, all=True,
                                                include_resends=True)

    print("Found %d exposures to be transferred" % (len(exposures)))
    for n,e in enumerate(exposures): #((id,exposure,path) in exposures):
        if (n%50 == 0):
            print("")
            print("#####  exp-id  ___________________OBSID   ........................... file-location ...........................")
            print("")
        (id,exposure,path,extra) = e
        print("%5d: %6d %25s   %s" % (n+1,id,exposure,path))