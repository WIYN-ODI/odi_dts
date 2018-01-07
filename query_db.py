#!/usr/bin/env python3

import os
import sys
import cx_Oracle

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

    def query_exposures_for_transfer(self):

        # select all exposures that have not been marked as complete (return code=0) yet
        sql = """\
    SELECT  exp.id,exp.exposure,exp.fileaddr
    FROM    EXPOSURES exp
    WHERE   exp.id NOT IN
    (
    SELECT  expid
    FROM    EXPOSURE_EVENT
    WHERE event like '%:: 0%'
    )
    """

        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        # pprint(results)
        return results

    def __del__(self):
        self.cursor.close()
        self.connection.close()



if __name__ == "__main__":

    db = ODIDB()

    exposures = db.query_exposures_for_transfer()
    print("Found %d exposures to be transferred" % (len(exposures)))