#!/usr/bin/env python3

import os
import sys
import cx_Oracle

from pprint import pprint

import config



def query_exposures_for_transfer():

    dsn = cx_Oracle.makedsn(host='odiserv.kpno.noao.edu',
                            port=1521, sid='odiprod')
    print(dsn)

    # establish connection to Oracle-DB
    # connection = odidb.connect('odi@//odiserv.kpno.noao.edu:1521/odiprod/')
    connection = cx_Oracle.connect(config.oracle_user, config.oracle_password, dsn)

    cursor = connection.cursor()

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

    cursor.execute(sql)
    results = cursor.fetchall()
    pprint(results)

    cursor.close()
    connection.close()



if __name__ == "__main__":

    query_exposures_for_transfer()