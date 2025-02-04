#!/usr/bin/env python


import argparse
import os
import glob
import astropy.io.fits as pyfits

from query_db import ODIDB
import commandline
import cx_Oracle

if __name__ == "__main__":

    special_options = [
        (['--reason'], dict(dest='reason', type=str, help="reason for resend", default=""))
        ]

    args = commandline.parse(special_options)
    # print(args.inputdir)

    # get OBSIDs for all input files
    obsids = []
    for fn in args.inputdir:
        if (os.path.isdir(fn)):
            dirlist = glob.glob(fn + "/*.fits")
            # print("Input was directory, checking files: %s" % (", ".join(dirlist)))
            for _fn in dirlist:
                hdulist = pyfits.open(_fn)
                try:
                    obsid = hdulist[0].header['OBSID']
                    print("Found OBSID %s in %s" % (obsid, _fn))
                    obsids.append(obsid)
                    break
                except KeyError:
                        continue
        elif (os.path.isfile(fn)):
            try:
                hdulist = pyfits.open(fn)
                obsid = hdulist[0].header['OBSID']
                print("Found OBSID %s in %s" % (obsid, _fn))
                obsids.append(obsid)
                break
            except:
                print("Unable to get OBSID from %s" % (fn))
        else:
            print("Not sure what to do with %s" % (fn))

    print("Adding resend requests for %s" % ("\n ** ".join(['']+obsids)))

    # Connect to database
    db = ODIDB()

    # get EXPID for list of files
    q = ' OR '.join(["EXPOSURE LIKE '%%%s%%'" % (obsid) for obsid in obsids])
    sql = "SELECT EXPOSURE, ID FROM EXPOSURES WHERE %s" % (q)
    print(sql)

    # execute SQL query
    db.lock.acquire()
    db.cursor.execute(sql)
    results = db.cursor.fetchall()
    db.lock.release()

    dryrun = args.dryrun
    reason = args.reason
    if (reason is None):
        reason = ""
    reason += " // by force_resend"

    # Add re-send commands for each exposure
    for result in results:
        obsid, exposure_id = result
        print("%s ==> %s" % (obsid, exposure_id))

        db.request_exposure_resend(obsid=obsid, reason=reason, exposure_id=exposure_id, dryrun=dryrun)

        # timestamp = datetime.datetime.now()
        # event = "ppa request resend (%s: %s // force_resend)" % (obsid, reason)
        # sql = "INSERT INTO EXPOSURE_EVENT (ID, EXPID, EVENTTIME, EVENT) VALUES (EXP_EVENTID.nextval, :expid, :eventtime, :event)"
        # values = {'expid': exposure_id, 'eventtime':timestamp, 'event':event}
        # if (not dryrun):
        #     db.cursor.prepare(sql)
        #     db.cursor.setinputsizes(eventtime=cx_Oracle.TIMESTAMP)
        #     db.cursor.execute(None, values)
        #     db.connection.commit()
        # else:
        #     print("DRYRUN: %s" % (sql))

    #     # print(results)
    #
    # else:
    #     # Just checking
    #     print("Checking database")
