#!/usr/bin/env python


import argparse
import os
import glob
import astropy.io.fits as pyfits

from query_db import ODIDB
import commandline


if __name__ == "__main__":

    args = commandline.parse()
    print(args.inputdir)

    # get OBSIDs for all input files
    obsids = []
    for fn in args.inputdir:
        if (os.path.isdir(fn)):
            dirlist = glob.glob(fn + "/*.fits")
            print("Input was directory, checking files: %s" % (", ".join(dirlist)))
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
    for result in results:
        obsid, exposure_id = result
        print("%s ==> %s" % (obsid, exposure_id))

    #     # print(results)
    #
    # else:
    #     # Just checking
    #     print("Checking database")
