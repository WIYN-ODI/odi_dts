#!/usr/bin/env python3


import cx_Oracle
import threading
import time
import queue
import logging
import datetime
import sys

import ppa_communication
import query_db
import dts_logger
import commandline


if __name__ == "__main__":

    odidb = query_db.ODIDB()
    ppa = ppa_communication.PPA()
    dtslog = dts_logger.dts_logging()
    args = commandline.parse()

    if (args.special == "fix_old"):
        older_than = args.timeframe
        current = datetime.datetime.now()
        old_cutoff = current - datetime.timedelta(days=older_than)

        exp = odidb.check_for_exposures()
        for (id,time,obsid) in exp:
            if (time < old_cutoff):
                print("Marking old exposure as reported to PPA: %s" % (obsid))
                odidb.mark_exposure_archived(
                    obsid=obsid,
                    event='ppa notification OK -- manually fixing old file for database only',
                    dryrun=args.verbose,
                )

        sys.exit(0)

    elif (args.special == "fix_backlog"):

        exp = odidb.check_for_exposures()
        for (id,createtime,obsid) in exp:

            # reconstruct the create/send/send_complete messages from the
            # database exposure_event for the complete transfer

            #
            # now get the completion timestamp from the exposure_event database
            #
            sql_send_event = "SELECT eventtime,event FROM exposure_event where event like '%%pyDTS %s%%'" % (obsid)
            # print sql_send_event
            odidb.cursor.execute(sql_send_event)
            results = odidb.cursor.fetchall()

            if (len(results) <= 0):
                # This file has not been reported yet
                # nothing to do therefore
                continue

            #print(results)
            eventtime,event = results[0]
            print(event)
            transfer_time = float(event.split("transfer(")[1].split("s")[0])
            #print(transfer_time)

            # now we have a send-start and send-complete timestamp
            send_start = eventtime - datetime.timedelta(seconds=transfer_time)
            send_end = eventtime

            print("Sending -->create<-- to PPA")
            ppa_create = ppa.report_exposure(
                obsid=obsid,
                msg_type='create',
                timestamp=createtime,
            )
            if (ppa_create):
                print("Updating database marking ppa notification OK")
                odidb.mark_exposure_archived(
                    obsid=obsid,
                    event='ppa notification OK -- manual create/send/send_complete to PPA for %s' % (obsid),
                )
            print("Sending -->send<-- to PPA")
            ppa.report_exposure(
                obsid=obsid, msg_type="send",
                timestamp=send_start,
            )
            print("Sending -->send_complete<-- to PPA")
            ppa.report_exposure(
                obsid=obsid, msg_type="send_complete",
                timestamp=send_end,
            )


            # break

            # if (time < old_cutoff):
            #     print("Marking old exposure as reported to PPA: %s" % (obsid))
            #     odidb.mark_exposure_archived(
            #         obsid=obsid,
            #         event='ppa notification OK -- manually fixing old file for database only',
            #         dryrun=args.verbose,
            #     )
        sys.exit(0)

    elif (args.special == "fix_all_pyDTS"):

        sql = """\
        select v.eventtime,v.event,e.exposure 
        from exposure_event v 
        join exposures e on e.id=v.expid 
        where v.event like 'pyDTS%OK :: 0' AND v.eventtime > :cutoff
        """
        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=args.timeframe)

        odidb.cursor.prepare(sql)
        odidb.cursor.setinputsizes(cutoff=cx_Oracle.TIMESTAMP)
        odidb.cursor.execute(None, {'cutoff': cutoff_time,})
        #self.cursor.execute(sql)
        #odidb.cursor.execute(sql)
        results = odidb.cursor.fetchall()
        print(results)
        sys.exit(0)

        for eventtime,event,obsid in results:
            #print(eventtime,event,obsid)
            print("resending ppa notifications for %s" %(obsid))

            transfer_time = float(event.split("transfer(")[1].split("s")[0])
            # print(transfer_time)

            # now we have a send-start and send-complete timestamp
            send_start = eventtime - datetime.timedelta(seconds=transfer_time + 20)
            send_end = eventtime

            print("Sending -->send<-- to PPA")
            ppa.report_exposure(
                obsid=obsid, msg_type="send",
                timestamp=send_start,
            )
            print("Sending -->send_complete<-- to PPA")
            ppa.report_exposure(
                obsid=obsid, msg_type="send_complete",
                timestamp=send_end,
            )

        # pprint(results)
        sys.exit(0)

