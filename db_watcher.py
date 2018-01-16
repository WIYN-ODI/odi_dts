#!/usr/bin/env python3


import cx_Oracle
import threading
import time
import queue
import logging

import ppa_communication
import query_db
import dts_logger

# shutdown = False

# queue = queue.Queue()
# registered = True
#
# def new_exposure_callback(message):
#     global queue
#     print("Message type:", message.type)
#     if message.type == cx_Oracle.EVENT_DEREG:
#         print("Deregistration has taken place...")
#         registered = False
#         return
#     print("Message database name:", message.dbname)
#     print("Message tables:")
#     for table in message.tables:
#         print("--> Table Name:", table.name)
#         print("--> Table Operation:", table.operation)
#         if table.rows is not None:
#             print("--> Table Rows:")
#             for row in table.rows:
#                 print("--> --> Row RowId:", row.rowid)
#                 print("--> --> Row Operation:", row.operation)
#                 print("-" * 60)
#         print("=" * 60)


class ExposureWatcher(threading.Thread):

    def __init__(self, ppa_comm, db_connection, last_exposure=0):
        threading.Thread.__init__(self)
        self.setDaemon(True)

        self.ppa_comm = ppa_comm
        self.odidb = db_connection

        self.logger = logging.getLogger("ExposureWatcher")

        # self.sub = self.odidb.connection.subscribe(
        #     callback=new_exposure_callback,
        #     timeout=30,
        #     operations=
        #     qos=cx_Oracle.SUBSCR_QOS_ROWIDS,
        # )
        #
        # print("Subscription:", self.sub)
        # print("--> Connection:", self.sub.connection)
        # print("--> Callback:", self.sub.callback)
        # print("--> Namespace:", self.sub.namespace)
        # print("--> Protocol:", self.sub.protocol)
        # print("--> Timeout:", self.sub.timeout)
        # print("--> Operations:", self.sub.operations)
        # print("--> Rowids?:", bool(self.sub.qos & cx_Oracle.SUBSCR_QOS_ROWIDS))
        # self.sub.registerquery("select * from DEBUGLOG")

        self.last_known = last_exposure
        self.shutdown = False

    def new_exposure(self):

        return


    def run(self):

        print("Running")
        while (not self.shutdown):

            new_exposures = odidb.check_for_exposures(self.last_known)

            if (len(new_exposures) > 0):
                for exp in new_exposures:
                    id, createtime, exposure = exp
                    if (id > self.last_known):
                        self.last_known = id

                    self.ppa_comm.report_exposure(
                        timestamp=createtime,
                        obsid=exposure,
                        msg_type="create"
                    )
                    self.logger.info("Reporting new exposure to PPA: %s" % (exposure))

            # sql = "select ID, EXPOSURE, CREATETIME from EXPOSURES ELSE WHERE id > %d" % (self.last_known)
            # self.odidb.
            # print("Waiting for new exposures....")


            loop = 0
            while (loop < 100 and not self.shutdown):
                time.sleep(0.01)
                loop += 1
            # if (self.shutdown):
            #     break
            #
            # try:
            #     time.sleep(1)
            # except (KeyboardInterrupt, SystemExit):
            #     break




if __name__ == "__main__":

    odidb = query_db.ODIDB()
    ppa = ppa_communication.PPA()
    dtslog = dts_logger.dts_logging()

    last_exposure = 58174

    watcher  = ExposureWatcher(ppa, odidb, last_exposure)
    watcher.start()

    while (True):
        try:
            watcher.join(10)
        except (SystemExit, KeyboardInterrupt):
            watcher.shutdown = True
            print("Setting watcher shutdown to True")
            break
            #time.sleep(1)
