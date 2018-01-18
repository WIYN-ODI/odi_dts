#!/usr/bin/env python3

import os
import sys
import subprocess
import time
import datetime

import threading
import queue
import logging

import dts
import query_db
import dts_logger
import config
import commandline
import ppa_communication
import db_watcher

class DTS_Thread(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue, out_queue=None, database=None, delete_when_done=True, ppa=None):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        self.database = database
        self.delete_when_done = delete_when_done
        self.logger = logging.getLogger("DTS")
        self.ppa = ppa

    def run(self):
        while(True):
            #grabs host from queue
            try:
                exposure_info = self.queue.get()
            except queue.Empty as e:
                break
            if (exposure_info is None):
                break

            (dir, obsid, extra) = exposure_info
            try:
                exposure2archive = dts.DTS(dir, obsid=obsid, database=self.database,
                                           cleanup=self.delete_when_done,
                                           extra=extra,
                                           ppa=self.ppa)
            except ValueError as v:
                self.logger.error("ERROR starting DTS for OBSID %s in %s" % (obsid, dir))
                if (not os.path.isdir(dir) and obsid is not None):
                    # Directory is missing
                    # mark this exposure as problematic but done to avoid
                    # running into the same problem again when we check the
                    # database again.
                    event_report = "pyDTS ERROR %s: directory %s not found :: 0" % (
                        obsid, dir
                    )
                    self.database.mark_exposure_archived(
                        obsid=obsid, event=event_report)

            #signals to queue job is done
            self.queue.task_done()



def transfer_onetime(odidb, ppa, args):

    dts_queue = queue.Queue()

    try: # Handle Ctrl-C and other aborts
        if (args.db):
            #
            # Check database for new exposures to transfer
            #
            exposures = odidb.query_exposures_for_transfer(timeframe=args.timeframe)
            input_dirs = [(dir, obsid) for (id,obsid,dir) in exposures]

        elif (len(args.inputdir) == 1 and os.path.isfile(args.inputdir[0])):
            #
            # Read list of exposures from input file given as only parameter on the
            # command line
            #
            input_dirs = []
            with open(args.inputdir[0], "r") as f:
                for line in f.readlines():
                    if (line.startswith("#") or len(line)<15):
                        continue
                    input_dirs.append((None, line.split(",")[0]))

        else:
            #
            # We are given a list of exposure directories to transfer
            #
            input_dirs = [(dir,None) for dir in args.inputdir]

        # create a work-queue and populate it with exposures to archive
        for (dir, obsid) in input_dirs:
            dts_queue.put((dir, obsid))

        #
        # Start the worker threads
        #
        threads = []
        threads_needed = args.nthreads  # if args.nthreads < len(input_dirs) else len(input_dirs)
        for i in range(threads_needed):
            t = DTS_Thread(queue=dts_queue, database=odidb,
                           delete_when_done=args.delete_when_done)
            t.setDaemon(True)
            t.start()
            threads.append(t)

            # send the queue termination message
            dts_queue.put(None)

        # join threads to wait until all are shutdown
        for t in threads:
            t.join()

            #
            # start the worker threads
            #
            # for i in input_dirs:
            #     task = dts_queue.get()
            #     print(task)

    except (KeyboardInterrupt, SystemExit) as e:
        print("Closing down")
        return

    return




class DTS_ExposureSender(threading.Thread):

    def __init__(self, odidb, ppa, args):
        threading.Thread.__init__(self)

        self.odidb = odidb
        self.ppa = ppa
        self.dts_queue = queue.Queue()
        self.args = args
        self.logger = logging.getLogger("ExposureSender")

        self.shutdown = False
        self.setDaemon(True)

    def run(self):
        self.logger.info("Starting up the DTS ExposureSender process")
        while (not self.shutdown):

            #
            # Check for new exposures
            #
            exposures = self.odidb.query_exposures_for_transfer(
                timeframe=self.args.timeframe,
                include_resends=True
            )
            input_dirs = [(dir, obsid,extra) for (id, obsid, dir,extra) in exposures]
            if (len(input_dirs) <= 0):
                # no new files to transfer have been found
                if (self.args.verbose):
                    sys.stdout.write(
                        #"\nNo files to transfer at %s, checking again soon\033[F" % (
                        "No files to transfer at %s, checking again soon\n" % (
                        str(datetime.datetime.now())))
                    sys.stdout.flush()
                delay = 0
                while (delay < self.args.checkevery and not self.shutdown):
                    time.sleep(0.1)
                    delay += 0.1
                continue

            #
            # Limit the number of exposures to be worked on in parallel
            #
            truncated_list = False
            if (len(input_dirs) > self.args.chunksize):
                truncated_list = True
                logger.info("Limiting work to %d exposures (out of %d)" % (
                    self.args.chunksize, len(input_dirs)
                ))
            input_dirs = input_dirs[:self.args.chunksize]

            #
            # create a work-queue and populate it with exposures to archive
            #
            for (dir, obsid, extra) in input_dirs:
                # print(dir, obsid)
                self.dts_queue.put((dir, obsid, extra))

            #
            # Start the worker threads
            #
            self.threads = []
            threads_needed = self.args.nthreads  # if args.nthreads < len(input_dirs) else len(input_dirs)
            for i in range(threads_needed):
                t = DTS_Thread(queue=self.dts_queue, database=self.odidb,
                               delete_when_done=self.args.delete_when_done,
                               ppa=self.ppa,
                               )
                t.setDaemon(True)
                t.start()
                self.threads.append(t)

                # send the queue termination message
                self.dts_queue.put(None)

            # join threads to wait until all are shutdown
            for t in self.threads:
                while (not self.shutdown and t.is_alive()):
                    t.join(timeout=0.1)

            # wait a little before checking again for more frames.
            time.sleep(0.002) # flush out log messages
            print("Checking for new frames after short break")
            if (not truncated_list):
                delay = 0
                while (delay < self.args.checkevery and not self.shutdown):
                    delay += 0.1
                    time.sleep(0.1)


if __name__ == "__main__":

    args = commandline.parse()

    # setup logging
    dtslog = dts_logger.dts_logging()

    odidb = query_db.ODIDB()
    ppa = ppa_communication.PPA()
    logger = logging.getLogger("ODI-DTS")

    # make sure to create the DTS scratch directory
    if (not os.path.isdir(config.tar_scratchdir)):
        os.mkdir(config.tar_scratchdir)

    if (not args.db or not args.monitor):
        # This is just a one-time transfer
        transfer_onetime(odidb=odidb, ppa=ppa, args=args)
        sys.exit(0)

    #
    # Coming up is the default way of running DTS:
    # Checking the database for new exposures on a regular basis
    #

    # Start the exposure-sender thread
    sender = DTS_ExposureSender(odidb=odidb, ppa=ppa, args=args)
    sender.start()

    watcher = db_watcher.ExposureWatcher(ppa_comm=ppa, db_connection=odidb, args=args)
    watcher.start()

    #
    # Now wait for the command to shut-down
    #
    while (True):
        try:
            time.sleep(1)
        except (SystemExit, KeyboardInterrupt):
            watcher.shutdown = True
            sender.shutdown = True
            print("\rOrdering sender and watcher to shutdown")
            break
            #time.sleep(1)

    print("Shutting down connections to ODI database and PPA")
    odidb.close()
    ppa.close()

    print("All done - have a nice day!")


