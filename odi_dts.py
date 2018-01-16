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

class DTS_Thread(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue, out_queue=None, database=None, delete_when_done=True):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        self.database = database
        self.delete_when_done = delete_when_done
        self.logger = logging.getLogger("DTS")

    def run(self):
        while(True):
            #grabs host from queue
            try:
                exposure_info = self.queue.get()
            except queue.Empty as e:
                break
            if (exposure_info is None):
                break

            (dir, obsid) = exposure_info
            try:
                exposure2archive = dts.DTS(dir, obsid=obsid, database=self.database,
                                           cleanup=self.delete_when_done)
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


if __name__ == "__main__":

    args = commandline.parse()

    # setup logging
    dtslog = dts_logger.dts_logging()

    odidb = query_db.ODIDB()
    dts_queue = queue.Queue()
    first_run = True

    # make sure to create the DTS scratch directory
    if (not os.path.isdir(config.tar_scratchdir)):
        os.mkdir(config.tar_scratchdir)

    logger = logging.getLogger("ODI-DTS")
    while(first_run or args.monitor):

        truncated_list = False
        try:
            if (args.db):
                exposures = odidb.query_exposures_for_transfer(timeframe=args.timeframe)
                input_dirs = [(dir, obsid) for (id,obsid,dir) in exposures]
                if (len(input_dirs) <= 0):
                    # no new files to transfer have been found
                    sys.stdout.write("\rNo files to transfer at %s, checking again soon" % (str(datetime.datetime.now())))
                    sys.stdout.flush()
                    time.sleep(args.checkevery)
                    continue
                else:
                    print()

                if (args.monitor):
                    if (len(input_dirs) > args.chunksize):
                        truncated_list = True
                        logger.info("Limiting work to %d exposures (out of %d)" % (
                            args.chunksize, len(input_dirs)
                        ))
                    input_dirs = input_dirs[:args.chunksize]

                # print(input_dirs)
            elif (len(args.inputdir) == 1 and os.path.isfile(args.inputdir[0])):
                # given a file to read input files from
                input_dirs = []
                with open(args.inputdir[0], "r") as f:
                    for line in f.readlines():
                        if (line.startswith("#") or len(line)<15):
                            continue
                        input_dirs.append((None, line.split(",")[0]))

            else:
                input_dirs = []
                # for dir in args.inputdir:
                #     obsid = get_obsid_from_directory(dir)
                #     input_dirs.append((dir,obsid))
                input_dirs = [(dir,None) for dir in args.inputdir]

            # create a work-queue and populate it with exposures to archive
            for (dir, obsid) in input_dirs:
                # print(dir, obsid)

                dts_queue.put((dir, obsid))
                # object = dts.DTS(dir, obsid=obsid, database=odidb)
                # odidb.mark_exposure_archived(obsid)
                #break

            # print(len(input_dirs))
                #transfer = dts.DTS(dir)

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

            # dts_queue.join()

            first_run = False

            if (args.db and args.monitor):
                # wait a little before checking again for more frames.
                time.sleep(0.002) # flush out log messages
                print("Checking for new frames after short break")
                time.sleep(1. if truncated_list else args.checkevery)

        except (KeyboardInterrupt, SystemExit) as e:
            print("Closing down")
            break
