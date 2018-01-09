#!/usr/bin/env python3

import os
import sys
import subprocess
import time

import argparse
import threading
import queue

import dts
import query_db


class DTS_Thread(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue, out_queue=None, database=None):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        self.database = database

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
            exposure2archive = dts.DTS(dir, obsid=obsid, database=self.database)

            #signals to queue job is done
            self.queue.task_done()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("inputdir", nargs='*',
                        help="List of exposure directories to transfer")
    parser.add_argument("--sshkey",
                        default=None,
                        help="Specify ssh-key for remote login")

    parser.add_argument('--db', default=False, action='store_true',
                        help="query ODI database")
    parser.add_argument('--nthreads', default=5, type=int,
                        help="number of threads for parallel transfer")
    parser.add_argument("--monitor", default=False, action="store_true",
                        help="keep monitoring the database")
    parser.add_argument("--chunksize", default=25, type=int,
                        help="number of frames to transfer between DB queries (only in monitoring mode)")

    args = parser.parse_args()
    odidb = query_db.ODIDB()
    dts_queue = queue.Queue()
    first_run = True

    while(first_run or args.monitor):

        try:
            if (args.db):
                exposures = odidb.query_exposures_for_transfer()
                input_dirs = [(dir, obsid) for (id,obsid,dir) in exposures]
                if (args.monitor):
                    input_dirs = input_dirs[:args.chunksize]

                # print(input_dirs)
            else:
                input_dirs = []
                # for dir in args.inputdir:
                #     obsid = get_obsid_from_directory(dir)
                #     input_dirs.append((dir,obsid))
                input_dirs = [(dir,None) for dir in args.inputdir]

            # create a work-queue and populate it with exposures to archive
            for (dir, obsid) in input_dirs:
                print(dir, obsid)

                dts_queue.put((dir, obsid))
                # object = dts.DTS(dir, obsid=obsid, database=odidb)
                # odidb.mark_exposure_archived(obsid)
                #break

            print(len(input_dirs))
                #transfer = dts.DTS(dir)

            #
            # start the worker threads
            #
            # for i in input_dirs:
            #     task = dts_queue.get()
            #     print(task)

            threads = []
            threads_needed = args.nthreads if args.nthreads < len(input_dirs) else len(input_dirs)
            for i in range(threads_needed):
                t = DTS_Thread(queue=dts_queue, database=odidb)
                t.setDaemon(True)
                t.start()
                threads.append(t)

            dts_queue.join()

            first_run = False

            if (args.db and args.monitor):
                # wait a little before checking again for more frames.
                print("Checking for new frames after short break")
                time.sleep(5)

        except (KeyboardInterrupt, SystemExit) as e:
            print("Closing down")
            break

