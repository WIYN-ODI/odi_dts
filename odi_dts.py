#!/usr/bin/env python3

import os
import sys
import subprocess

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
    parser.add_argument('--nthreads', default=1, type=int,
                        help="number of threads for parallel transfer")

    args = parser.parse_args()

    odidb = query_db.ODIDB()

    if (args.db):
        exposures = odidb.query_exposures_for_transfer()
        input_dirs = [(dir, obsid) for (id,obsid,dir) in exposures]
    else:
        input_dirs = []
        # for dir in args.inputdir:
        #     obsid = get_obsid_from_directory(dir)
        #     input_dirs.append((dir,obsid))
        input_dirs = [(dir,None) for dir in args.inputdir]

    # create a work-queue and populate it with exposures to archive
    dts_queue = queue.Queue()
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
    for i in range(args.nthreads):
        t = DTS_Thread(queue=dts_queue, database=odidb)
        t.setDaemon(True)
        t.start()

    dts_queue.join()
