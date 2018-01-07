#!/usr/bin/env python3

import os
import sys
import subprocess

import argparse

import dts
import query_db

def get_obsid_from_directory(dirname):
    return None

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
        for dir in args.inputdir:
            obsid = get_obsid_from_directory(dir)
            input_dirs.append((dir,obsid))
        # input_dirs = args.inputdir

    for (dir, obsid) in input_dirs:
        print(dir, obsid)
        odidb.mark_exposure_archived(obsid)
        break

    print(len(input_dirs))
        #transfer = dts.DTS(dir)

