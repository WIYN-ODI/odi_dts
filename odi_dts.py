#!/usr/bin/env python3

import os
import sys
import subprocess

import argparse

import dts
import query_db

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("inputdir", nargs='*',
                        help="List of exposure directories to transfer")
    parser.add_argument("--sshkey",
                        default=None,
                        help="Specify ssh-key for remote login")

    parser.add_argument('--db', default=False, action='store_true',
                        help="query ODI database")

    args = parser.parse_args()

    if (args.db):
        pass
        odidb = query_db.ODIDB()
        exposures = odidb.query_exposures_for_transfer()
        input_dirs = [dir for (id,exp,dir) in exposures]
    else:
        input_dirs = args.inputdir

    for dir in input_dirs:
        print(dir)

    print(len(input_dirs))
        #transfer = dts.DTS(dir)

