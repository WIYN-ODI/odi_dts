

import argparse

def parse():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "inputdir", nargs='*',
        help="List of exposure directories to transfer")

    parser.add_argument(
        "--sshkey",
        default=None,
        help="Specify ssh-key for remote login")

    parser.add_argument(
        '--db', default=False, action='store_true',
        help="query ODI database")

    parser.add_argument(
        '--nthreads', default=5, type=int,
        help="number of threads for parallel transfer")

    parser.add_argument(
        "--monitor", default=False, action="store_true",
        help="keep monitoring the database")

    parser.add_argument(
        "--chunksize", default=25, type=int,
        help="number of frames to transfer between DB queries (only in monitoring mode)")

    parser.add_argument(
        "--keep", dest="delete_when_done", default=True, action='store_false',
        help="Keep intermediate files (fz-compressed FITS & tar-ball) rather than deleting them when done")

    parser.add_argument(
        "--checkevery", default=15, type=float,
        help="Pause between database checks when no new exposures were found")

    parser.add_argument(
        "--newexp_poll", default=0.5, type=float,
        help="Pause between database checks when no new exposures were found")

    parser.add_argument(
        "--timeframe", default=7, type=float,
        help="time-scale in days to search for exposures to transfer (e.g 7 means all exposures taken during past 7 days)")

    parser.add_argument(
        "--resend", default=False, action="store_true",
        help="Re-send files even though they might be marked as complete already"
    )

    parser.add_argument(
        "--verbose", default=False, action="store_true",
        help="Print still-alive time-stamp to terminal"
    )

    parser.add_argument(
        "--dryrun", default=False, action="store_true",
        help="dry-run only, do not actually do any work"
    )

    parser.add_argument(
        "--special", default="",
        help="special keyword for developer only"
    )

    args = parser.parse_args()

    return args
