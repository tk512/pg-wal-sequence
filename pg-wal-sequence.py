#!/usr/bin/env python
#
# Usage:
# pg-wal-sequence.py --directory <your WAL directory path>
#
# Given a directory containing PostgreSQL WAL archives, this tool
# will analyze the WAL files and ensure that there are no gaps which
# would break any potential recovery of data.
#
#  Return code 0 for "OK"
#  Return code 1 for "WARNING"
#  Return code 2 for "CRITICAL"
#
# If a gap exists, but the next WAL archive is less than 1 day old, then
# a status of WARNING is returned. After 1 day, the status goes to CRITICAL.
#
# This script is compatible with Nagios.

import glob, os
import sys
import argparse

OK, WARNING, CRITICAL = range(3) # Status codes that makes script compatible with Nagios
WAL_EXTENSION = ''               # Add your own extension if necessary
GAP_THRESHOLD = 3600*24          # Gap threshold value

class NagiosStatus(Exception):
    def __init__(self, status, message):
        print >> sys.stderr, message
        sys.exit(status)

def getWalNo(fileName):
    return os.path.basename(fileName).rstrip(WAL_EXTENSION)

def getTimeline(oldest, newest):
    oldestTimeline = getWalNo(oldest)[:8]
    newestTimeline = getWalNo(newest)[:8]
    if oldestTimeline != newestTimeline:
        raise NagiosStatus(WARNING, 
                'Found two timelines: %s and %s. Not supported' % (oldestTimeline, newestTimeline))
    return oldestTimeline

def getWalFileParts(walNo):
    logical = walNo[8:16]
    physical = walNo[16:] 
    return logical + physical

def buildWalNo(timeline, seq):
    walNo = timeline + '{:016X}'.format(int(seq))
    return walNo

def getNextWalModificationTime(baseDir, timeline, nextSeq, maxSeq):
    while nextSeq <= maxSeq:
        logical = '{:X}'.format(nextSeq)[:-2]
        segment = '{:X}'.format(nextSeq)[-2:]
        cur = int(logical + '0'*6 + segment, 16)
        walNo = buildWalNo(timeline, int(logical + '0'*6 + segment, 16))
        walFile = os.path.join(baseDir, walNo + WAL_EXTENSION)
        if os.path.exists(walFile):
            return os.path.getmtime(walFile)
        nextSeq += 1

def examineGapDifference(gapDifference, walNo):
    if gapDifference > GAP_THRESHOLD:
        raise NagiosStatus(CRITICAL, 
            'Missing WAL files at %s (next WAL more than 1 day old)' % walNo)
    else:
        raise NagiosStatus(WARNING, 
            'Missing WAL files at %s (next WAL less than 1 day old)' % walNo)

def verifySequence(baseDir, oldest, newest):
    timeline = getTimeline(oldest, newest)
    fromSequence = getWalFileParts(getWalNo(oldest))
    toSequence = getWalFileParts(getWalNo(newest))

    prevWalModificationTime = None
    fromSeq = int(fromSequence[:8].lstrip('0') + fromSequence[-2:], 16)
    maxSeq = int(toSequence[:8].lstrip('0') + toSequence[-2:], 16)
    
    gapExists = False
    for n in range(fromSeq, maxSeq + 1):
        segment = '{:X}'.format(n)[-2:]
        logical = '{:X}'.format(n)[:-2]
        # the segments are always 00-FF, never more than 2 digits
        cur = int(logical + '0'*6 + segment, 16)
        walNo = buildWalNo(timeline, cur) 
        walFile = os.path.join(baseDir, walNo + WAL_EXTENSION)
        if not os.path.exists(walFile):
            if  prevWalModificationTime:
                nextWalModificationTime = getNextWalModificationTime(baseDir, timeline, n+1, maxSeq) 
                if not nextWalModificationTime:
                    gapExists = True
                    continue

                gapDifference = nextWalModificationTime - prevWalModificationTime
                examineGapDifference(gapDifference, walNo)
        else:
            if gapExists:
                curWalModificationTime = getNextWalModificationTime(baseDir, timeline, n, maxSeq) 
                gapDifference = curWalModificationTime - prevWalModificationTime
                examineGapDifference(gapDifference, walNo)
             
            prevWalModificationTime = os.path.getmtime(walFile)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Verify WAL sequences for PostgreSQL')
    parser.add_argument('--directory', dest='directory', required=True,
                       help='PostgreSQL WAL directory')

    args = parser.parse_args()

    if not os.path.exists(args.directory):
        raise NagiosStatus(WARNING, 'The directory %s does not exist' % args.directory)

    files = list(glob.iglob(args.directory + '/*' + WAL_EXTENSION))
    if not files:
        raise NagiosStatus(WARNING, 'The directory %s does not contain WAL files' % args.directory)

    oldest = min(files, key=os.path.getmtime)
    newest = max(files, key=os.path.getmtime)

    verifySequence(args.directory, oldest, newest)

    raise NagiosStatus(OK, 'WAL file sequences verified to be OK');
