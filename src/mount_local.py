#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

# Python boto uses several deprecated modules
import warnings
warnings.filterwarnings("ignore", "", DeprecationWarning, "boto")

from optparse import OptionParser
from getpass  import getpass
from time import sleep
from s3ql.common import init_logging
from s3ql.cursor_manager import CursorManager
from s3ql import fs, s3, mkfs, fsck
from s3ql.s3cache import S3Cache
import sys
import os
import tempfile
import logging 

#
# Parse command line
#
parser = OptionParser(
    usage="%prog  [options] <mountpoint>\n"
          "       %prog --help",
    description="Emulates S3QL filesystem using in-memory storage"
    "instead of actually connecting to S3. Only for testing purposes.")

parser.add_option("--debug", action="append", 
                  help="Activate debugging output from specified facility. Valid facility names "
                        "are: fs, fs.fuse, s3, fsck, mkfs, frontend. "
                        "This option can be specified multiple times.")
parser.add_option("--quiet", action="store_true", default=False,
                  help="Be really quiet")
parser.add_option("--allow_others", action="store_true", default=False,
                  help="Allow others users to access the filesystem")
parser.add_option("--allow_root", action="store_true", default=False,
                  help="Allow root to access the filesystem")
parser.add_option("--nonempty", action="store_true", default=False,
                  help="Allow mount if even mount point is not empty")
parser.add_option("--fg", action="store_true", default=False,
                  help="Do not daemonize, stay in foreground")
parser.add_option("--single", action="store_true", default=False,
                  help="Single threaded operation only")
parser.add_option("--encrypt", action="store_true", default=None,
                  help="Create an AES encrypted filesystem")
parser.add_option("--blocksize", type="int", default=1,
                  help="Maximum size of s3 objects in KB (default: %default)")
parser.add_option("--fsck", action="store_true", default=False,
                  help="Runs fsck after the filesystem is unmounted.")
parser.add_option("--txdelay", type="float", default=0.0,
                  help="Simulated transmission time to/from S3 in seconds (default: %default)")
parser.add_option("--propdelay", type="float", default=0.0,
                  help="Simulated propagation in S3 in seconds (default: %default)")


(options, pps) = parser.parse_args()

#
# Verify parameters
#
if not len(pps) == 1:
    parser.error("Wrong number of parameters")
mountpoint = pps[0]


#
# Read password(s)
#
if options.encrypt:
    if sys.stdin.isatty():
        options.encrypt = getpass("Enter encryption password: ")
        if not options.encrypt == getpass("Confirm encryption password: "):
            sys.stderr.write("Passwords don't match.\n")
            sys.exit(1)
    else:
        options.encrypt = sys.stdin.readline().rstrip()


#
# Pass on fuse options
#
fuse_opts = dict()
if options.allow_others:
    fuse_opts["allow_others"] = True
if options.allow_root:
    fuse_opts["allow_root"] = True
if options.nonempty:
    fuse_opts["nonempty"] = True
if options.single:
    fuse_opts["nothreads"] = True
if options.fg:
    fuse_opts["foreground"] = True


# Activate logging
init_logging(options.fg, options.quiet, options.debug)
log = logging.getLogger("frontend")

#
# Initialize local bucket
#

bucket = s3.LocalBucket()
bucket.tx_delay = options.txdelay
bucket.prop_delay = options.propdelay

dbfile = tempfile.NamedTemporaryFile()
cachedir = tempfile.mkdtemp() + "/"
cm = CursorManager(dbfile.name, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
mkfs.setup_db(cm, options.blocksize * 1024)
log.debug("Temporary database in " + dbfile.name)

#
# Start server
#

cache =  S3Cache(bucket, cachedir, options.blocksize * 5, options.blocksize, cm)
server = fs.Server(cache, cm)
server.main(mountpoint, **fuse_opts)
cache.close()

# We have to make sure that all changes have been comitted by the
# background threads
sleep(options.propdelay)

#
# Do fsck
#
if options.fsck:
    if not fsck.fsck(cm, cachedir, bucket, checkonly=True):
        log.info("fsck found errors -- preserving database in %s", dbfile)
        os.rmdir(cachedir)
        sys.exit(1)
    else:
        dbfile.close()
        os.rmdir(cachedir)
        sys.exit(0)
else:
    dbfile.close()
    os.rmdir(cachedir)
    sys.exit(0)
