#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

import sys
if sys.version_info[0] < 2 or \
    (sys.version_info[0] == 2 and sys.version_info[1] < 6):
    sys.stderr.write('Python version too old, must be between 2.6.0 and 3.0!\n') 
    sys.exit(1)
if sys.version_info[0] > 2:
    sys.stderr.write('Python version too new, must be between 2.6.0 and 3.0!\n')
    sys.exit(1)
    

# Python boto uses several deprecated modules
import warnings
warnings.filterwarnings("ignore", "", DeprecationWarning, "boto")

from optparse import OptionParser
#from getpass  import getpass
from s3ql import fs, s3
from s3ql.s3cache import S3Cache
from s3ql.common import init_logging, get_credentials, get_cachedir, get_dbfile
from s3ql.cursor_manager import CursorManager 
import os
import stat
import logging

#
# Parse command line
#
parser = OptionParser(
    usage="%prog  [options] <bucketname> <mountpoint>\n"
          "       %prog --help",
    description="Mounts an amazon S3 bucket as a filesystem.")

parser.add_option("--awskey", type="string",
                  help="Amazon Webservices access key to use. The password is "
                  "read from stdin. If this option is not specified, both access key "
                  "and password are read from ~/.awssecret (separated by newlines).")
parser.add_option("--debug", action="append", 
                  help="Activate debugging output from specified facility. Valid facility names "
                        "are: fs, fs.fuse, s3, frontend. "
                        "This option can be specified multiple times.")
parser.add_option("--quiet", action="store_true", default=False,
                  help="Be really quiet")
parser.add_option("--s3timeout", type="int", default=50,
                  help="Maximum time to wait for propagation in S3 (default: %default)")
parser.add_option("--allow_others", action="store_true", default=False,
                  help="Allow others users to access the filesystem")
parser.add_option("--allow_root", action="store_true", default=False,
                  help="Allow root to access the filesystem")
parser.add_option("--fg", action="store_true", default=False,
                  help="Do not daemonize, stay in foreground")
parser.add_option("--noatime", action="store_true", default=False,
                  help="Do not update file and directory access time. May improve performance.")
parser.add_option("--cachesize", type="int", default=51200,
                  help="Cache size in kb (default: 51200 (50 MB)). Should be at least 10 times "
                  "the blocksize of the filesystem, otherwise an object may be retrieved and "
                  "written several times during a single write() or read() operation." )
parser.add_option("--single", action="store_true", default=False,
                  help="Single threaded operation only")
parser.add_option("-o", type='string', default=None,
                  help="For compatibility with mount(8). Specifies mount options in "
                       "the form key=val,key2=val2,etc. Valid keys are s3timeout, "
                       "allow_others, allow_root, cachesize, noatime.")
                       

(options, pps) = parser.parse_args()

#
# Verify parameters
#
if not len(pps) == 2:
    parser.error("Wrong number of parameters")
bucketname = pps[0]
mountpoint = pps[1]

#
# Parse -o style mount options
#
if options.o is not None:
    for pair in options.o.split(','):
        try:
            if '=' in pair:
                (key, val) = pair.split('=')
                if key == 's3timeout':
                    options.s3timeout = int(val)
                if key == 'cachesize':
                    options.cachesize = int(val)
                else:
                    raise ValueError()
            else:
                key = pair
                if key == 'allow_others':
                    options.allow_others = True
                if key == 'allow_root':
                    options.allow_root = True
                if key == 'noatime':
                    options.noatime = True                    
                else:
                    raise ValueError()
        except ValueError:
            parser.error('Unknown mount option: "%s"' % pair)

# Use this once we have encryption support
#if fs_is_encrypted
#    if sys.stdin.isatty():
#        options.encrypt = getpass("Enter encryption password: ")
#    else:
#        options.encrypt = sys.stdin.readline().rstrip()


#
# Pass on fuse options
#
fuse_opts = dict()
fuse_opts["nonempty"] = True
if options.allow_others:
    fuse_opts["allow_others"] = True
if options.allow_root:
    fuse_opts["allow_root"] = True
if options.single:
    fuse_opts["nothreads"] = True
if options.fg:
    fuse_opts["foreground"] = True


# Activate logging
init_logging(options.fg, options.quiet, options.debug)
log = logging.getLogger("frontend")

#
# Read password
#
(awskey, awspass) = get_credentials(options.awskey)

#
# Connect to S3
#
conn = s3.Connection(awskey, awspass)
bucket = conn.get_bucket(bucketname)

cachedir = get_cachedir(bucketname)
dbfile = get_dbfile(bucketname)

#
# Check consistency
#
log.debug("Checking consistency...")
if bucket["s3ql_dirty"] != "no":
    print >> sys.stderr, \
        "Metadata is dirty! Either some changes have not yet propagated\n" \
        "through S3 or the filesystem has not been umounted cleanly. In\n" \
        "the later case you should run fsck.s3ql on the system where the\n" \
        "filesystem has been mounted most recently!\n"
    sys.exit(1)

# Init cache
if os.path.exists(cachedir) or os.path.exists(dbfile):
    print >> sys.stderr, \
        "Local cache files already exists! Either you are trying to\n" \
        "to mount a filesystem that is already mounted, or the filesystem\n" \
        "has not been umounted cleanly. In the later case you should run\n" \
        "s3fsck.\n"
    sys.exit(1)

# Init cache + get metadata
try:
    if options.fg:
        log.info("Downloading metadata...")

    os.mknod(dbfile, 0600 | stat.S_IFREG)
    os.mkdir(cachedir, 0700)
    bucket.fetch_to_file("s3ql_metadata", dbfile)

    # Check that the fs itself is clean
    cur = CursorManager(dbfile, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
    if cur.get_val("SELECT needs_fsck FROM parameters"):
        sys.stderr.write("Filesystem damaged, run s3fsk!\n")
        sys.exit(1)

    #
    # Start server
    #
    bucket.store("s3ql_dirty", "yes")
    cache =  S3Cache(bucket, cachedir, options.cachesize * 1024, cur, options.s3timeout)
    server = fs.Server(cache, cur, options.noatime)
    if options.fg:
        log.info('Mounting filesystem..')
    ret = server.main(mountpoint, **fuse_opts)
    cache.close()

    # Upload database
    cur.execute("VACUUM")
    log.debug("Uploading database..")
    if bucket.has_key("s3ql_metadata_bak_2"):
        bucket.copy("s3ql_metadata_bak_2", "s3ql_metadata_bak_3")
    if bucket.has_key("s3ql_metadata_bak_1"):
        bucket.copy("s3ql_metadata_bak_1", "s3ql_metadata_bak_2")
    bucket.copy("s3ql_metadata", "s3ql_metadata_bak_1")
    bucket.store_from_file("s3ql_metadata", dbfile)
    bucket.store("s3ql_dirty", "no")

# Remove database
finally:
    # Ignore exceptions when cleaning up
    try:
        log.debug("Cleaning up...")
        os.unlink(dbfile)
        os.rmdir(cachedir)
    except:
        pass

if not ret:
    log.warn('Some errors occured while handling requests. '
             'Please examine the logs for more information.')
    sys.exit(1)
else:
    sys.exit(0)