#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import sys
from optparse import OptionParser
from s3ql import fs, s3
from s3ql.s3cache import S3Cache
from s3ql.common import init_logging, get_credentials, get_cachedir, get_dbfile
from s3ql.database import ConnectionManager 
import os
import stat
import logging
#import cProfile

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
parser.add_option("--bgcommit", action="store_true", default=False,
                  help="Activate background commit mode. In this mode all datatransfer to S3 "
                  'is done in a separate background thread. The cache may exceed the ' 
                  'cachesize without bounds and is not flushed even when the fs is unmounted.')
parser.add_option("-o", type='string', default=None,
                  help="For compatibility with mount(8). Specifies mount options in "
                       "the form key=val,key2=val2,etc. Valid keys are s3timeout, "
                       "allow_others, allow_root, cachesize, noatime, bgcommit.")
                       

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
                if key == 'bgcommit':
                    options.bgcommit = True                    
                else:
                    raise ValueError()
        except ValueError:
            parser.error('Unknown mount option: "%s"' % pair)


#
# Pass on fuse options
#
fuse_opts = dict()
fuse_opts[b"nonempty"] = True
if options.allow_others:
    fuse_opts[b"allow_others"] = True
if options.allow_root:
    fuse_opts[b"allow_root"] = True
if options.single:
    fuse_opts[b"nothreads"] = True
if options.fg:
    fuse_opts[b"foreground"] = True


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
        "through S3 or the file system has not been unmounted cleanly. In\n" \
        "the later case you should run fsck on the system where the\n" \
        "file system has been mounted most recently!\n"
    sys.exit(1)

# Init cache
if (os.path.exists(cachedir) and bucket['s3ql_bgcommit'] != 'yes') or os.path.exists(dbfile):
    print >> sys.stderr, \
        "Local cache files already exists! Either you are trying to\n" \
        "to mount a file system that is already mounted, or the filesystem\n" \
        "has not been unmounted cleanly. In the later case you should run\n" \
        "fsck.\n"
    sys.exit(1)

if bucket['s3ql_bgcommit'] == 'yes' and not os.path.exists(cachedir):
    sys.stderr.write(
        'File system has been mounted in background commit mode, but no\n'
        'local cache exists. In background commit mode, the file system\n'
        'can only be mounted on one computer with the same cache directory.\n' 
        'To disable the background commit flag, run mount the file system\n'
        'without background commit on the system where it was mounted most recently.\n')
    sys.exit(1)
    
# Init cache + get metadata
try:
    if options.fg:
        log.info("Downloading metadata...")

    os.mknod(dbfile, stat.S_IRUSR | stat.S_IWUSR | stat.S_IFREG)
    if not os.path.exists(cachedir):
        os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    bucket.fetch_to_file("s3ql_metadata", dbfile)

    # Check that the fs itself is clean
    dbcm = ConnectionManager(dbfile, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
    if dbcm.get_val("SELECT needs_fsck FROM parameters"):
        sys.stderr.write("File system damaged, run fsck!\n")
        sys.exit(1)

    #
    # Start server
    #
    bucket.store("s3ql_dirty", "yes")
    if options.bgcommit:
        bucket['s3ql_bgcommit'] = 'yes'
        
    cache =  S3Cache(bucket, cachedir, options.cachesize * 1024, dbcm, 
                     options.s3timeout, options.bgcommit)
    server = fs.Server(cache, dbcm, options.noatime)
    if options.fg:
        log.info('Mounting filesystem..')
    
    # Uncomment this and import cProfile to activate profiling.
    # Note that profiling only works in single threaded mode.
    #retcache = list()
    #def doit():
    #    retcache.append(server.main(mountpoint, **fuse_opts))
    #cProfile.run('doit()', 'profile_psyco.dat')
    #ret = retcache[0]   
    ret = server.main(mountpoint, **fuse_opts)

    cache.close()
    if not options.bgcommit:
        bucket['s3ql_bgcommit'] = 'no'

    # Upload database
    dbcm.execute("VACUUM")
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
        if not options.bgcommit:
            os.rmdir(cachedir)
    except:
        pass

if not ret:
    log.warn('Some errors occured while handling requests. '
             'Please examine the logs for more information.')
    sys.exit(1)
else:
    sys.exit(0)