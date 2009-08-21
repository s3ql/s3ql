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
import cProfile


#
# Parse command line
#
parser = OptionParser(
    usage="%prog  [options] <bucketname> <mountpoint>\n"
          "       %prog --help",
    description="Mounts an amazon S3 bucket as a filesystem.")

parser.add_option("--awskey", type="string",
                  help="Amazon Webservices access key to use. If not "
                  "specified, tries to read ~/.awssecret or the file given by --credfile.")
parser.add_option("--debuglog", type="string",
                  help="Write debugging information in specified file. You will need to "
                        'use --debug as well in order to get any output.')
parser.add_option("--credfile", type="string", default=os.environ["HOME"].rstrip("/") + "/.awssecret",
                  help='Try to read AWS access key and key id from this file. '
                  'The file must be readable only be the owner and should contain '
                  'the key id and the secret key separated by a newline. '
                  'Default: ~/.awssecret')
parser.add_option("--cachedir", type="string", default=os.environ["HOME"].rstrip("/") + "/.s3ql",
                  help="Specifies the directory for cache files. Different S3QL file systems "
                  '(i.e. located in different S3 buckets) can share a cache location, even if ' 
                  'they are mounted at the same time. '
                  'You should try to always use the same location here, so that S3QL can detect '
                  'and, as far as possible, recover from unclean umounts. Default is ~/.s3ql.')
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
parser.add_option("--atime", action="store_true", default=False,
                  help="Update file and directory access time. Will decrease performance.")
parser.add_option("--cachesize", type="int", default=51200,
                  help="Cache size in kb (default: 51200 (50 MB)). Should be at least 10 times "
                  "the blocksize of the filesystem, otherwise an object may be retrieved and "
                  "written several times during a single write() or read() operation." )
parser.add_option("--single", action="store_true", default=False,
                  help="Single threaded operation only")
parser.add_option("-o", type='string', default=None,
                  help="For compatibility with mount(8). Specifies mount options in "
                       "the form key=val,key2=val2,etc. Valid keys are s3timeout, "
                       "allow_others, allow_root, cachesize, atime")
                       

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
                if key == 'atime':
                    options.atime = True              
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
if options.debug is not None and options.debuglog is None and not options.fg:
    sys.stderr.write('Warning! Debugging output will be lost. '
                     'You should use either --fg or --debuglog.\n')
# Still in foreground mode
init_logging(True, options.quiet, options.debug, options.debuglog)
log = logging.getLogger("frontend")

#
# Read password
#
(awskey, awspass) = get_credentials(options.credfile, options.awskey)

#
# Connect to S3
#
conn = s3.Connection(awskey, awspass)
bucket = conn.get_bucket(bucketname)

options.cachedir = options.cachedir.rstrip('/')
dbfile = get_dbfile(bucketname, options.cachedir)
cachedir = get_cachedir(bucketname, options.cachedir)

#
# Check consistency
#
log.debug("Checking consistency...")
if bucket["s3ql_dirty"] != "no":
    log.error(
        "Metadata is dirty! Either some changes have not yet propagated\n" 
        "through S3 or the file system has not been unmounted cleanly. In\n" 
        "the later case you should run fsck on the system where the\n" 
        "file system has been mounted most recently!\n")
    sys.exit(1)

# Init cache
if os.path.exists(cachedir) or os.path.exists(dbfile):
    log.error(
        "Local cache files already exists! Either you are trying to\n" 
        "to mount a file system that is already mounted, or the filesystem\n" 
        "has not been unmounted cleanly. In the later case you should run\n" 
        "fsck.\n")
    sys.exit(1)
    
if (bucket.lookup_key("s3ql_metadata").last_modified 
    < bucket.lookup_key("s3ql_dirty").last_modified):
    log.error(
        'Metadata from most recent mount has not yet propagated '
        'through Amazon S3. Please try again later.\n')
    sys.exit(1)
    
# Init cache + get metadata
try:
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
        
    cache = S3Cache(bucket, cachedir, options.cachesize * 1024, dbcm, 
                    options.s3timeout)
    
    server = fs.Server(cache, dbcm, not options.atime)
    log.info('Mounting filesystem..')
    
    # Switch to background if necessary
    init_logging(options.fg, options.quiet, options.debug, options.debuglog)
    
    # Uncomment this and import cProfile to activate profiling.
    # Note that profiling only works in single threaded mode.
    retcache = list()
    def doit():
        retcache.append(server.main(mountpoint, **fuse_opts))
    cProfile.run('doit()', '/home/nikratio/profile_psyco.dat')
    ret = retcache[0]   
    #ret = server.main(mountpoint, **fuse_opts)


    log.info("Filesystem unmounted, committing cache...")
    cache.close()

    # Upload database
    log.info("Uploading database..")
    dbcm.execute("VACUUM")
    if bucket.has_key("s3ql_metadata_bak_2"):
        bucket.copy("s3ql_metadata_bak_2", "s3ql_metadata_bak_3")
    if bucket.has_key("s3ql_metadata_bak_1"):
        bucket.copy("s3ql_metadata_bak_1", "s3ql_metadata_bak_2")
    bucket.copy("s3ql_metadata", "s3ql_metadata_bak_1")
    
    bucket.store("s3ql_dirty", "no")
    bucket.store_from_file("s3ql_metadata", dbfile)
    

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