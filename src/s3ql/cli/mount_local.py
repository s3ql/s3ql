#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
import sys
from optparse import OptionParser
from time import sleep
from s3ql.common import init_logging
from s3ql.database import ConnectionManager
from s3ql import fs, s3, mkfs, fsck, llfuse
from s3ql.s3cache import S3Cache 
import os
import tempfile
import logging 

# This is a very big method with lots of branches, variables
# and statements
#pylint: disable-msg=R0912,R0914,R0915
def main():
    #
    # Parse command line
    #
    parser = OptionParser(
        usage="%prog  [options] <mountpoint>\n"
              "       %prog --help",
        description="Emulates S3QL filesystem using in-memory storage"
        "instead of actually connecting to S3. Only for testing purposes.")
    
    
    parser.add_option("--debuglog", type="string",
                      help="Write debugging information in specified file. You will need to "
                            'use --debug as well in order to get any output.')
    parser.add_option("--debug", action="append", 
                      help="Activate debugging output from specified facility. Valid facility names "
                            "are: fs, fuse, s3, fsck, mkfs, frontend. "
                            "This option can be specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("--allow_others", action="store_true", default=False, help=
                      "Allow other users to access the filesystem as well and enforce unix permissions. "
                      "(if neither this option nor --allow_others is specified, only the mounting user "
                      "can access the file system, and has full access to every file, independent of "
                      "individual permissions.")
    parser.add_option("--allow_root", action="store_true", default=False,
                      help="Allow root to access the filesystem as well and enforce unix permissions. "
                      "(if neither this option nor --allow_others is specified, only the mounting user "
                      "can access the file system, and has full access to every file, independent of "
                      "individual permissions.")
    parser.add_option("--fg", action="store_true", default=False,
                      help="Do not daemonize, stay in foreground")
    parser.add_option("--single", action="store_true", default=False,
                      help="Single threaded operation only")
    parser.add_option("--atime", action="store_true", default=False,
                      help="Update file and directory access time. Will decrease performance.")
    parser.add_option("--blocksize", type="int", default=1,
                      help="Maximum size of s3 objects in KB (default: %default)")
    parser.add_option("--cachesize", type="int", default=10,
                      help="Cache size in kb (default: %default). Should be at least 10 times "
                      "the blocksize of the filesystem, otherwise an object may be retrieved and "
                      "written several times during a single write() or read() operation." )
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
    
    if not os.path.exists(mountpoint):
        sys.stderr.write('Mountpoint does not exist.\n')
        sys.exit(1)
    
    # Activate logging
    if options.debug is not None and options.debuglog is None and not options.fg:
        sys.stderr.write('Warning! Debugging output will be lost. '
                         'You should use either --fg or --debuglog.\n')
        
    # Foreground logging until we daemonize
    init_logging(True, options.quiet, options.debug, options.debuglog)
    log = logging.getLogger("frontend")
    
    #
    # Initialize local bucket
    #
    
    bucket = s3.LocalBucket()
    bucket.tx_delay = options.txdelay
    bucket.prop_delay = options.propdelay
    
    dbfile = tempfile.NamedTemporaryFile()
    cachedir = tempfile.mkdtemp() + "/"
    try:
        dbcm = ConnectionManager(dbfile.name, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
        with dbcm() as conn:
            mkfs.setup_db(conn, options.blocksize * 1024)
        log.debug("Temporary database in " + dbfile.name)
        
        #
        # Start server
        #
        
        cache =  S3Cache(bucket, cachedir, options.cachesize, dbcm,
                         timeout=options.propdelay+1)
        try:
            fuse_opts = [ b"nonempty", b'fsname=s3ql_local', b'debug' ]
            #              b'large_read', b'big_writes' ]
            if options.allow_others:
                fuse_opts.append(b'allow_others')
            if options.allow_root:
                fuse_opts.append(b'allow_root')
            if options.allow_others or options.allow_root:
                fuse_opts.append(b'default_permissions')
                
            operations = fs.Operations(cache, dbcm, not options.atime)
            llfuse.init(operations)
            
            # Switch to background if necessary
            init_logging(options.fg, options.quiet, options.debug, options.debuglog)
            
            llfuse.main(mountpoint, fuse_opts, single=options.single, 
                        foreground=options.fg)
            
        finally:
            cache.close()
        
        # We have to make sure that all changes have been committed by the
        # background threads
        sleep(options.propdelay)
        
        #
        # Do fsck
        #
        if options.fsck:
            with dbcm() as conn:
                if not fsck.fsck(conn, cachedir, bucket, checkonly=True):
                    log.warn("fsck found errors -- preserving database in %s", dbfile)
                    os.rmdir(cachedir)
                    sys.exit(1)
    finally:
        dbfile.close()
        os.rmdir(cachedir)
        
    if operations.encountered_errors:
        log.warn('Some errors occured while handling requests. '
                 'Please examine the logs for more information.')
        sys.exit(1)
    else:
        sys.exit(0)
