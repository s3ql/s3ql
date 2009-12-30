'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import unicode_literals, division, print_function

import sys
from optparse import OptionParser
from s3ql import fs, s3
from s3ql.s3cache import S3Cache
from s3ql.common import init_logging, get_credentials, get_cachedir, get_dbfile
from s3ql.database import ConnectionManager 
import llfuse
import os
import stat
import logging
#import cProfile

__all__ = [ 'main', 'add_common_mount_opts', 'run_server' ]

log = logging.getLogger("frontend")

def main():
    '''Mount S3QL file system
    
    This function writes to stdout/stderr and calls `system.exit()` instead
    of returning.
    '''
    
    # Parse options
    options = parse_args()  
    
    # Activate logging
    if options.debug is not None and options.debuglog is None and not options.fg:
        print('Warning! Debugging output will be lost. '
              'You should use either --fg or --debuglog.', file=sys.stderr)
        
    # Foreground logging until we daemonize
    init_logging(True, options.quiet, options.debug, options.debuglog)

    # Check mountpoint
    if not os.path.exists(options.mountpoint):
        print('Mountpoint does not exist.', file=sys.stderr)
        sys.exit(1)
        
    # Read password
    (awskey, awspass) = get_credentials(options.credfile, options.awskey)
    
    # Connect to S3
    conn = s3.Connection(awskey, awspass)
    bucket = conn.get_bucket(options.bucketname)
    
    options.cachedir = options.cachedir.rstrip('/')
    dbfile = get_dbfile(options.bucketname, options.cachedir)
    cachedir = get_cachedir(options.bucketname, options.cachedir)
    
    # Check consistency
    check_fs(bucket, cachedir, dbfile)
   
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
            print("File system damaged, run fsck!", file=sys.stderr)
            sys.exit(1)
    
        # Start server
        bucket.store("s3ql_dirty", "yes")
        operations = run_server(bucket, cachedir, dbcm, options)
        if operations.encountered_errors:
            log.warn('Some errors occured while handling requests. '
                     'Please examine the logs for more information.')
            
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
         
    sys.exit(0 if not operations.encountered_errors else 1)
 
        
def get_fuse_opts(options):
    '''Return fuse options for given command line options'''
    
    fuse_opts = [ b"nonempty", b'fsname=s3ql_local' ]
    #              b'large_read', b'big_writes' ]
    if options.allow_others:
        fuse_opts.append(b'allow_others')
    if options.allow_root:
        fuse_opts.append(b'allow_root')
    if options.allow_others or options.allow_root:
        fuse_opts.append(b'default_permissions')
        
    return fuse_opts
    
def run_server(bucket, cachedir, dbcm, options):
    '''Start FUSE server and run main loop
    
    Returns the used `Operations` instance so that the `encountered_errors`
    attribute can be checked. '''
    
    fuse_opts = get_fuse_opts(options) 
    cache =  S3Cache(bucket, cachedir, options.cachesize, dbcm,
                     timeout=options.s3timeout)
    try:    
        operations = fs.Operations(cache, dbcm, not options.atime)
        llfuse.init(operations, options.mountpoint, fuse_opts)
        
        # Switch to background logging if necessary
        init_logging(options.fg, options.quiet, options.debug, options.debuglog)
        
        # Uncomment this and import cProfile to activate profiling.
        # Note that profiling only works in single threaded mode.
        #cProfile.run('server.main(options.single, options.fg)',
        #             '/home/nikratio/profile_psyco.dat')
        llfuse.main(options.single, options.fg)
        
    finally:
        log.info("Filesystem unmounted, committing cache...")
        cache.clear()
   
    return operations


def add_common_mount_opts(parser):
    '''Add options common to mount and mount_local'''
        
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


def check_fs(bucket, cachedir, dbfile):
    '''Check if file system seems to be consistent
    
    This function writes to stdout/stderr and may call `system.exit()` instead 
    of throwing an exception if it encounters errors.
    '''
     
    log.debug("Checking consistency...")
    if bucket["s3ql_dirty"] != "no":
        print(
            "Metadata is dirty! Either some changes have not yet propagated\n" 
            "through S3 or the file system has not been unmounted cleanly. In\n" 
            "the later case you should run fsck on the system where the\n" 
            "file system has been mounted most recently!", file=sys.stderr)
        sys.exit(1)
    
    # Init cache
    if os.path.exists(cachedir) or os.path.exists(dbfile):
        print(
            "Local cache files already exists! Either you are trying to\n" 
            "to mount a file system that is already mounted, or the filesystem\n" 
            "has not been unmounted cleanly. In the later case you should run\n" 
            "fsck.", file=sys.stderr)
        sys.exit(1)
        
    if (bucket.lookup_key("s3ql_metadata").last_modified 
        < bucket.lookup_key("s3ql_dirty").last_modified):
        print(
            'Metadata from most recent mount has not yet propagated '
            'through Amazon S3. Please try again later.', file=sys.stderr)
        sys.exit(1)
    
     
    
def parse_args():
    '''Parse command line
    
    This function writes to stdout/stderr and may call `system.exit()` instead 
    of throwing an exception if it encounters errors.
    '''
    
    # Not too many branches
    #pylint: disable-msg=R0912
    
    parser = OptionParser(
        usage="%prog  [options] <bucketname> <mountpoint>\n"
              "       %prog --help",
        description="Mounts an amazon S3 bucket as a filesystem.")
    
    add_common_mount_opts(parser)
        
    parser.add_option("--awskey", type="string",
                      help="Amazon Webservices access key to use. If not "
                      "specified, tries to read ~/.awssecret or the file given by --credfile.")
    parser.add_option("--credfile", type="string", default=os.environ["HOME"].rstrip("/")
                       + "/.awssecret",
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
    parser.add_option("--s3timeout", type="int", default=120,
                      help="Maximum time in seconds to wait for propagation in S3 (default: %default)")
    parser.add_option("--cachesize", type="int", default=51200,
                      help="Cache size in kb (default: 51200 (50 MB)). Should be at least 10 times "
                      "the blocksize of the filesystem, otherwise an object may be retrieved and "
                      "written several times during a single write() or read() operation." )
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
    options.bucketname = pps[0]
    options.mountpoint = pps[1]
    
    if not options.o:
        return options
    
    # Parse -o style mount options
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
                
    return options