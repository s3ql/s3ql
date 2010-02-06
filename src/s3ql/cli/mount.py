'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import sys
from optparse import OptionParser
from s3ql import fs, s3
from s3ql.s3cache import SynchronizedS3Cache
from s3ql.common import (init_logging_from_options, get_credentials, get_cachedir, get_dbfile,
                         QuietError, unlock_bucket)
from s3ql.database import ConnectionManager
import llfuse
import tempfile
import os
import stat
import logging
import cPickle as pickle

__all__ = [ 'main', 'add_common_mount_opts', 'run_server' ]

log = logging.getLogger("mount")

# TODO: Evaluate if we can save the db in plain text, that seems to
# make it much smaller and easier to compress.

def main(args):
    '''Mount S3QL file system'''

    try:
        import psyco
        psyco.profile()
    except ImportError:
        pass

    options = parse_args(args)
    init_logging_from_options(options)

    if not os.path.exists(options.mountpoint):
        raise QuietError('Mountpoint does not exist.')

    (awskey, awspass) = get_credentials(options.credfile, options.awskey)
    conn = s3.Connection(awskey, awspass)
    bucket = conn.get_bucket(options.bucketname)

    try:
        unlock_bucket(bucket)
    except s3.ChecksumError:
        raise QuietError('Checksum error - incorrect password?')

    options.cachedir = options.cachedir
    dbfile = get_dbfile(options.bucketname, options.cachedir)
    cachedir = get_cachedir(options.bucketname, options.cachedir)

    if 's3ql_parameters' not in bucket:
        raise QuietError('Old file system revision, please run tune.s3ql --upgrade first.')

    param = pickle.loads(bucket['s3ql_parameters'])

    if param['revision'] < 2:
        raise QuietError('File system revision too old, please run tune.s3ql --upgrade first.')
    elif param['revision'] > 2:
        raise QuietError('File system revision too new, please update your '
                         'S3QL installation.')

    if os.path.exists(dbfile):
        dbcm = ConnectionManager(dbfile, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
        mountcnt = dbcm.get_val('SELECT mountcnt FROM parameters')
        if mountcnt != param['mountcnt']:
            raise QuietError('Local cache files exist, but file system appears to have \n'
                             'been mounted elsewhere after the unclean shutdown.')

        log.info('Recovering old metadata from unclean shutdown..')
        # TODO: We should run multithreaded at some point
        cache = SynchronizedS3Cache(bucket, cachedir, int(options.cachesize * 1024), dbcm,
                                    timeout=options.s3timeout)
        cache.recover()
    else:
        if os.path.exists(cachedir):
            raise RuntimeError('cachedir exists, but no local metadata.'
                               'This should not happen.')

        log.info("Downloading metadata...")
        os.mknod(dbfile, stat.S_IRUSR | stat.S_IWUSR | stat.S_IFREG)
        bucket.fetch_fh("s3ql_metadata", open(dbfile, 'w'))
        dbcm = ConnectionManager(dbfile, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')

        mountcnt_db = dbcm.get_val('SELECT mountcnt FROM parameters')
        if mountcnt_db < param['mountcnt']:
            os.unlink(dbfile)
            raise QuietError('Metadata from most recent mount has not yet propagated through S3, or\n'
                             'file system has not been unmounted cleanly. In the later case you\n'
                             'should run fsck.s3l on the computer where the bucket has been\n'
                             'mounted most-recently.')
        elif mountcnt_db > param['mountcnt']:
            os.unlink(dbfile)
            raise RuntimeError('mountcnt_db > mountcnt_s3, this should not happen.')

        os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        # TODO: We should run multithreaded at some point
        cache = SynchronizedS3Cache(bucket, cachedir, int(options.cachesize * 1024), dbcm,
                                    timeout=options.s3timeout)

    # Check that the fs itself is clean
    if dbcm.get_val("SELECT needs_fsck FROM parameters"):
        raise QuietError("File system damaged, run fsck!")

    # Start server
    log.info('Analyzing metadata...')
    dbcm.execute('ANALYZE')

    param['mountcnt'] += 1
    bucket.store_wait('s3ql_parameters', pickle.dumps(param, 2))
    dbcm.execute('UPDATE parameters SET mountcnt=mountcnt+1')
    try:
        try:
            operations = run_server(bucket, cache, dbcm, options)
        finally:
            log.info('Clearing cache...')
            cache.clear()
    finally:
        log.info("Uploading database..")
        dbcm.execute("VACUUM")
        if bucket.has_key("s3ql_metadata_bak_2"):
            bucket.copy("s3ql_metadata_bak_2", "s3ql_metadata_bak_3")
        if bucket.has_key("s3ql_metadata_bak_1"):
            bucket.copy("s3ql_metadata_bak_1", "s3ql_metadata_bak_2")
        bucket.copy("s3ql_metadata", "s3ql_metadata_bak_1")
        bucket.store_fh("s3ql_metadata", open(dbfile, 'r'))

    # Remove database
    log.debug("Cleaning up...")
    os.unlink(dbfile)
    os.rmdir(cachedir)

    if operations.encountered_errors:
        raise QuietError('Some errors were encountered while the file system was mounted.\n'
                         'Please examine the log files for more information.')


def get_fuse_opts(options):
    '''Return fuse options for given command line options'''

    fuse_opts = [ b"nonempty", b'fsname=%s' % options.bucketname,
                  'subtype=s3ql' ]

    if options.allow_others:
        fuse_opts.append(b'allow_others')
    if options.allow_root:
        fuse_opts.append(b'allow_root')
    if options.allow_others or options.allow_root:
        fuse_opts.append(b'default_permissions')

    return fuse_opts

def run_server(bucket, cache, dbcm, options):
    '''Start FUSE server and run main loop
    
    Returns the used `Operations` instance so that the `encountered_errors`
    attribute can be checked.
    '''

    if options.profile:
        import cProfile
        import pstats
        prof = cProfile.Profile()

    log.info('Mounting filesystem...')
    fuse_opts = get_fuse_opts(options)

    operations = fs.Operations(cache, dbcm, not options.atime)
    llfuse.init(operations, options.mountpoint, fuse_opts)
    try:
        # Switch to background logging if necessary
        init_logging_from_options(options, daemon=not options.fg)

        if options.profile:
            prof.runcall(llfuse.main, options.single, options.fg)
        else:
            llfuse.main(options.single, options.fg)

    except:
        llfuse.close()
        raise

    if options.profile:
        tmp = tempfile.NamedTemporaryFile()
        prof.dump_stats(tmp.name)
        fh = open('s3ql_profile.txt', 'w')
        p = pstats.Stats(tmp.name, stream=fh)
        tmp.close()
        p.strip_dirs()
        p.sort_stats('cumulative')
        p.print_stats(50)
        p.sort_stats('time')
        p.print_stats(50)
        fh.close()

    return operations


def add_common_mount_opts(parser):
    '''Add options common to mount and mount_local'''

    parser.add_option("--logfile", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql", 'mount.log'),
                      help="Write log messages in this file. Default: ~/.s3ql/mount.log")
    parser.add_option("--debug", action="append",
                      help="Activate debugging output from specified module. Use 'all' "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
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
                      help="Run in single threaded mode. If you don't understand this, "
                           "then you don't need it.")
    parser.add_option("--atime", action="store_true", default=False,
                      help="Update directory access time. Will decrease performance.")
    parser.add_option("--profile", action="store_true", default=False,
                      help="Create profiling information. If you don't understand this, "
                           "then you don't need it.")



def parse_args(args):
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
    parser.add_option("--credfile", type="string",
                      default=os.path.join(os.environ["HOME"], ".awssecret"),
                      help='Try to read AWS access key and key id from this file. '
                      'The file must be readable only be the owner and should contain '
                      'the key id and the secret key separated by a newline. '
                      'Default: ~/.awssecret')
    parser.add_option("--cachedir", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql"),
                      help="Specifies the directory for cache files. Different S3QL file systems "
                      '(i.e. located in different S3 buckets) can share a cache location, even if '
                      'they are mounted at the same time. '
                      'You should try to always use the same location here, so that S3QL can detect '
                      'and, as far as possible, recover from unclean umounts. Default is ~/.s3ql.')
    parser.add_option("--s3timeout", type="int", default=120,
                      help="Maximum time in seconds to wait for propagation in S3 (default: %default)")
    parser.add_option("--cachesize", type="int", default=102400,
                      help="Cache size in kb (default: 102400 (100 MB)). Should be at least 10 times "
                      "the blocksize of the filesystem, otherwise an object may be retrieved and "
                      "written several times during a single write() or read() operation.")


    (options, pps) = parser.parse_args(args)

    #
    # Verify parameters
    #
    if not len(pps) == 2:
        parser.error("Wrong number of parameters")
    options.bucketname = pps[0]
    options.mountpoint = pps[1]

    if options.profile:
        options.single = True

    return options

if __name__ == '__main__':
    main(sys.argv[1:])
