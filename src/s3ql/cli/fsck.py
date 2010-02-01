'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import os
import stat
import time
from optparse import OptionParser
from s3ql.common import (init_logging_from_options, get_credentials, get_cachedir, get_dbfile,
                         unlock_bucket, QuietError)
from s3ql.database import ConnectionManager
import logging
from s3ql import s3, fsck
import sys
import shutil
import cPickle as pickle
import apsw

log = logging.getLogger("fsck")

def parse_args(args):

    parser = OptionParser(
        usage="%prog  [options] <bucketname>\n"
        "%prog --help",
        description="Checks and repairs an s3ql filesystem.")

    parser.add_option("--awskey", type="string",
                      help="Amazon Webservices access key to use. If not "
                      "specified, tries to read ~/.awssecret or the file given by --credfile.")
    parser.add_option("--logfile", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql", 'fsck.log'),
                      help="Write log messages in this file. Default: ~/.s3ql/fsck.log")
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
    parser.add_option("--debug", action="append",
                      help="Activate debugging output from specified module. Use 'all' "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")

    (options, pps) = parser.parse_args(args)

    if not len(pps) == 1:
        parser.error("bucketname not specificed")

    options.bucketname = pps[0]

    return options


def main(args):

    try:
        import psyco
        psyco.profile()
    except ImportError:
        pass

    options = parse_args(args)
    init_logging_from_options(options)

    dbfile = get_dbfile(options.bucketname, options.cachedir)
    cachedir = get_cachedir(options.bucketname, options.cachedir)

    (awskey, awspass) = get_credentials(options.credfile, options.awskey)
    conn = s3.Connection(awskey, awspass)
    if not conn.bucket_exists(options.bucketname):
        log.error("Bucket does not exist.")
        sys.exit(1)
    bucket = conn.get_bucket(options.bucketname)

    try:
        unlock_bucket(bucket)
    except s3.ChecksumError:
        raise QuietError('Checksum error - incorrect password?')


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
        mountcnt_db = dbcm.get_val('SELECT mountcnt FROM parameters')
        if mountcnt_db < param['mountcnt']:
            choice = None
            print('Local cache files exist, but file system appears to have \n'
                  'been mounted elsewhere after the unclean shutdown.\n'
                  'You can either:\n'
                  '  a) Remove the local cache files and loose the changes in there\n'
                  '  b) Use the local cache and loose the changes performed during\n'
                  '     all mounts after the unclean shutdown.\n')
            while choice in ('a', 'b'):
                print('Your choice [ab]? ')
                choice = sys.stdin.readline().strip().tolower()

            if choice == 'a':
                log.info('Removing local cache files..')
                os.unlink(dbfile)
                shutil.rmtree(cachedir)
                log.info("Downloading metadata...")
                os.mknod(dbfile, stat.S_IRUSR | stat.S_IWUSR | stat.S_IFREG)
                bucket.fetch_fh("s3ql_metadata", open(dbfile, 'w'))
                dbcm = ConnectionManager(dbfile,
                                         initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
                mountcnt_db = dbcm.get_val('SELECT mountcnt FROM parameters')
            else:
                log.info('Using local cache files.')

        elif mountcnt_db > param['mountcnt']:
            raise RuntimeError('mountcnt_db > mountcnt_s3, this should not happen.')
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
        print('Metadata from most recent mount has not yet propagated through S3, or\n'
              'file system has not been unmounted cleanly. In the later case you\n'
              'should run fsck.s3l on the computer where the bucket has been\n'
              'mounted most-recently.')
        print('Enter "continue" to use the outdated data anyway:')
        if sys.stdin.readline().strip() != 'continue':
            os.unlink(dbfile)
            raise QuietError(1)

    elif mountcnt_db > param['mountcnt']:
        os.unlink(dbfile)
        raise RuntimeError('mountcnt_db > mountcnt_s3, this should not happen.')

    mountcnt = max(mountcnt_db, param['mountcnt']) + 1
    param['mountcnt'] = mountcnt
    bucket.store('s3ql_parameters', pickle.dumps(param, 2))

    if not os.path.exists(cachedir):
        os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    with dbcm.transaction() as conn:
        fsck.fsck(conn, cachedir, bucket)

    log.info("Committing data to S3...")
    conn.execute("UPDATE parameters SET needs_fsck=?, last_fsck=?, "
                 "mountcnt=?", (False, time.time() - time.timezone, mountcnt))

    conn.execute("VACUUM")
    log.debug("Uploading database..")
    if bucket.has_key("s3ql_metadata_bak_2"):
        bucket.copy("s3ql_metadata_bak_2", "s3ql_metadata_bak_3")
    if bucket.has_key("s3ql_metadata_bak_1"):
        bucket.copy("s3ql_metadata_bak_1", "s3ql_metadata_bak_2")
    bucket.copy("s3ql_metadata", "s3ql_metadata_bak_1")

    bucket.store_fh("s3ql_metadata", open(dbfile, 'r'))

    os.unlink(dbfile)
    os.rmdir(cachedir)

if __name__ == '__main__':
    main(sys.argv[1:])
