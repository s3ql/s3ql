'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import os
import stat
import time
from optparse import OptionParser
from ..common import (init_logging_from_options, get_cachedir, get_dbfile, cycle_metadata,
                      unlock_bucket, QuietError, get_parameters, get_backend)
from ..database import ConnectionManager
import logging
from .. import fsck
from .. import backends
from ..backends.common import ChecksumError
import sys
import shutil
import cPickle as pickle
import textwrap

log = logging.getLogger("fsck")

def parse_args(args):

    parser = OptionParser(
        usage="%prog  [options] <storage-url>\n"
        "%prog --help",
        description="Checks and repairs an S3QL filesystem.")

    parser.add_option("--homedir", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql"),
                      help='Directory for log files, cache and authentication info.'
                      'Default: ~/.s3ql')
    parser.add_option("--debug", action="append",
                      help="Activate debugging output from specified module. Use 'all' "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")

    (options, pps) = parser.parse_args(args)

    if len(pps) != 1:
        parser.error("Incorrect number of arguments.")

    options.storage_url = pps[0]

    return options


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    try:
        import psyco
        psyco.profile()
    except ImportError:
        pass

    options = parse_args(args)
    init_logging_from_options(options, 'fsck.log')

    with get_backend(options) as (conn, bucketname):
        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)

        try:
            unlock_bucket(bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        dbfile = get_dbfile(options.storage_url, options.homedir)
        cachedir = get_cachedir(options.storage_url, options.homedir)

        # Get most-recent s3ql_parameters object and check fs revision
        param = get_parameters(bucket)

        if os.path.exists(dbfile):
            dbcm = ConnectionManager(dbfile)
            mountcnt_db = dbcm.get_val('SELECT mountcnt FROM parameters')
            if mountcnt_db < param['mountcnt']:
                choice = None
                print('Local cache files exist, but file system appears to have \n'
                      'been mounted elsewhere after the unclean shutdown.\n'
                      'You can either:\n'
                      '  a) Remove the local cache files and loose the changes in there\n'
                      '  b) Use the local cache and loose the changes performed during\n'
                      '     all mounts after the unclean shutdown.\n')
                while choice not in ('a', 'b'):
                    print('Your choice [ab]? ', end='')
                    choice = sys.stdin.readline().strip().lower()

                if choice == 'a':
                    log.info('Removing local cache files..')
                    os.unlink(dbfile)
                    shutil.rmtree(cachedir)
                    do_download = True
                else:
                    log.info('Using local cache files.')
                    do_download = False

            elif mountcnt_db > param['mountcnt']:
                raise RuntimeError('mountcnt_db > param[mountcnt], this should not happen.')
            else:
                do_download = False
        else:
            if os.path.exists(cachedir):
                raise RuntimeError('cachedir exists, but no local metadata.'
                                   'This should not happen.')
            do_download = True

        if do_download:
            log.info("Downloading metadata...")
            os.mknod(dbfile, stat.S_IRUSR | stat.S_IWUSR | stat.S_IFREG)
            bucket.fetch_fh("s3ql_metadata", open(dbfile, 'wb'))
            dbcm = ConnectionManager(dbfile)
            mountcnt_db = dbcm.get_val('SELECT mountcnt FROM parameters')

            if mountcnt_db < param['mountcnt']:
                if isinstance(bucket, backends.s3.Bucket):
                    print(textwrap.fill(textwrap.dedent('''
                          Up to date metadata is not available. Either the file system has not
                          been unmounted cleanly or the data has not yet propagated through S3.
                          In the later case, waiting for a while should fix the problem, in
                          the former case you should try to run fsck on the computer where
                          the file system has been mounted most recently
                          ''')))
                else:
                    print(textwrap.fill(textwrap.dedent('''
                          Up to date metadata is not available. Probably the file system has not
                          been unmounted and you should try to run fsck on the computer where
                          the file system has been mounted most recently.
                          ''')))
                print('Enter "continue" to use the outdated data anyway:',
                      '> ', sep='\n', end='')
                if sys.stdin.readline().strip() != 'continue':
                    os.unlink(dbfile)
                    raise QuietError(1)

            elif mountcnt_db > param['mountcnt']:
                os.unlink(dbfile)
                raise RuntimeError('mountcnt_db > param[mountcnt], this should not happen.')

        mountcnt = max(mountcnt_db, param['mountcnt']) + 1
        param['mountcnt'] = mountcnt
        dbcm.execute('UPDATE parameters SET mountcnt=?', (mountcnt,))
        bucket.store('s3ql_parameters_%d' % param['mountcnt'],
                     pickle.dumps(param, 2))

        if not os.path.exists(cachedir):
            os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        fsck.fsck(dbcm, cachedir, bucket)

        log.info("Committing data to backend...")
        dbcm.execute("UPDATE parameters SET needs_fsck=?, last_fsck=?",
                     (False, time.time() - time.timezone))

        dbcm.execute("VACUUM")
        log.debug("Uploading database..")
        cycle_metadata(bucket)
        bucket.store_fh("s3ql_metadata", open(dbfile, 'r'))

        os.unlink(dbfile)
        os.rmdir(cachedir)

if __name__ == '__main__':
    main(sys.argv[1:])
