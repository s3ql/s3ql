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
from s3ql.common import (init_logging_from_options, get_bucket_home, cycle_metadata,
                      unlock_bucket, QuietError, CURRENT_FS_REV, get_backend, dump_metadata,
                      restore_metadata)
import s3ql.database as dbcm
import logging
from s3ql import fsck
from s3ql import backends
from s3ql.backends.common import ChecksumError
import sys
import shutil
import tempfile
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
                      help='Directory for log files, cache and authentication info. '
                      'Default: ~/.s3ql')
    parser.add_option("--debug", action="append",
                      help="Activate debugging output from specified module. Use 'all' "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("--batch", action="store_true", default=False,
                      help="If user input is required, exit without prompting.")
    parser.add_option("--force", action="store_true", default=False,
                      help="Force checking even if file system is marked clean.")
    

    (options, pps) = parser.parse_args(args)

    if len(pps) != 1:
        parser.error("Incorrect number of arguments.")

    options.storage_url = pps[0]

    return options


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    init_logging_from_options(options, 'fsck.log')

    with get_backend(options) as (conn, bucketname):
        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)

        try:
            unlock_bucket(options, bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        home = get_bucket_home(options.storage_url, options.homedir)

        # Get file system parameters
        log.info('Getting file system parameters..')
        seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
        if not seq_nos:
            raise QuietError('Old file system revision, please run tune.s3ql --upgrade first.')
        seq_no = max(seq_nos)

        param = None
        metadata_loaded = False
        if os.path.exists(home + '.params'):
            param = pickle.load(open(home + '.params', 'rb'))
            param['needs_fsck'] = True
            if param['seq_no'] < seq_no:
                choice = None
                print('Local cache files exist, but file system appears to have \n'
                      'been mounted elsewhere after the unclean shutdown.\n'
                      'You can either:\n'
                      '  a) Remove the local cache files and loose the changes in there\n'
                      '  b) Use the local cache and loose the changes performed during\n'
                      '     all mounts after the unclean shutdown.\n')
                if options.batch:
                    raise QuietError('(in batch mode, exiting)')
                while choice not in ('a', 'b'):
                    print('Your choice [ab]? ', end='')
                    choice = sys.stdin.readline().strip().lower()

                if choice == 'a':
                    log.info('Removing local cache files..')
                    os.unlink(home + '.db')
                    os.unlink(home + '.params')
                    shutil.rmtree(home + '-cache')
                    param =  None
                else:
                    log.info('Using local cache files.')
                    param['seq_no'] = seq_no
                    dbcm.init(home + '.db')
                    metadata_loaded = True

            elif param['seq_no'] > seq_no:
                raise RuntimeError('param[seq_no] > seq_no, this should not happen.')
            else:
                log.info('Using locally cached metadata.')
                dbcm.init(home + '.db')
                metadata_loaded = True
        else:
            if os.path.exists(home + '-cache') or os.path.exists(home + '.db'):
                raise RuntimeError('cachedir exists, but no local metadata.'
                                   'This should not happen.')

        if param is None:
            param = bucket.lookup('s3ql_metadata')
             
        # Check revision
        if param['revision'] < CURRENT_FS_REV:
            raise QuietError('File system revision too old, please run tune.s3ql --upgrade first.')
        elif param['revision'] > CURRENT_FS_REV:
            raise QuietError('File system revision too new, please update your '
                             'S3QL installation.')
            
        if param['seq_no'] < seq_no:
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
            if options.batch:
                raise QuietError('(in batch mode, exiting)')
            if sys.stdin.readline().strip() != 'continue':
                raise QuietError(1)
            param['seq_no'] = seq_no
            
        elif param['seq_no'] > seq_no:
            raise RuntimeError('param[seq_no] > seq_no, this should not happen.')
        elif not param['needs_fsck']:
            if options.force:
                log.info('File system seems clean, checking anyway.')
            else:
                log.info('File system is marked as clean. Use --force to force checking.')
                return

        # Download metadata
        if not metadata_loaded:
            log.info("Downloading & uncompressing metadata...")
            fh = os.fdopen(os.open(home + '.db', os.O_RDWR | os.O_CREAT,
                                  stat.S_IRUSR | stat.S_IWUSR), 'w+b')
            fh.close()
            dbcm.init(home + '.db')
            fh = tempfile.TemporaryFile()
            bucket.fetch_fh("s3ql_metadata", fh)
            fh.seek(0)
            log.info('Reading metadata...')
            restore_metadata(fh)
            fh.close()
    
        # Increase metadata sequence no
        param['seq_no'] += 1
        bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')
        for i in seq_nos:
            if i < param['seq_no'] - 5:
                del bucket['s3ql_seq_no_%d' % i ]
                
        # Save parameters
        pickle.dump(param, open(home + '.params', 'wb'), 2)

        if not os.path.exists(home + '-cache'):
            os.mkdir(home + '-cache', stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        fsck.fsck(home + '-cache', bucket, param)

        log.info("Saving metadata...")
        fh = tempfile.TemporaryFile()
        dump_metadata(fh)
        fh.seek(0)
        log.info("Compressing & uploading metadata..")
        cycle_metadata(bucket)
        param['needs_fsck'] = False
        param['last_fsck'] = time.time() - time.timezone
        bucket.store_fh("s3ql_metadata", fh, param)
        fh.close()

        # Remove database
        log.debug("Cleaning up...")
        os.unlink(home + '.db')
        os.unlink(home + '.params')
        os.rmdir(home + '-cache')

if __name__ == '__main__':
    main(sys.argv[1:])
