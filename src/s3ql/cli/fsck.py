'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import os
import stat
import time
from s3ql.common import (get_bucket_home, cycle_metadata, setup_logging,  
                         unlock_bucket, QuietError, get_backend, get_seq_no,
                         restore_metadata, dump_metadata)
from s3ql.parse_args import ArgumentParser
from s3ql import CURRENT_FS_REV
from s3ql.database import Connection
import logging
from s3ql.fsck import Fsck
from s3ql.backends.common import ChecksumError
import sys
import apsw
import tempfile
import cPickle as pickle
import textwrap

log = logging.getLogger("fsck")

def parse_args(args):

    parser = ArgumentParser(
        description="Checks and repairs an S3QL filesystem.")

    parser.add_homedir()
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()
    parser.add_storage_url()

    parser.add_argument("--batch", action="store_true", default=False,
                      help="If user input is required, exit without prompting.")
    parser.add_argument("--force", action="store_true", default=False,
                      help="Force checking even if file system is marked clean.")

    options = parser.parse_args(args)

    if not os.path.exists(options.homedir):
        os.mkdir(options.homedir, 0700)

    return options


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options, 'fsck.log')
        
    with get_backend(options.storage_url, options.homedir) as (conn, bucketname):

        # Check if fs is mounted on this computer
        # This is not foolproof but should prevent common mistakes
        match = options.storage_url + ' /'
        with open('/proc/mounts', 'r') as fh:
            for line in fh:
                if line.startswith(match):
                    raise QuietError('Can not check mounted file system.')

        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)

        try:
            unlock_bucket(options.homedir, options.storage_url, bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        home = get_bucket_home(options.storage_url, options.homedir)
        seq_no = get_seq_no(bucket)
        param_remote = bucket.lookup('s3ql_metadata')
        db = None
        
        if os.path.exists(home + '.params'):
            assert os.path.exists(home + '.db')
            param = pickle.load(open(home + '.params', 'rb'))
            if param['seq_no'] < seq_no:
                log.info('Ignoring locally cached metadata (outdated).')
                param = bucket.lookup('s3ql_metadata')
            else:
                log.info('Using cached metadata.')
                db = Connection(home + '.db')
                assert not os.path.exists(home + '-cache') or param['needs_fsck']
 
            if param_remote['seq_no'] != param['seq_no']:
                log.warn('Remote metadata is outdated.')
                param['needs_fsck'] = True
                
        else:
            param = param_remote
            assert not os.path.exists(home + '-cache')
            # .db might exist if mount.s3ql is killed at exactly the right instant
            # and should just be ignored.
           
        # Check revision
        if param['revision'] < CURRENT_FS_REV:
            raise QuietError('File system revision too old, please run `s3qladm upgrade` first.')
        elif param['revision'] > CURRENT_FS_REV:
            raise QuietError('File system revision too new, please update your '
                             'S3QL installation.')

        if param['seq_no'] < seq_no:
            if (bucket.read_after_write_consistent() and
                bucket.read_after_delete_consistent()):
                print(textwrap.fill(textwrap.dedent('''\
                      Up to date metadata is not available. Probably the file system has not
                      been properly unmounted and you should try to run fsck on the computer 
                      where the file system has been mounted most recently.
                      ''')))
            else:
                print(textwrap.fill(textwrap.dedent('''\
                      Up to date metadata is not available. Either the file system has not
                      been unmounted cleanly or the data has not yet propagated through the backend.
                      In the later case, waiting for a while should fix the problem, in
                      the former case you should try to run fsck on the computer where
                      the file system has been mounted most recently
                      ''')))

            print('Enter "continue" to use the outdated data anyway:',
                  '> ', sep='\n', end='')
            if options.batch:
                raise QuietError('(in batch mode, exiting)')
            if sys.stdin.readline().strip() != 'continue':
                raise QuietError()
            
            param['seq_no'] = seq_no
            param['needs_fsck'] = True


        if (not param['needs_fsck'] 
            and ((time.time() - time.timezone) - param['last_fsck'])
                 < 60 * 60 * 24 * 31): # last check more than 1 month ago
            if options.force:
                log.info('File system seems clean, checking anyway.')
            else:
                log.info('File system is marked as clean. Use --force to force checking.')
                return

        # If using local metadata, check consistency
        if db:
            log.info('Checking DB integrity...')
            try:
                # get_list may raise CorruptError itself
                res = db.get_list('PRAGMA integrity_check(20)')
                if res[0][0] != u'ok':
                    log.error('\n'.join(x[0] for x in res ))
                    raise apsw.CorruptError()
            except apsw.CorruptError:
                raise QuietError('Local metadata is corrupted. Remove or repair the following '
                                 'files manually and re-run fsck:\n'
                                 + home + '.db (corrupted)\n'
                                 + home + '.param (intact)')
        else:
            log.info("Downloading & uncompressing metadata...")
            fh = tempfile.TemporaryFile()
            bucket.fetch_fh("s3ql_metadata", fh)
            os.close(os.open(home + '.db', os.O_RDWR | os.O_CREAT | os.O_TRUNC,
                             stat.S_IRUSR | stat.S_IWUSR)) 
            db = Connection(home + '.db')
            fh.seek(0)
            log.info('Reading metadata...')
            restore_metadata(fh, db)
            fh.close()

        # Increase metadata sequence no 
        param['seq_no'] += 1
        param['needs_fsck'] = True
        bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')
        pickle.dump(param, open(home + '.params', 'wb'), 2)

        fsck = Fsck(home + '-cache', bucket, param, db)
        fsck.check()

        if os.path.exists(home + '-cache'):
            os.rmdir(home + '-cache')
            
        log.info('Saving metadata...')
        fh = tempfile.TemporaryFile()
        dump_metadata(fh, db)  
                
        log.info("Compressing & uploading metadata..")
        cycle_metadata(bucket)
        fh.seek(0)
        param['needs_fsck'] = False
        param['last_fsck'] = time.time() - time.timezone
        param['last-modified'] = time.time() - time.timezone
        bucket.store_fh("s3ql_metadata", fh, param)
        fh.close()
        pickle.dump(param, open(home + '.params', 'wb'), 2)
        
    db.execute('ANALYZE')
    db.execute('VACUUM')
    db.close() 

if __name__ == '__main__':
    main(sys.argv[1:])
