#!/usr/bin/env python3
'''
fix_block_sizes.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import os
import time
import signal
import faulthandler
import sys
import textwrap

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging
from s3ql.mount import get_metadata, dump_and_upload_metadata
from s3ql import BUFSIZE
from s3ql.common import get_seq_no, get_backend_factory, save_params
from s3ql.parse_args import ArgumentParser

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Fixes block sizes corrupted by the fsck.s3ql bug present in
        S3QL versions 3.4.1 to 3.7.2 (inclusive).
        '''))

    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    parser.add_cachedir()
    parser.add_backend_options()
    parser.add_storage_url()

    parser.add_argument("--start-with", default=0, type=int, metavar='<n>',
                      help="Skip over first <n> blocks and start with verifying "
                           "block <n>+1.")

    options = parser.parse_args(args)

    return options

def main(args=None):
    faulthandler.enable()
    faulthandler.register(signal.SIGUSR1)

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    backend_factory = get_backend_factory(options)

    # Retrieve metadata
    with backend_factory() as backend:
        (param, db) = get_metadata(backend, options.cachepath)

        log.info('Scanning blocks...')

        total_count = db.get_val('SELECT COUNT(id) FROM blocks')
        sql = ('SELECT obj_id, blocks.id, blocks.size '
               'FROM blocks LEFT JOIN objects ON obj_id = objects.id '
               'ORDER BY obj_id')
        i = 0 # Make sure this is set if there are zero objects
        rep_cnt = 0
        stamp1 = 0
        try:
            for (i, (obj_id, block_id, block_size)) in enumerate(db.query(sql)):
                if i < options.start_with:
                    continue

                i += 1 # start at 1
                stamp2 = time.time()
                if stamp2 - stamp1 > 1 or i == total_count:
                    stamp1 = stamp2
                    sys.stdout.write(
                        f'\r...scanned {i} objects ({i/total_count:.2%}), repaired {rep_cnt}...')
                    sys.stdout.flush()

                if fix_block(obj_id, block_id, block_size, db, backend):
                    rep_cnt += 1
        finally:
            sys.stdout.write('\n')

        log.info('Verified all %d storage objects.', i)
        seq_no = get_seq_no(backend)
        if rep_cnt == 0:
            del backend['s3ql_seq_no_%d' % param['seq_no']]
            param['seq_no'] -= 1
            save_params(options.cachepath, param)
            return

        elif seq_no == param['seq_no']:
            param['last-modified'] = time.time()
            dump_and_upload_metadata(backend, db, param)
            save_params(options.cachepath, param)
        else:
            log.error('Remote metadata is newer than local (%d vs %d), '
                      'refusing to overwrite!', seq_no, param['seq_no'])
            log.error('The locally cached metadata will be *lost* the next time the file system '
                      'is mounted or checked and has therefore been backed up.')
            for name in (options.cachepath + '.params', options.cachepath + '.db'):
                for i in range(4)[::-1]:
                    if os.path.exists(name + '.%d' % i):
                        os.rename(name + '.%d' % i, name + '.%d' % (i + 1))
                os.rename(name, name + '.0')



def fix_block(obj_id, block_id, cur_size, db, backend):
    '''Fix block size in metadata table if needed.

     From commit 06ac78a4 (2020-03-22) to commit 5e7ac93ba3, fsck.s3ql would accidentally
     round up the size of any uploaded dirty blocks to the next multiple of 512. This will
     then cause later fsck.s3ql run to (correctly) complain that the file size is smaller
     than the data stored in the file blocks. To fix this retroactively, we need to detect
     when this happens and correct the block size instead of the file size.
    '''

    if cur_size % 512 != 0:
        return False

    act_size = None
    def do_read(fh):
        nonlocal act_size
        act_size = 0
        while True:
            buf = fh.read(BUFSIZE)
            act_size += len(buf)
            if not buf:
                break

    log.debug('reading object %s', obj_id)
    key = 's3ql_data_%d' % obj_id
    backend.perform_read(do_read, key)

    if act_size == cur_size:
        return False

    log.debug('Adjusting size of block %d from %d to %d',
              block_id, cur_size, act_size)

    db.execute('UPDATE blocks SET size=? WHERE id=?', (act_size, block_id))
    return True


if __name__ == '__main__':
    main(sys.argv[1:])
