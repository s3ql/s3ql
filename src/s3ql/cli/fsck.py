'''
fsck.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from s3ql import CURRENT_FS_REV
from s3ql.backends.common import get_bucket
from s3ql.common import (get_bucket_cachedir, cycle_metadata, setup_logging, 
    QuietError, get_seq_no, restore_metadata, dump_metadata, CTRL_INODE)
from s3ql.database import Connection
from s3ql.fsck import Fsck
from s3ql.parse_args import ArgumentParser
import apsw
import cPickle as pickle
import logging
import os
import stat
import sys
import textwrap
import time

log = logging.getLogger("fsck")

def parse_args(args):

    parser = ArgumentParser(
        description="Checks and repairs an S3QL filesystem.")

    parser.add_log('~/.s3ql/fsck.log')
    parser.add_cachedir()
    parser.add_authfile()
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()
    parser.add_storage_url()

    parser.add_argument("--batch", action="store_true", default=False,
                      help="If user input is required, exit without prompting.")
    parser.add_argument("--force", action="store_true", default=False,
                      help="Force checking even if file system is marked clean.")
    parser.add_argument("--renumber-inodes", action="store_true", default=False,
                      help="Renumber inodes to be stricly sequential starting from %d"
                           % (CTRL_INODE+1))
    options = parser.parse_args(args)

    return options

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)
        
    # Check if fs is mounted on this computer
    # This is not foolproof but should prevent common mistakes
    match = options.storage_url + ' /'
    with open('/proc/mounts', 'r') as fh:
        for line in fh:
            if line.startswith(match):
                raise QuietError('Can not check mounted file system.')
    
    bucket = get_bucket(options)
    
    cachepath = get_bucket_cachedir(options.storage_url, options.cachedir)
    seq_no = get_seq_no(bucket)
    param_remote = bucket.lookup('s3ql_metadata')
    db = None
    
    if os.path.exists(cachepath + '.params'):
        assert os.path.exists(cachepath + '.db')
        param = pickle.load(open(cachepath + '.params', 'rb'))
        if param['seq_no'] < seq_no:
            log.info('Ignoring locally cached metadata (outdated).')
            param = bucket.lookup('s3ql_metadata')
        else:
            log.info('Using cached metadata.')
            db = Connection(cachepath + '.db')
            assert not os.path.exists(cachepath + '-cache') or param['needs_fsck']
    
        if param_remote['seq_no'] != param['seq_no']:
            log.warn('Remote metadata is outdated.')
            param['needs_fsck'] = True
            
    else:
        param = param_remote
        assert not os.path.exists(cachepath + '-cache')
        # .db might exist if mount.s3ql is killed at exactly the right instant
        # and should just be ignored.
       
    # Check revision
    if param['revision'] < CURRENT_FS_REV:
        raise QuietError('File system revision too old, please run `s3qladm upgrade` first.')
    elif param['revision'] > CURRENT_FS_REV:
        raise QuietError('File system revision too new, please update your '
                         'S3QL installation.')
    
    if param['seq_no'] < seq_no:
        if bucket.is_get_consistent():
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
        if options.force or options.renumber_inodes:
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
                             + cachepath + '.db (corrupted)\n'
                             + cachepath + '.param (intact)')
    else:
        def do_read(fh):
            os.close(os.open(cachepath + '.db.tmp', os.O_RDWR | os.O_CREAT | os.O_TRUNC,
                             stat.S_IRUSR | stat.S_IWUSR)) 
            db = Connection(cachepath + '.db.tmp', fast_mode=True)
            try:
                restore_metadata(fh, db)
            finally:
                # If metata reading has to be retried, we don't want to hold
                # a lock on the database.
                db.close()
        db = bucket.perform_read(do_read, "s3ql_metadata") 
        os.rename(cachepath + '.db.tmp', cachepath + '.db')
        db = Connection(cachepath + '.db')
    
    # Increase metadata sequence no 
    param['seq_no'] += 1
    param['needs_fsck'] = True
    bucket['s3ql_seq_no_%d' % param['seq_no']] = 'Empty'
    pickle.dump(param, open(cachepath + '.params', 'wb'), 2)
    
    fsck = Fsck(cachepath + '-cache', bucket, param, db)
    fsck.check()
    
    if fsck.uncorrectable_errors:
        raise QuietError("Uncorrectable errors found, aborting.")
        
    if os.path.exists(cachepath + '-cache'):
        os.rmdir(cachepath + '-cache')
        
    if options.renumber_inodes:
        renumber_inodes(db)
            
    cycle_metadata(bucket)
    param['needs_fsck'] = False
    param['last_fsck'] = time.time() - time.timezone
    param['last-modified'] = time.time() - time.timezone
    bucket.perform_write(lambda fh: dump_metadata(fh, db) , "s3ql_metadata",
                         metadata=param, is_compressed=True) 
    pickle.dump(param, open(cachepath + '.params', 'wb'), 2)
        
    db.execute('ANALYZE')
    db.execute('VACUUM')
    db.close() 

    if options.renumber_inodes:
        print('',
              'Inodes were renumbered. If this file system has been exported over NFS,',
              'all NFS clients need to be restarted before mounting the S3QL file system ',
              'again or data corruption may occur.', sep='\n')
        

def renumber_inodes(db):
    '''Renumber inodes'''
    
    log.info('Renumbering inodes...')
    total = db.get_val('SELECT COUNT(id) FROM inodes')
    db.execute('CREATE TEMPORARY TABLE inode_ids AS '
               'SELECT id FROM inodes WHERE id > ? ORDER BY id DESC', 
               (max(total, CTRL_INODE),))
    db.execute('CREATE INDEX IF NOT EXISTS ix_contents_inode ON contents(inode)')
    try:
        i = 0
        cur = CTRL_INODE+1
        for (id_,) in db.query("SELECT id FROM inode_ids"):
            while True:
                try:
                    db.execute('UPDATE inodes SET id=? WHERE id=?', (cur, id_))
                except apsw.ConstraintError:
                    cur += 1
                else:
                    break            
            
            db.execute('UPDATE contents SET inode=? WHERE inode=?', (cur, id_))
            db.execute('UPDATE contents SET parent_inode=? WHERE parent_inode=?', (cur, id_))
            db.execute('UPDATE inode_blocks SET inode=? WHERE inode=?', (cur, id_))
            db.execute('UPDATE symlink_targets SET inode=? WHERE inode=?', (cur, id_))
            db.execute('UPDATE ext_attributes SET inode=? WHERE inode=?', (cur, id_))
                                    
            cur += 1
            i += 1
            if i % 5000 == 0:
                log.info('..processed %d inodes so far..', i)
        
    finally:
        db.execute('DROP TABLE inode_ids')
        db.execute('DROP INDEX ix_contents_inode')

        
if __name__ == '__main__':
    main(sys.argv[1:])

