'''
upload_manager.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .backends.common import NoSuchObject
from .common import sha256_fh, without, TimeoutError
from .thread_group import ThreadGroup
from . import database as dbcm
from .database import NoSuchRowError
import logging
import os
import threading
import time

__all__ = [ "UploadManager", 'retry_exc' ]

# standard logger for this module
log = logging.getLogger("UploadManager")
     

MAX_UPLOAD_THREADS = 10
MIN_TRANSIT_SIZE = 1024 * 1024
class UploadManager(object):
    '''
    Schedules and executes object uploads to make optimum usage
    network bandwidth and CPU time.
    '''
    
    def __init__(self, bucket):
        self.threads = ThreadGroup(MAX_UPLOAD_THREADS)
        self.bucket = bucket
        self.transit_size = 0
        self.transit_size_lock = threading.Lock()
        
    def add(self, el, lock):
        '''Upload cache entry `el` asynchronously
        
        Return size of cache entry.
        
        If the maximum number of upload threads has been reached,
        `lock` is released while the method waits for a free slot. 
        '''
        
        log.debug('UploadManager.add(%s): start', el)

        size = os.fstat(el.fileno()).st_size
        el.seek(0)
        hash_ = sha256_fh(el)
        old_obj_id = el.obj_id
        with dbcm.write_lock() as conn:
            try:
                el.obj_id = conn.get_val('SELECT id FROM objects WHERE hash=?', (hash_,))

            except NoSuchRowError:
                need_upload = True
                el.obj_id = conn.rowid('INSERT INTO objects (refcount, hash, size) VALUES(?, ?, ?)',
                                      (1, hash_, size))
                log.debug('add(inode=%d, blockno=%d): created new object %d',
                          el.inode, el.blockno, el.obj_id)

            else:
                need_upload = False
                log.debug('add(inode=%d, blockno=%d): (re)linking to %d',
                          el.inode, el.blockno, el.obj_id)
                conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?',
                             (el.obj_id,))
                
            to_delete = False
            if old_obj_id is None:
                log.debug('add(inode=%d, blockno=%d): no previous object',
                          el.inode, el.blockno)
                conn.execute('INSERT INTO blocks (obj_id, inode, blockno) VALUES(?,?,?)',
                             (el.obj_id, el.inode, el.blockno))    
            else:
                conn.execute('UPDATE blocks SET obj_id=? WHERE inode=? AND blockno=?',
                             (el.obj_id, el.inode, el.blockno))
                refcount = conn.get_val('SELECT refcount FROM objects WHERE id=?',
                                        (old_obj_id,))
                if refcount > 1:
                    log.debug('add(inode=%d, blockno=%d): '
                              'decreased refcount for prev. obj: %d',
                              el.inode, el.blockno, old_obj_id)
                    conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                                 (old_obj_id,))
                else:
                    log.debug('add(inode=%d, blockno=%d): '
                              'prev. obj %d marked for removal',
                              el.inode, el.blockno, old_obj_id)
                    conn.execute('DELETE FROM objects WHERE id=?', (old_obj_id,))
                    to_delete = True


        el.dirty = False
        os.rename(el.name + '.d', el.name)
        
        if need_upload or to_delete:
            if need_upload:
                with self.transit_size_lock:
                    self.transit_size += size
                fn = self.bucket.prep_store_fh('s3ql_data_%d' % el.obj_id, el)
                def doit():
                    t = time.time()
                    fn()
                    t = time.time() - t
                    log.debug('add(inode=%d, blockno=%d): '
                             'transferred %d bytes in %.3f seconds, %.2f MB/s',
                              el.inode, el.blockno, size, t, size / (1024**2 * t))
                    with self.transit_size_lock:
                        self.transit_size -= size
                    if to_delete:
                        retry_exc(300, [ NoSuchObject ], self.bucket.delete,
                                  's3ql_data_%d' % old_obj_id)

            elif to_delete:
                doit = lambda : retry_exc(300, [ NoSuchObject ], self.bucket.delete,
                                          's3ql_data_%d' % old_obj_id)
                
                
            # If we already have the minimum transit size, do not start more
            # than two threads
            log.debug('add(%s): starting upload thread', el)
            if self.transit_size > MIN_TRANSIT_SIZE:
                with without(lock):
                    self.threads.add(doit, max_threads=2)
            else:
                with without(lock):
                    self.threads.add(doit)

        log.debug('add(%s): end', el)
        return size

    def join_all(self):
        self.threads.join_all()

    def join_one(self):
        self.threads.join_one()
                
    def upload_in_progress(self):
        return len(self.threads) > 0


def retry_exc(timeout, exc_types, fn, *a, **kw):
    """Wait for fn(*a, **kw) to succeed
    
    If `fn(*a, **kw)` raises an exception in `exc_types`, the function is called again.
    If the timeout is reached, `TimeoutError` is raised.
    """

    step = 0.2
    waited = 0
    while waited < timeout:
        try:
            return fn(*a, **kw)
        except BaseException as exc:
            for exc_type in exc_types:
                if isinstance(exc, exc_type):
                    log.warn('Encountered %s error when calling %s, retrying...',
                             exc.__class__.__name__, fn.__name__)
                    break
            else:
                raise exc

        time.sleep(step)
        waited += step
        if step < timeout / 30:
            step *= 2

    raise TimeoutError()
