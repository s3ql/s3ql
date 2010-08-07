'''
upload_manager.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .backends.common import NoSuchObject
from .common import sha256_fh, without, TimeoutError
from .thread_group import ThreadGroup, Thread
from . import database as dbcm
from .database import NoSuchRowError
import logging
import os
import time

__all__ = [ "UploadManager", 'retry_exc', 'RemoveThread' ]

# standard logger for this module
log = logging.getLogger("UploadManager")
     

MAX_UPLOAD_THREADS = 10
MIN_TRANSIT_SIZE = 1024 * 1024
class UploadManager(object):
    '''
    Schedules and executes object uploads to make optimum usage
    network bandwidth and CPU time.
    '''
    
    def __init__(self, bucket, removal_queue):
        self.threads = ThreadGroup(MAX_UPLOAD_THREADS)
        self.removal_queue = removal_queue
        self.bucket = bucket
        self.transit_size = 0
        self.in_transit = set()
        
    def add(self, el, lock):
        '''Upload cache entry `el` asynchronously
        
        Return (uncompressed) size of cache entry.
        
        `lock` is released while the cache entry is compressed and
        while waiting for a free upload slot if the maximum number
        of parallel uploads has been reached.
        '''
        
        log.debug('UploadManager.add(%s): start', el)

        if (el.inode, el.blockno) in self.in_transit:
            raise ValueError('Block already in transit')
        
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

        if need_upload:
            el.modified_after_upload = False
            self.in_transit.add((el.inode, el.blockno))
            try:
                # Create a new fd so that we don't get confused if another
                # thread repositions the cursor (and do so before unlocking)
                with open(el.name + '.d', 'rb') as fh:
                    with without(lock):
                        (compr_size, fn) = self.bucket.prep_store_fh('s3ql_data_%d' % el.obj_id, 
                                                                     fh)
                dbcm.execute('UPDATE objects SET compr_size=? WHERE id=?', 
                             (compr_size, el.obj_id))
    
                # If we already have the minimum transit size, do not start more
                # than two threads
                log.debug('add(%s): starting upload thread', el)
                if self.transit_size > MIN_TRANSIT_SIZE:
                    max_threads = 2
                else:
                    max_threads = None
                self.transit_size += compr_size        
                with without(lock):
                    self.threads.add_thread(UploadThread(fn, el, compr_size, self), 
                                            max_threads)
            except:
                self.in_transit.remove((el.inode, el.blockno))
                raise
        else:
            el.dirty = False
            el.modified_after_upload = False
            os.rename(el.name + '.d', el.name)
                
        if to_delete:
            log.debug('add(%s): removing object %d', el, old_obj_id)
            with without(lock):
                self.removal_queue.add_thread(RemoveThread(old_obj_id,
                                                           self.bucket))
                                
        log.debug('add(%s): end', el)
        return size

    def join_all(self):
        self.threads.join_all()
        assert not self.in_transit

    def join_one(self):
        self.threads.join_one()
            
    def upload_in_progress(self):
        return len(self.threads) > 0
    
                    
class UploadThread(Thread):
    '''
    Uploads a cache entry with the function passed in the constructor.
    '''
    
    def __init__(self, fn, el, size, um):
        super(UploadThread, self).__init__()
        self.fn = fn
        self.el = el
        self.size = size
        self.time = 0
        self.um = um
        
    def run_protected(self):
        '''Call self.fn() and time execution
        
        This method expects to run in a separate thread.
        '''
        self.time = time.time()
        self.fn()
        self.time = time.time() - self.time           
                      
    def finalize(self):
        '''Mark cache entry as uploaded
        
        This function should be called from the main thread once
        the thread running `run_protected` has finished. 
        '''
        
        self.um.transit_size -= self.size
        self.um.in_transit.remove((self.el.inode, self.el.blockno))
        log.debug('UploadThread(inode=%d, blockno=%d): '
                 'transferred %d bytes in %.3f seconds, %.2f MB/s',
                  self.el.inode, self.el.blockno, self.size, 
                  self.time, self.size / (1024**2 * self.time))     
                
        if not self.el.modified_after_upload:
            self.el.dirty = False
            os.rename(self.el.name + '.d', self.el.name)          
        
      
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

class RemoveThread(Thread):
    '''
    Remove an object from backend
    '''

    def __init__(self, id_, bucket):
        super(RemoveThread, self).__init__()
        self.id = id_
        self.bucket = bucket
            
    def run_protected(self): 
        if self.bucket.read_after_create_consistent():
            self.bucket.delete('s3ql_data_%d' % self.id)
        else:
            retry_exc(300, [ NoSuchObject ], self.bucket.delete,
                      's3ql_data_%d' % self.id)