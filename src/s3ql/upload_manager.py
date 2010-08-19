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
import threading
import os
import errno
import time
from s3ql.common import EmbeddedException

__all__ = [ "UploadManager", 'retry_exc', 'RemoveThread' ]

# standard logger for this module
log = logging.getLogger("UploadManager")
     

MAX_UPLOAD_THREADS = 10
MAX_COMPRESS_THREADS = 1
MIN_TRANSIT_SIZE = 1024 * 1024
class UploadManager(object):
    '''
    Schedules and executes object uploads to make optimum usage
    network bandwidth and CPU time.
    
    Attributes:
    -----------
    
    :encountered_errors: This attribute is set if some non-fatal errors
            were encountered during asynchronous operations (for
            example, an object that was supposed to be deleted did
            not exist).    
    '''
    
    def __init__(self, bucket, removal_queue):
        self.upload_threads = ThreadGroup(MAX_UPLOAD_THREADS)
        self.compress_threads = ThreadGroup(MAX_COMPRESS_THREADS)
        self.removal_queue = removal_queue
        self.bucket = bucket
        self.transit_size = 0
        self.transit_size_lock = threading.Lock()
        self.in_transit = set()
        self.encountered_errors = False        
        
    def add(self, el, lock):
        '''Upload cache entry `el` asynchronously
        
        Return (uncompressed) size of cache entry.
        
        If there are no free compression or upload slots, `lock` is released
        while waiting for a free slot.
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
            log.debug('add(inode=%d, blockno=%d): starting compression thread', 
                      el.inode, el.blockno)
            el.modified_after_upload = False
            self.in_transit.add((el.inode, el.blockno))
            
            # Create a new fd so that we don't get confused if another
            # thread repositions the cursor (and do so before unlocking)
            fh = open(el.name + '.d', 'rb')
            with without(lock):
                self.compress_threads.add_thread(CompressThread(el, fh, self))

        else:
            el.dirty = False
            el.modified_after_upload = False
            os.rename(el.name + '.d', el.name)
                
        if to_delete:
            log.debug('add(inode=%d, blockno=%d): removing object %d', 
                      el.inode, el.blockno, old_obj_id)
            
            try:
                with without(lock):
                    # Old object can not be in transit
                    self.removal_queue.add_thread(RemoveThread(old_obj_id, self.bucket))
            except EmbeddedException as exc:
                exc = exc.exc
                if isinstance(exc, NoSuchObject):
                    log.warn('Backend seems to have lost object %s', exc.key)
                    self.encountered_errors = True
                else:
                    raise
                                                
        log.debug('add(inode=%d, blockno=%d): end', el.inode, el.blockno)
        return size

    def join_all(self):
        '''Wait until all blocks in transit have been uploaded'''
        
        self.compress_threads.join_all()
        self.upload_threads.join_all()
        assert not self.in_transit

    def join_one(self):
        '''Wait until one block has been uploaded
        
        If there are no blocks in transit, return immediately.
        '''
        
        if len(self.upload_threads) == 0:
            self.compress_threads.join_one()
            
        self.upload_threads.join_one()
            
    def upload_in_progress(self):
        '''Return True if there are any blocks in transit'''
        
        return len(self.compress_threads) + len(self.upload_threads) > 0
    
    
class CompressThread(Thread):
    '''
    Compress a block and then pass it on for uploading.
    '''
    
    def __init__(self, el, fh, um):
        super(CompressThread, self).__init__()
        self.el = el
        self.fh = fh
        self.um = um
        self.size = None
        
    def run_protected(self):
        '''Compress block
        
        After compression:
         - the file handle is closed
         - the compressed block size is updated in the database
         - an UploadThread instance started for uploading the data.
         
        In case of an exception, the block is removed from the in_transit
        set. 
        '''
                     
        (self.size, fn) = self.um.bucket.prep_store_fh('s3ql_data_%d' % self.el.obj_id, 
                                                  self.fh)
        self.fh.close()

        # If we already have the minimum transit size, do not start more
        # than two threads
        log.debug('CompressThread(%s): starting upload thread', self.el)
        with self.um.transit_size_lock:
            if self.um.transit_size > MIN_TRANSIT_SIZE:
                max_threads = 2
            else:
                max_threads = None
                
            self.um.transit_size += self.size    
            
        self.um.upload_threads.add_thread(UploadThread(fn, self.el, self.size, self.um), 
                                          max_threads)
            
    def handle_exc(self, exc):
        '''Remove block from in_transit unless UploadThread has been started'''
        
        if not isinstance(exc, EmbeddedException):
            self.um.in_transit.remove((self.el.inode, self.el.blockno))
            with self.um.transit_size_lock:
                self.um.transit_size -= self.size
            
    def finalize(self):
        dbcm.execute('UPDATE objects SET compr_size=? WHERE id=?', 
                     (self.size, self.el.obj_id))
                
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
        '''Upload block by calling self.fn()
        
        The upload duration is timed. After the upload (or if an exception
        occurs), the block is removed from in_transit.      
        '''
        self.time = time.time()
        self.fn()
        self.time = time.time() - self.time           
              
                
    def handle_exc(self, _):
        '''Remove block from in_transit'''
        
        self.um.in_transit.remove((self.el.inode, self.el.blockno))
        with self.um.transit_size_lock:
            self.um.transit_size -= self.size
            
    def finalize(self):
        '''Mark block as uploaded'''
        
        self.um.in_transit.remove((self.el.inode, self.el.blockno))
        with self.um.transit_size_lock:
            self.um.transit_size -= self.size
            
        log.debug('UploadThread(inode=%d, blockno=%d): '
                 'transferred %d bytes in %.3f seconds, %.2f MB/s',
                  self.el.inode, self.el.blockno, self.size, 
                  self.time, self.size / (1024**2 * self.time))     
                
        if not self.el.modified_after_upload:
            self.el.dirty = False
            try:
                os.rename(self.el.name + '.d', self.el.name)
            except OSError as exc:
                # Entry may have been removed while being uploaded
                if exc.errno != errno.ENOENT:
                    raise
        
      
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
    Remove an object from backend. If a transit key is specified, the
    thread first waits until the object is no longer in transit.
    '''

    def __init__(self, id_, bucket, transit_key=None, upload_manager=None):
        super(RemoveThread, self).__init__()
        self.id = id_
        self.bucket = bucket
        self.transit_key = transit_key
        self.um = upload_manager
            
    def run_protected(self): 
        if self.transit_key:
            while self.transit_key in self.um.in_transit:
                self.um.join_one()
                           
        if self.bucket.read_after_create_consistent():
            self.bucket.delete('s3ql_data_%d' % self.id)
        else:
            retry_exc(300, [ NoSuchObject ], self.bucket.delete,
                      's3ql_data_%d' % self.id)