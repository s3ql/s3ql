'''
upload_manager.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from .backends.common import  CompressFilter
from .common import sha256_fh
from .database import NoSuchRowError
from Queue import Queue
from llfuse import lock, lock_released
import errno
import logging
import os
import shutil
import time
import threading

log = logging.getLogger("UploadManager")

# Special queue entry that signals threads to terminate
QuitSentinel = object()

class Distributor(object):
    '''
    Distributes objects to consumers.
    '''
    
    def __init__(self):
        super(Distributor, self).__init__()
        
        self.slot = None
        self.cv = threading.Condition()
        
    def put(self, obj):
        '''Offer *obj* for consumption
        
        The method blocks until another thread calls `get()` to consume
        the object.
        '''
        
        if obj is None:
            raise ValueError("Can't put None into Queue")
        
        with self.cv:
            while self.slot is not None:
                self.cv.wait()
            self.slot = obj
            self.cv.notify_all()
            
    def get(self):
        '''Consume and return an object
        
        The method blocks until another thread offers an object
        by calling the `put` method.
        '''
        with self.cv:
            while not self.slot:
                self.cv.wait()
            tmp = self.slot
            self.slot = None
            self.cv.notify_all()
        return tmp

        
class UploadManager(object):
    '''
    Schedules and executes object uploads to make optimum usage of network
    bandwidth and CPU time.
    
    Methods which release the global lock have are marked as such in their
    docstring.
    '''
    
    def __init__(self, bucket_pool, db):
        
        self.bucket_pool = bucket_pool
        self.db = db
        self.in_transit = set()
        self.to_upload = Distributor()
        self.to_remove = Queue()
        self.upload_threads = []
        self.remove_threads = []
        self.upload_completed = threading.Event()
        
    def init(self, threads=1):
        '''Start worker threads'''
        
        # Start threads as daemons, so that we don't get hangs if the main
        # program terminates with an exception. Note that this actually makes
        # calling the destroy() method especially important, otherwise threads
        # may be interrupted by program exit in the middle of their work.
        for _ in range(threads):
            t = threading.Thread(target=self._do_uploads)
            t.daemon = True
            t.start()
            self.upload_threads.append(t)
            
        for _ in range(10): 
            t = threading.Thread(target=self._do_removals)
            t.daemon = True
            t.start()
            self.remove_threads.append(t)
                        
    def destroy(self):
        '''Stop worker threads'''
        for t in self.upload_threads:
            self.to_upload.put(QuitSentinel)
        
        for t in self.remove_threads:
            self.to_remove.put(QuitSentinel)
                    
        with lock_released:
            for t in self.upload_threads:
                t.join()

            for t in self.remove_threads:
                t.join()
                            
        self.upload_threads = []
        self.remove_threads = []
                
    def _do_uploads(self):
        '''Upload objects
        
        After upload, the file handle is closed and the compressed block size is
        updated in the database
         
        No matter how this method terminates, the block is removed from the
        in_transit set.
        '''
        
        while True:
            tmp = self.to_upload.get()
            if tmp is QuitSentinel:
                break         
            (el, src_fh, size, obj_id) = tmp
            
            try:
                if log.isEnabledFor(logging.DEBUG):
                    time_ = time.time()
                       
                with self.bucket_pool() as bucket:  
                    with bucket.open_write('s3ql_data_%d' % obj_id) as dst_fh:
                        shutil.copyfileobj(src_fh, dst_fh)
                src_fh.close()
                  
                if log.isEnabledFor(logging.DEBUG):
                    time_ = time.time() - time_
                    rate = size / (1024**2 * time_) if time_ != 0 else 0
                    log.debug('UploadThread(inode=%d, blockno=%d): '
                             'uploaded %d bytes in %.3f seconds, %.2f MB/s',
                              el.inode, el.blockno, size, time_, rate)             
                
                if isinstance(dst_fh, CompressFilter):
                    obj_size = dst_fh.compr_size
                else:
                    obj_size = size
                                    
                with lock:
                    self.db.execute('UPDATE objects SET compr_size=? WHERE id=?', (obj_size, obj_id))
                    
                    if not el.modified_after_upload:
                        el.dirty = False       
    
            finally:
                with lock:
                    self.in_transit.remove((el.inode, el.blockno))
                    self.upload_completed.set()
          
    def join_one(self):
        '''Wait until an object has been uploaded
        
        If there are no objects in transit, return immediately. This method
        releases the global lock.
        '''
        
        if not self.upload_in_progress():
            return
        
        self.upload_completed.clear()
        with lock_released:
            self.upload_completed.wait()
                                            
    def add(self, el):
        '''Upload cache entry `el` asynchronously
        
        Return (uncompressed) size of cache entry.
        
        This method releases the global lock.
        '''
        
        # TODO: Big parts of this should be moved into _do_work()
        
        log.debug('UploadManager.add(%s): start', el)

        if not self.upload_threads:
            raise RuntimeError("No upload threads started")
        
        if (el.inode, el.blockno) in self.in_transit:
            raise ValueError('Block already in transit')
        
        old_block_id = el.block_id
        size = os.fstat(el.fileno()).st_size
        el.seek(0)
        if log.isEnabledFor(logging.DEBUG):
            time_ = time.time()
            hash_ = sha256_fh(el)
            time_ = time.time() - time_
            if time_ != 0:
                rate = size / (1024**2 * time_)
            else:
                rate = 0
            log.debug('UploadManager(inode=%d, blockno=%d): '
                     'hashed %d bytes in %.3f seconds, %.2f MB/s',
                      el.inode, el.blockno, size, time_, rate)             
        else:
            hash_ = sha256_fh(el)
        
        # Check if we need to upload a new block, or can link
        # to an existing block
        try:
            el.block_id = self.db.get_val('SELECT id FROM blocks WHERE hash=?', (hash_,))

        except NoSuchRowError:
            need_upload = True
            obj_id = self.db.rowid('INSERT INTO objects (refcount) VALUES(1)')
            log.debug('add(inode=%d, blockno=%d): created new object %d',
                      el.inode, el.blockno, obj_id)
            el.block_id = self.db.rowid('INSERT INTO blocks (refcount, hash, obj_id, size) VALUES(?,?,?,?)',
                                        (1, hash_, obj_id, size))
            log.debug('add(inode=%d, blockno=%d): created new block %d',
                      el.inode, el.blockno, el.block_id)

        else:
            need_upload = False
            if old_block_id == el.block_id:
                log.debug('add(inode=%d, blockno=%d): unchanged, block_id=%d',
                          el.inode, el.blockno, el.block_id)
                el.dirty = False
                el.modified_after_upload = False
                return size
                  
            log.debug('add(inode=%d, blockno=%d): (re)linking to %d',
                      el.inode, el.blockno, el.block_id)
            self.db.execute('UPDATE blocks SET refcount=refcount+1 WHERE id=?',
                            (el.block_id,))
            
        # Check if we have to remove an old block
        to_delete = False
        if old_block_id is None:
            log.debug('add(inode=%d, blockno=%d): no previous object',
                      el.inode, el.blockno)
            if el.blockno == 0:
                self.db.execute('UPDATE inodes SET block_id=? WHERE id=?', (el.block_id, el.inode))
            else:
                self.db.execute('INSERT INTO inode_blocks (block_id, inode, blockno) VALUES(?,?,?)',
                                (el.block_id, el.inode, el.blockno))    
        else:
            if el.blockno == 0:
                self.db.execute('UPDATE inodes SET block_id=? WHERE id=?', (el.block_id, el.inode))
            else:
                self.db.execute('UPDATE inode_blocks SET block_id=? WHERE inode=? AND blockno=?',
                                (el.block_id, el.inode, el.blockno))
                
            refcount = self.db.get_val('SELECT refcount FROM blocks WHERE id=?', (old_block_id,))
            if refcount > 1:
                log.debug('add(inode=%d, blockno=%d):  decreased refcount for prev. block: %d',
                          el.inode, el.blockno, old_block_id)
                self.db.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?', (old_block_id,))
            else:
                log.debug('add(inode=%d, blockno=%d): removing prev. block %d',
                          el.inode, el.blockno, old_block_id)
                old_obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (old_block_id,))
                self.db.execute('DELETE FROM blocks WHERE id=?', (old_block_id,))
                refcount = self.db.get_val('SELECT refcount FROM objects WHERE id=?', (old_obj_id,))
                if refcount > 1:
                    log.debug('add(inode=%d, blockno=%d):  decreased refcount for prev. obj: %d',
                          el.inode, el.blockno, old_obj_id)
                    self.db.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                                    (old_obj_id,))
                else:
                    log.debug('add(inode=%d, blockno=%d): marking prev. obj %d for removal',
                              el.inode, el.blockno, old_obj_id)
                    self.db.execute('DELETE FROM objects WHERE id=?', (old_obj_id,))
                    to_delete = True

        if need_upload:
            log.debug('add(inode=%d, blockno=%d): adding to queue', 
                      el.inode, el.blockno)
            el.modified_after_upload = False
            self.in_transit.add((el.inode, el.blockno))
            
            # Create a new fd so that we don't get confused if another thread
            # repositions the cursor (and do so before unlocking)
            fh = open(el.name, 'rb')
            with lock_released:
                self.to_upload.put((el, fh, size, obj_id))

        else:
            el.dirty = False
            el.modified_after_upload = False
                
        if to_delete:
            log.debug('add(inode=%d, blockno=%d): removing object %d', 
                      el.inode, el.blockno, old_obj_id)
            
            # Note: Old object can not be in transit
            # FIXME: Probably no longer true once objects can contain several blocks
            self.remove(old_obj_id)
                                                
        log.debug('add(inode=%d, blockno=%d): end', el.inode, el.blockno)
        return size
            
    def upload_in_progress(self):
        '''Return True if there are any blocks in transit'''
        
        return len(self.in_transit) > 0
    
    def remove(self, obj_id, transit_key=None):
        '''
        Remove an object from backend. If a transit key is specified, the removing
        thread first waits until the object is no longer in transit.
        '''

        # TODO: Not sure how to handle the transit key..
        # Shouldn't we wait for upload of the *object*?
        self.to_remove.put((obj_id, transit_key))

    def _do_removals(self):
        '''Remove objects'''
              
        while True:
            tmp = self.to_remove.get()
            
            if tmp is QuitSentinel:
                break         

            (obj_id, transit_key) = tmp
            
            if transit_key:
                while transit_key in self.in_transit:
                    with lock:
                        self.join_one()
                    
            with self.bucket_pool() as bucket:
                bucket.delete('s3ql_data_%d' % obj_id)            