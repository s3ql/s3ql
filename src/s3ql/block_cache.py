'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from .backends.common import CompressFilter
from .common import sha256_fh
from .database import NoSuchRowError
from .multi_lock import MultiLock
from .ordered_dict import OrderedDict
from Queue import Queue
from contextlib import contextmanager
from llfuse import lock, lock_released
import logging
import os
import shutil
import threading
import time

# standard logger for this module
log = logging.getLogger("BlockCache")
 
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
                   
class CacheEntry(file):
    """An element in the block cache
    
    If `obj_id` is `None`, then the object has not yet been
    uploaded to the backend. 
    
    Attributes:
    -----------
    
    :modified_after_upload:
        This attribute is only significant when the cache entry
        is currently being uploaded. At the beginning of the upload,
        it is set to False. For any write access, it is set to True.
        If it is still False when the upload has completed, 
        `dirty` is set to False.

    """

    __slots__ = [ 'dirty', 'block_id', 'inode', 'blockno', 'last_access',
                  'modified_after_upload' ]

    def __init__(self, inode, blockno, block_id, filename):
        # Open unbuffered, so that os.fstat(fh.fileno).st_size is
        # always correct (we can expect that data is read and
        # written in reasonable chunks)
        super(CacheEntry, self).__init__(filename, "w+b", buffering=0)
        self.dirty = False
        self.modified_after_upload = False
        self.block_id = block_id
        self.inode = inode
        self.blockno = blockno
        self.last_access = 0

    def truncate(self, *a, **kw):
        self.dirty = True
        self.modified_after_upload = True
        return super(CacheEntry, self).truncate(*a, **kw)

    def write(self, *a, **kw):
        self.dirty = True
        self.modified_after_upload = True
        return super(CacheEntry, self).write(*a, **kw)

    def writelines(self, *a, **kw):
        self.dirty = True
        self.modified_after_upload = True
        return super(CacheEntry, self).writelines(*a, **kw)

    def __str__(self):
        return ('<CacheEntry, inode=%d, blockno=%d, dirty=%s, block_id=%r>' % 
                (self.inode, self.blockno, self.dirty, self.block_id))

class BlockCache(object):
    """Provides access to file blocks
    
    This class manages access to file blocks. It takes care of creation,
    uploading, downloading and deduplication.
 
    This class uses the llfuse global lock. Methods which release the lock have
    are marked as such in their docstring.

            
    Attributes:
    -----------
    
    :mlock: locks on (inode, blockno) during `get`, so that we do not
            download the same object with more than one thread.
    """

    def __init__(self, bucket_pool, db, cachedir, max_size, max_entries=768):
        log.debug('Initializing')
        
        self.path = cachedir
        self.db = db
        self.bucket_pool = bucket_pool
        self.entries = OrderedDict()
        self.max_entries = max_entries
        self.size = 0
        self.max_size = max_size
        self.mlock = MultiLock()
        self.in_transit = set()
        self.to_upload = Distributor()
        self.to_remove = Queue()
        self.upload_threads = []
        self.removal_threads = []
        self.upload_completed = threading.Event()

        if not os.path.exists(self.path):
            os.mkdir(self.path)
        
    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.entries)

    def init(self, threads=1):
        '''Start worker threads'''
        
        for _ in range(threads):
            t = threading.Thread(target=self._upload_loop)
            t.start()
            self.upload_threads.append(t)
            
        for _ in range(10): 
            t = threading.Thread(target=self._removal_loop)
            t.daemon = True # interruption will do no permanent harm
            t.start()
            self.removal_threads.append(t)
                        
    def destroy(self):
        '''Clean up and stop worker threads'''
        
        self.clear()
        
        for t in self.upload_threads:
            self.to_upload.put(QuitSentinel)
        
        for t in self.removal_threads:
            self.to_remove.put(QuitSentinel)
                    
        with lock_released:
            for t in self.upload_threads:
                t.join()

            for t in self.removal_threads:
                t.join()
                            
        self.upload_threads = []
        self.removal_threads = []
                
        os.rmdir(self.path)
        
    def _upload_loop(self):
        '''Process upload queue'''
              
        while True:
            tmp = self.to_upload.get()
            
            if tmp is QuitSentinel:
                break   
            
            self._do_upload(*tmp)   
                    
    def _do_upload(self, el, src_fh, size, obj_id):
        '''Upload object
        
        After upload, the file handle is closed and the compressed block size is
        updated in the database
         
        No matter how this method terminates, the block is removed from the
        in_transit set.
        '''
        
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
          
    def wait(self):
        '''Wait until an object has been uploaded
        
        If there are no objects in transit, return immediately. This method
        releases the global lock.
        '''
        
        if not self.upload_in_progress():
            return
        
        self.upload_completed.clear()
        with lock_released:
            self.upload_completed.wait()
                                            
    def upload(self, el):
        '''Upload cache entry `el` asynchronously
        
        Return (uncompressed) size of cache entry.
        
        This method releases the global lock.
        '''
        
        # TODO: Big parts of this should be moved into _do_work()
        
        log.debug('UploadManager.add(%s): start', el)

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
                if not self.upload_threads:
                    log.warn("upload(): no upload threads, uploading synchronously")
                    self._do_upload(el, fh, size, obj_id)
                else:
                    self.to_upload.put((el, fh, size, obj_id))

        else:
            el.dirty = False
            el.modified_after_upload = False
                
        if to_delete:
            log.debug('add(inode=%d, blockno=%d): removing object %d', 
                      el.inode, el.blockno, old_obj_id)
            
            # Note: Old object can not be in transit
            # FIXME: Probably no longer true once objects can contain several blocks
            if not self.removal_threads:
                log.warn("upload(): no removal threads, removing synchronously")
                with lock_released:
                    self._do_removal(old_obj_id, None)
            else:            
                self.to_remove.put((old_obj_id, None))
                                                
        log.debug('add(inode=%d, blockno=%d): end', el.inode, el.blockno)
        return size
            
    def upload_in_progress(self):
        '''Return True if there are any blocks in transit'''
        
        return len(self.in_transit) > 0

    def _removal_loop(self):
        '''Process removal queue'''
              
        while True:
            tmp = self.to_remove.get()
            
            if tmp is QuitSentinel:
                break   
            
            self._do_removal(*tmp)        
    
    def _do_removal(self, obj_id, transit_key):
        '''Remove object'''
              
        # TODO: Not sure how to handle the transit key..
        # Shouldn't we wait for upload of the *object*?            
        if transit_key:
            while transit_key in self.in_transit:
                with lock:
                    self.wait()
                
        with self.bucket_pool() as bucket:
            bucket.delete('s3ql_data_%d' % obj_id)           
                
    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for block `blockno` of `inode`
        
        This method releases the global lock, and the managed block
        may do so as well.
        
        Note: if `get` and `remove` are called concurrently, then it is
        possible that a block that has been requested with `get` and
        passed to `remove` for deletion will not be deleted.
        """

        log.debug('get(inode=%d, block=%d): start', inode, blockno)

        if self.size > self.max_size or len(self.entries) > self.max_entries:
            self.expire()

        # Need to release global lock to acquire mlock to prevent deadlocking
        lock.release()
        with self.mlock(inode, blockno):
            lock.acquire()
            
            try:
                el = self.entries[(inode, blockno)]
    
            # Not in cache
            except KeyError:
                filename = os.path.join(self.path,
                                        'inode_%d_block_%d' % (inode, blockno))
                try:
                    block_id = self.db.get_val('SELECT block_id FROM inode_blocks_v '
                                               'WHERE inode=? AND blockno=?', (inode, blockno))                    
    
                # No corresponding object
                except NoSuchRowError:
                    log.debug('get(inode=%d, block=%d): creating new block', inode, blockno)
                    el = CacheEntry(inode, blockno, None, filename)
    
                # Need to download corresponding object
                else:
                    log.debug('get(inode=%d, block=%d): downloading block', inode, blockno)
                    # At the moment, objects contain just one block
                    el = CacheEntry(inode, blockno, block_id, filename)
                    obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
                    
                    try:
                        with lock_released:
                            with self.bucket_pool() as bucket:
                                with bucket.open_read('s3ql_data_%d' % obj_id) as fh:
                                    shutil.copyfileobj(fh, el)
                    except:
                        os.unlink(filename)
                        raise
                        
                    # Writing will have set dirty flag
                    el.dirty = False
                    
                    self.size += os.fstat(el.fileno()).st_size
    
                self.entries[(inode, blockno)] = el
    
            # In Cache
            else:
                log.debug('get(inode=%d, block=%d): in cache', inode, blockno)
                self.entries.to_head((inode, blockno))

        el.last_access = time.time()
        oldsize = os.fstat(el.fileno()).st_size

        # Provide fh to caller
        try:
            log.debug('get(inode=%d, block=%d): yield', inode, blockno)
            yield el
        finally:
            # Update cachesize
            newsize = os.fstat(el.fileno()).st_size
            self.size += newsize - oldsize

        log.debug('get(inode=%d, block=%d): end', inode, blockno)


    def expire(self):
        """Perform cache expiry
        
        This method releases the global lock.
        """

        # Note that we have to make sure that the cache entry is written into
        # the database before we remove it from the cache!

        log.debug('expire: start')

        while (len(self.entries) > self.max_entries or
               (len(self.entries) > 0  and self.size > self.max_size)):

            need_size = self.size - self.max_size
            need_entries = len(self.entries) - self.max_entries
            
            # Try to expire entries that are not dirty
            for el in self.entries.values_rev():
                if el.dirty:
                    if (el.inode, el.blockno) not in self.in_transit:
                        log.debug('expire: %s is dirty, trying to flush', el)
                        break
                    else:
                        continue
                
                del self.entries[(el.inode, el.blockno)]
                size = os.fstat(el.fileno()).st_size
                el.close()
                os.unlink(el.name)
                need_entries -= 1
                self.size -= size
                need_size -= size
                
                if need_size <= 0 and need_entries <= 0:
                    break
                     
            if need_size <= 0 and need_entries <= 0:
                break
                
            # If nothing is being uploaded, try to upload just enough
            if not self.upload_in_progress():
                for el in self.entries.values_rev():
                    log.debug('expire: uploading %s..', el)
                    if el.dirty and (el.inode, el.blockno) not in self.in_transit:
                        freed = self.upload(el) # Releases global lock
                        need_size -= freed
                    else:
                        need_size -= os.fstat(el.fileno()).st_size
                    need_entries -= 1                
            
                    if need_size <= 0 and need_entries <= 0:
                        break
                                    
            # Wait for the next entry  
            log.debug('expire: waiting for upload threads..')
            self.wait() # Releases global lock

        log.debug('expire: end')


    def remove(self, inode, start_no, end_no=None):
        """Remove blocks for `inode`
        
        If `end_no` is not specified, remove just the `start_no` block.
        Otherwise removes all blocks from `start_no` to, but not including,
         `end_no`. 
        
        This method releases the global lock.
        
        Note: if `get` and `remove` are called concurrently, then it is possible
        that a block that has been requested with `get` and passed to `remove`
        for deletion will not be deleted.
        """

        log.debug('remove(inode=%d, start=%d, end=%s): start', inode, start_no, end_no)

        if end_no is None:
            end_no = start_no + 1
            
        for blockno in range(start_no, end_no):
            # We can't use self.mlock here to prevent simultaneous retrieval
            # of the block with get(), because this could deadlock
            if (inode, blockno) in self.entries:
                # Type inference fails here
                #pylint: disable-msg=E1103
                el = self.entries.pop((inode, blockno))

                self.size -= os.fstat(el.fileno()).st_size
                el.close()
                os.unlink(el.name)

                if el.block_id is None:
                    log.debug('remove(inode=%d, blockno=%d): block only in cache',
                              inode, blockno)
                    continue

                log.debug('remove(inode=%d, blockno=%d): block in cache and db', inode, blockno)
                block_id = el.block_id

            else:
                try:
                    block_id = self.db.get_val('SELECT block_id FROM inode_blocks_v '
                                               'WHERE inode=? AND blockno=?', (inode, blockno))   
                except NoSuchRowError:
                    log.debug('remove(inode=%d, blockno=%d): block does not exist',
                              inode, blockno)
                    continue

                log.debug('remove(inode=%d, blockno=%d): block only in db ', inode, blockno)

            # Detach inode from block
            if blockno == 0:
                self.db.execute('UPDATE inodes SET block_id=NULL WHERE id=?', (inode,))
            else:
                self.db.execute('DELETE FROM inode_blocks WHERE inode=? AND blockno=?',
                                (inode, blockno))
                
            # Decrease block refcount
            refcount = self.db.get_val('SELECT refcount FROM blocks WHERE id=?', (block_id,))
            if refcount > 1:
                log.debug('remove(inode=%d, blockno=%d): decreasing refcount for block %d',
                          inode, blockno, block_id)                    
                self.db.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?',
                                (block_id,))
                continue
            
            # Detach block from object
            log.debug('remove(inode=%d, blockno=%d): deleting block %d',
                      inode, blockno, block_id)     
            obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
            self.db.execute('DELETE FROM blocks WHERE id=?', (block_id,))
            
            # Decrease object refcount
            refcount = self.db.get_val('SELECT refcount FROM objects WHERE id=?', (obj_id,))
            if refcount > 1:
                log.debug('remove(inode=%d, blockno=%d): decreasing refcount for object %d',
                          inode, blockno, obj_id)                    
                self.db.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                                (obj_id,))
            else:
                self.db.execute('DELETE FROM objects WHERE id=?', (obj_id,))
                if not self.removal_threads:
                    log.warn("remove(): no removal threads, removing synchronously")
                    with lock_released:
                        self._do_removal(obj_id, (inode, blockno))
                else:                            
                    self.to_remove.put((obj_id, (inode, blockno)))
 
        log.debug('remove(inode=%d, start=%d, end=%s): end', inode, start_no, end_no)

    def flush(self, inode):
        """Flush buffers for `inode`"""

        # Cache entries are automatically flushed after each read() and write()
        pass

    def commit(self):
        """Upload all dirty blocks
        
        This method releases the global lock.
        """
    
        in_transit = set()
        
        for el in self.entries.itervalues():
            if not el.dirty:
                continue
            
            if (el.inode, el.blockno) in self.in_transit:
                if not el.modified_after_upload:
                    continue
                
                # We need to wait for the current upload to complete
                in_transit.add(el)
            else:
                self.upload(el) # Releases global lock
    
        while in_transit:
            log.warn('commit(): in_transit: %s', in_transit)
            self.wait()
            finished = in_transit.difference(self.in_transit)
            in_transit = in_transit.intersection(self.in_transit)    
                    
            for el in finished:
                # Object may no longer be dirty or already in transit
                # if a different thread initiated the object while
                # the global lock was released in a previous iteration.
                if el.dirty:
                    continue
                if  (el.inode, el.blockno) in self.in_transit:
                    continue
                
                self.upload(el) # Releases global lock
                
        # Now wait for all uploads to complete
        while self.upload_in_progress():
            self.wait() # Releases global lock
          
    def clear(self):
        """Upload all dirty data and clear cache
        
        This method releases the global lock.
        """

        log.debug('clear: start')
        bak = self.max_entries
        self.max_entries = 0
        self.expire() # Releases global lock
        self.max_entries = bak

        # Now wait for all uploads to complete
        while self.upload_in_progress():
            self.wait() # Releases global lock
            
        log.debug('clear: end')


    def __del__(self):
        if len(self.entries) > 0:
            raise RuntimeError("BlockCache instance was destroyed without calling destroy()!")

