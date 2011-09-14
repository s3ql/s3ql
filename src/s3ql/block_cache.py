'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from .backends.common import CompressFilter
from .common import sha256_fh
from .database import NoSuchRowError
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
                
                
class SimpleEvent(object):
    '''
    Like threading.Event, but without any internal flag. Calls
    to `wait` always block until some other thread calls
    `notify` or `notify_all`.
    '''
    
    def __init__(self):
        super(SimpleEvent, self).__init__()
        self.__cond = threading.Condition(threading.Lock())

    def notify_all(self):
        self.__cond.acquire()
        try:
            self.__cond.notify_all()
        finally:
            self.__cond.release()

    def notify(self):
        self.__cond.acquire()
        try:
            self.__cond.notify()
        finally:
            self.__cond.release()
            
    def wait(self):
        self.__cond.acquire()
        try:
            self.__cond.wait()
            return
        finally:
            self.__cond.release()
  
                               
class CacheEntry(file):
    """An element in the block cache
    
    If `obj_id` is `None`, then the object has not yet been
    uploaded to the backend. 
    
    Attributes:
    -----------
    
    :dirty:
       entry has been changed since it was last uploaded.
    
    :modified_after_upload:
       entry has been changed since the *beginning* of the
       last upload.
       
    This implies that if `dirty` is True, but `modified_after_upload`
    is False, an upload of this cache entry is currently in progress.
    
    An upload may also be in progress if `dirty` is True and
    `modified_after_upload` is True, but this upload is then already outdated
    and has a corresponding entry in the removal queue.
    """

    __slots__ = [ 'dirty', 'inode', 'blockno', 'last_access',
                  'modified_after_upload' ]

    def __init__(self, inode, blockno, filename):
        # Open unbuffered, so that os.fstat(fh.fileno).st_size is
        # always correct (we can expect that data is read and
        # written in reasonable chunks)
        super(CacheEntry, self).__init__(filename, "w+b", buffering=0)
        self.dirty = False
        self.modified_after_upload = False
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
        return ('<CacheEntry, inode=%d, blockno=%d>' % (self.inode, self.blockno))

class BlockCache(object):
    """Provides access to file blocks
    
    This class manages access to file blocks. It takes care of creation,
    uploading, downloading and deduplication.
 
    This class uses the llfuse global lock. Methods which release the lock have
    are marked as such in their docstring.
    
    Attributes
    ----------
    
    :path: where cached data is stored
    :entries: ordered dictionary of cache entries
    :size: current size of all cached entries
    :max_size: maximum size to which cache can grow
    :in_transit: set of objects currently in transit
    :removed_in_transit: set of objects that have been removed from the db
       while in transit, and should be removed from the backend as soon
       as the transit completes.
    :to_upload: distributes objects to upload to worker threads
    :to_remove: distributes objects to remove to worker threads
    :transfer_complete: signals completion of an object transfer
       (either upload or download)
    
    The `in_transit` attribute is used to
    - Prevent multiple threads from downloading the same object
    - Prevent threads from downloading an object before it has been
      uploaded completely (can happen when a cache entry is linked to a
      block while the object containing the block is still being
      uploaded)
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
        self.in_transit = set()
        self.removed_in_transit = set()
        self.to_upload = Distributor()
        self.to_remove = Queue()
        self.upload_threads = []
        self.removal_threads = []
        self.transfer_completed = SimpleEvent()

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
                log.debug('_do_upload(%d): uploaded %d bytes in %.3f seconds, %.2f MB/s',
                          obj_id, size, time_, rate)             
            
            if isinstance(dst_fh, CompressFilter):
                obj_size = dst_fh.compr_size
            else:
                obj_size = size
                                
            with lock:
                if obj_id in self.removed_in_transit:
                    # Object has been removed, either the corresponding cache
                    # entry does not exist anymore, or it has been assigned a
                    # new block id (since while we uploaded this object, the
                    # cache entry may have been modified again *and* a second
                    # upload may already have started)
                    self.removed_in_transit.remove(obj_id)
                    remove_again = True
                else:
                    remove_again = False
                    self.db.execute('UPDATE objects SET compr_size=? WHERE id=?',
                                    (obj_size, obj_id))

                    if not el.modified_after_upload:
                        el.dirty = False     
                          
                # Don't move this into finally block, would race with obj_id
                # being added to removed_in_transit
                self.in_transit.remove(obj_id)
                                                     
            # Postponed removal, we don't want to hold the lock                         
            if remove_again:
                self._do_removal(obj_id)
                
        except:
            # In case of errors we still want to remove the object from
            # in_transit, to prevent other threads hanging (e.g. copy_tree)
            with lock:
                self.in_transit.remove(obj_id)
        
        finally:
            self.transfer_completed.notify_all()
    
    def wait(self):
        '''Wait until an object has been transferred
        
        If there are no objects in transit, return immediately. This method
        releases the global lock.
        '''
        
        if not self.transfer_in_progress():
            return
        
        with lock_released:
            self.transfer_completed.wait()
                                            
    def upload(self, el):
        '''Upload cache entry `el` asynchronously
        
        Return (uncompressed) size of cache entry.
        
        This method releases the global lock.
        '''
        
        log.debug('upload(%s): start', el)
        
        
        size = os.fstat(el.fileno()).st_size
        el.seek(0)
        if log.isEnabledFor(logging.DEBUG):
            time_ = time.time()
            hash_ = sha256_fh(el)
            time_ = time.time() - time_
            rate = size / (1024**2 * time_) if time_ != 0 else 0
            log.debug('upload(%s): hashed %d bytes in %.3f seconds, %.2f MB/s',
                      el, size, time_, rate)             
        else:
            hash_ = sha256_fh(el)
        
        try:
            old_block_id = self.db.get_val('SELECT block_id FROM inode_blocks_v '
                                           'WHERE inode=? AND blockno=?', 
                                           (el.inode, el.blockno))
        except NoSuchRowError:
            old_block_id = None
            
        try:
            block_id = self.db.get_val('SELECT id FROM blocks WHERE hash=?', (hash_,))

        except NoSuchRowError:
            need_upload = True
            obj_id = self.db.rowid('INSERT INTO objects (refcount) VALUES(1)')
            log.debug('upload(%s):: created new object %d', el, obj_id)
            block_id = self.db.rowid('INSERT INTO blocks (refcount, hash, obj_id, size) '
                                        'VALUES(?,?,?,?)', (1, hash_, obj_id, size))
            log.debug('upload(%s): created new block %d', el, block_id)

        else:
            need_upload = False
            if old_block_id == block_id:
                log.debug('upload(%s): unchanged, block_id=%d', el, block_id)
                el.dirty = False
                el.modified_after_upload = False
                return size
                  
            log.debug('upload(%s): (re)linking to %d', el, block_id)
            self.db.execute('UPDATE blocks SET refcount=refcount+1 WHERE id=?',
                            (block_id,))
            
        # Check if we have to remove an old block
        to_delete = False
        if old_block_id is None:
            log.debug('upload(%s): no previous object', el)
            if el.blockno == 0:
                self.db.execute('UPDATE inodes SET block_id=? WHERE id=?', (block_id, el.inode))
            else:
                self.db.execute('INSERT INTO inode_blocks (block_id, inode, blockno) VALUES(?,?,?)',
                                (block_id, el.inode, el.blockno))    
        else:
            if el.blockno == 0:
                self.db.execute('UPDATE inodes SET block_id=? WHERE id=?', (block_id, el.inode))
            else:
                self.db.execute('UPDATE inode_blocks SET block_id=? WHERE inode=? AND blockno=?',
                                (block_id, el.inode, el.blockno))
                
            refcount = self.db.get_val('SELECT refcount FROM blocks WHERE id=?', (old_block_id,))
            if refcount > 1:
                log.debug('upload(%s):  decreased refcount for prev. block: %d', el, old_block_id)
                self.db.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?', (old_block_id,))
            else:
                log.debug('upload(%s): removing prev. block %d', el, old_block_id)
                old_obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (old_block_id,))
                self.db.execute('DELETE FROM blocks WHERE id=?', (old_block_id,))
                refcount = self.db.get_val('SELECT refcount FROM objects WHERE id=?', (old_obj_id,))
                if refcount > 1:
                    log.debug('upload(%s):  decreased refcount for prev. obj: %d',
                              el, old_obj_id)
                    self.db.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                                    (old_obj_id,))
                else:
                    log.debug('upload(%s): marking prev. obj %d for removal',
                              el, old_obj_id)
                    self.db.execute('DELETE FROM objects WHERE id=?', (old_obj_id,))
                    to_delete = True

        if need_upload:
            log.debug('upload(%s): adding to queue', el)
            el.modified_after_upload = False
            self.in_transit.add(obj_id)
            
            # Create a new fd so that we don't get confused if another thread
            # repositions the cursor (and do so before unlocking)
            fh = open(el.name, 'rb')
            with lock_released:
                if not self.upload_threads:
                    log.warn("upload(%s): no upload threads, uploading synchronously", el)
                    self._do_upload(el, fh, size, obj_id)
                else:
                    self.to_upload.put((el, fh, size, obj_id))

        else:
            el.dirty = False
            el.modified_after_upload = False
                
        if to_delete:
            log.debug('upload(%s):: removing object %d',  el, old_obj_id)
            
            if old_obj_id in self.in_transit:
                # Tell transit thread that the object has been deleted
                self.removed_in_transit.add(old_obj_id)                  
            if not self.removal_threads:
                log.warn("upload(%s): no removal threads, removing synchronously", el)
                with lock_released:
                    self._do_removal(old_obj_id)
            else:            
                self.to_remove.put(old_obj_id)
                                                
        log.debug('upload(%s): end', el)
        return size
            
    def transfer_in_progress(self):
        '''Return True if there are any blocks in transit'''
        
        return len(self.in_transit) > 0

    def _removal_loop(self):
        '''Process removal queue'''
              
        while True:
            tmp = self.to_remove.get()
            
            if tmp is QuitSentinel:
                break   
            
            self._do_removal(*tmp)        
    
    def _do_removal(self, obj_id):
        '''Remove object'''
      
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

        el = None
        while el is None:
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
                    el = CacheEntry(inode, blockno, filename)
                    
                # Need to download corresponding object
                else:
                    log.debug('get(inode=%d, block=%d): downloading block', inode, blockno)
                    obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
                    
                    if obj_id in self.in_transit:
                        with lock_released:
                            self.transfer_completed.wait()
                            continue
                        
                    # We need to download
                    self.in_transit.add(obj_id)
                    try:
                        el = CacheEntry(inode, blockno, filename)   
                        with lock_released:
                            with self.bucket_pool() as bucket:
                                with bucket.open_read('s3ql_data_%d' % obj_id) as fh:
                                    shutil.copyfileobj(fh, el)
                    except:
                        os.unlink(filename)
                        raise
                    finally:
                        self.in_transit.remove(obj_id)
                        with lock_released:
                            self.transfer_completed.notify_all()
                        
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
                    if el.modified_after_upload:
                        log.debug('expire: %s is dirty, trying to flush', el)
                        break
                    else:
                        # Already being uploaded
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
                
            # Try to upload just enough
            for el in self.entries.values_rev():
                log.debug('expire: uploading %s..', el)
                if el.dirty and el.modified_after_upload:
                    freed = self.upload(el) # Releases global lock
                    need_size -= freed
                else:
                    need_size -= os.fstat(el.fileno()).st_size
                need_entries -= 1                
        
                if need_size <= 0 and need_entries <= 0:
                    break
                                    
            # Wait for the next entry  
            log.debug('expire: waiting for transfer threads..')
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
                log.debug('remove(inode=%d, blockno=%d): removing from cache', 
                          inode, blockno)
                
                # Type inference fails here
                #pylint: disable-msg=E1103
                el = self.entries.pop((inode, blockno))

                self.size -= os.fstat(el.fileno()).st_size
                el.close()
                os.unlink(el.name)

            try:
                block_id = self.db.get_val('SELECT block_id FROM inode_blocks_v '
                                           'WHERE inode=? AND blockno=?', (inode, blockno))
            except NoSuchRowError:
                log.debug('remove(inode=%d, blockno=%d): block not in db', inode, blockno)
                continue

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
                if obj_id in self.in_transit:
                    # Tell transit thread that the object has been deleted
                    self.removed_in_transit.add(obj_id)
                elif not self.removal_threads:  
                    log.warn("remove(): no removal threads, removing synchronously")
                    with lock_released:
                        self._do_removal(obj_id)
                else:                            
                    self.to_remove.put(obj_id)
 
        log.debug('remove(inode=%d, start=%d, end=%s): end', inode, start_no, end_no)

    def flush(self, inode):
        """Flush buffers for `inode`"""

        # Cache entries are automatically flushed after each read() and write()
        pass

    def commit(self):
        """Initiate upload of all dirty blocks
        
        When the method returns, all blocks have been registered
        in the database (but the actual uploads may still be 
        in progress).
        
        This method releases the global lock.
        """
    
        for el in self.entries.itervalues():
            if not (el.dirty and el.modified_after_upload):
                continue
            
            self.upload(el) # Releases global lock
          
    def clear(self):
        """Clear cache
        
        This method releases the global lock.
        """

        log.debug('clear: start')
        bak = self.max_entries
        self.max_entries = 0
        self.expire() # Releases global lock
        self.max_entries = bak
            
        log.debug('clear: end')


    def __del__(self):
        if len(self.entries) > 0:
            raise RuntimeError("BlockCache instance was destroyed without calling destroy()!")

