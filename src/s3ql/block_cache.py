'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from contextlib import contextmanager
from .multi_lock import MultiLock
from .backends.common import NoSuchObject
from .ordered_dict import OrderedDict
from .common import EmbeddedException, without
from .thread_group import ThreadGroup
from . import database as dbcm
from .upload_manager import UploadManager, RemoveThread, retry_exc
from .database import NoSuchRowError
import logging
import os
import sys
import threading
import time
import stat

__all__ = [ "BlockCache" ]

# standard logger for this module
log = logging.getLogger("BlockCache")
        
               
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
        `dirty` is set to False and the object looses the ``.d`` suffix
        in its name.

    """

    __slots__ = [ 'dirty', 'obj_id', 'inode', 'blockno', 'last_access',
                  'modified_after_upload' ]

    def __init__(self, inode, blockno, obj_id, filename, mode):
        super(CacheEntry, self).__init__(filename, mode)
        self.dirty = False
        self.modified_after_upload = False
        self.obj_id = obj_id
        self.inode = inode
        self.blockno = blockno
        self.last_access = 0

    def truncate(self, *a, **kw):
        if not self.dirty:
            os.rename(self.name, self.name + '.d')
            self.dirty = True
        self.modified_after_upload = True
        return super(CacheEntry, self).truncate(*a, **kw)

    def write(self, *a, **kw):
        if not self.dirty:
            os.rename(self.name, self.name + '.d')
            self.dirty = True
        self.modified_after_upload = True
        return super(CacheEntry, self).write(*a, **kw)

    def writelines(self, *a, **kw):
        if not self.dirty:
            os.rename(self.name, self.name + '.d')
            self.dirty = True
        self.modified_after_upload = True
        return super(CacheEntry, self).writelines(*a, **kw)

    def __str__(self):
        return ('<CacheEntry, inode=%d, blockno=%d, dirty=%s, obj_id=%r>' % 
                (self.inode, self.blockno, self.dirty, self.obj_id))

MAX_REMOVAL_THREADS = 25
class BlockCache(object):
    """Provides access to file blocks
    
    This class manages access to file blocks. It takes care of creation,
    uploading, downloading and deduplication.
 
    This class is partially threadsafe: the constructor accepts a lock
    object, and it is safe to call any instance method (except for
    `destroy`) when the caller has acquired the lock.
    
    To allow limited concurrency, the lock is released by
    some instance methods for time consuming operations. 
    However, the caller must not hold any prior locks when calling
    such a method, since this may lead to deadlocks.
            
    Attributes:
    -----------
    
    :lock:  Synchronizes instance access
    :mlock: locks on (inode, blockno) during `get`, so that we do not
            download the same object with more than one thread.
    
    """

    def __init__(self, bucket, lock, cachedir, max_size, max_entries=768):
        log.debug('Initializing')
        self.cache = OrderedDict()
        self.cachedir = cachedir
        self.max_size = max_size
        self.max_entries = max_entries
        self.size = 0
        self.bucket = bucket
        self.mlock = MultiLock()
        self.lock = lock
        self.removal_queue = ThreadGroup(MAX_REMOVAL_THREADS)
        self.upload_manager = UploadManager(bucket, self.removal_queue)
        self.commit_thread = CommitThread(self)

    def init(self):
        log.debug('init: start')
        if not os.path.exists(self.cachedir):
            os.mkdir(self.cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        self.commit_thread.start()
        log.debug('init: end')

    def destroy(self):
        log.debug('destroy: start')
        self.commit_thread.stop()
        self.clear()
        self.upload_manager.join_all()
        self.removal_queue.join_all()        
        os.rmdir(self.cachedir)
        log.debug('destroy: end')

    def get_bucket_size(self):
        '''Return total size of the underlying bucket'''

        return self.bucket.get_size()

    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.cache)

    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for block `blockno` of `inode`
        
        This method releases the instance `lock` passed in the
        constructor.
        
        Note: if `get` and `remove` are called concurrently, then it
        is possible that a block that has been requested with `get` and
        passed to `remove` for deletion will not be deleted.
        """

        log.debug('get(inode=%d, block=%d): start', inode, blockno)

        if self.size > self.max_size or len(self.cache) > self.max_entries:
            self.expire()

        # Need to release global lock to acquire mlock to prevent deadlocking
        self.lock.release()
        with self.mlock(inode, blockno):
            self.lock.acquire()
            
            try:
                el = self.cache[(inode, blockno)]
    
            # Not in cache
            except KeyError:
                filename = os.path.join(self.cachedir,
                                        'inode_%d_block_%d' % (inode, blockno))
                try:
                    obj_id = dbcm.get_val("SELECT obj_id FROM blocks WHERE inode=? AND blockno=?",
                                          (inode, blockno))
    
                # No corresponding object
                except NoSuchRowError:
                    log.debug('get(inode=%d, block=%d): creating new block', inode, blockno)
                    el = CacheEntry(inode, blockno, None, filename, "w+b")
    
                # Need to download corresponding object
                else:
                    log.debug('get(inode=%d, block=%d): downloading block', inode, blockno)
                    el = CacheEntry(inode, blockno, obj_id, filename, "w+b")
                    with without(self.lock):
                        if self.bucket.read_after_create_consistent():
                            self.bucket.fetch_fh('s3ql_data_%d' % obj_id, el)
                        else:
                            retry_exc(300, [ NoSuchObject ], self.bucket.fetch_fh,
                                      's3ql_data_%d' % obj_id, el)
                    self.size += os.fstat(el.fileno()).st_size
    
                self.cache[(inode, blockno)] = el
    
            # In Cache
            else:
                log.debug('get(inode=%d, block=%d): in cache', inode, blockno)
                self.cache.to_head((inode, blockno))

        
        el.last_access = time.time()
        oldsize = os.fstat(el.fileno()).st_size

        # Provide fh to caller
        try:
            log.debug('get(inode=%d, block=%d): yield', inode, blockno)
            yield el
        finally:
            # Update cachesize
            el.flush()
            newsize = os.fstat(el.fileno()).st_size
            self.size += newsize - oldsize

        log.debug('get(inode=%d, block=%d): end', inode, blockno)


    def expire(self):
        """Perform cache expiry
        
        This method releases the instance `lock` passed in the
        constructor.
        """

        # Note that we have to make sure that the cache entry is written into
        # the database before we remove it from the cache!

        log.debug('expire: start')

        while (len(self.cache) > self.max_entries or
               (len(self.cache) > 0  and self.size > self.max_size)):

            need_size = self.size - self.max_size
            need_entries = len(self.cache) - self.max_entries
            
            # Try to expire entries that are not dirty
            for el in self.cache.values_rev():
                if el.dirty and not (el.inode, el.blockno) in self.upload_manager.in_transit:
                    log.debug('expire: %s is dirty, trying to flush', el)
                    break
                elif el.dirty: # currently in transit
                    continue
                
                del self.cache[(el.inode, el.blockno)]
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
            if not self.upload_manager.upload_in_progress():
                for el in self.cache.values_rev():
                    log.debug('expire: uploading %s..', el)
                    if el.dirty and (el.inode, el.blockno) not in self.upload_manager.in_transit:
                        freed = self.upload_manager.add(el, self.lock)
                        need_size -= freed
                    else:
                        need_size -= os.fstat(el.fileno()).st_size
                    need_entries -= 1                
            
                    if need_size <= 0 and need_entries <= 0:
                        break
                                    
            # Wait for the next entry  
            log.debug('expire: waiting for upload threads..')
            with without(self.lock):
                self.upload_manager.join_one()

        log.debug('expire: end')


    def remove(self, inode, start_no, end_no=None):
        """Remove blocks for `inode`
        
        If `end_no` is not specified, remove just the `start_no` block.
        Otherwise removes all blocks from `start_no` to, but not including,
         `end_no`. 
        
        This method releases the instance `lock` passed in the
        constructor.
        
        Note: if `get` and `remove` are called concurrently, then it
        is possible that a block that has been requested with `get` and
        passed to `remove` for deletion will not be deleted.
        """

        log.debug('remove(inode=%d, start=%d, end=%s): start',
                  inode, start_no, end_no)

        if end_no is None:
            end_no = start_no + 1
            
        for blockno in range(start_no, end_no):
            # We can't use self.mlock here to prevent simultaneous retrieval
            # of the block with get(), because this could deadlock
            if (inode, blockno) in self.cache:
                # Type inference fails here
                #pylint: disable-msg=E1103
                el = self.cache.pop((inode, blockno))

                self.size -= os.fstat(el.fileno()).st_size
                el.close()
                if el.dirty:
                    os.unlink(el.name + '.d')
                else:
                    os.unlink(el.name)

                if el.obj_id is None:
                    log.debug('remove(inode=%d, blockno=%d): block only in cache',
                              inode, blockno)
                    continue

                log.debug('remove(inode=%d, blockno=%d): block in cache and db', inode, blockno)
                obj_id = el.obj_id

            else:
                try:
                    obj_id = dbcm.get_val('SELECT obj_id FROM blocks WHERE inode=? '
                                          'AND blockno = ?', (inode, blockno))
                except NoSuchRowError:
                    log.debug('remove(inode=%d, blockno=%d): block does not exist',
                              inode, blockno)
                    continue

                log.debug('remove(inode=%d, blockno=%d): block only in db ', inode, blockno)

            with dbcm.write_lock() as conn:
                conn.execute('DELETE FROM blocks WHERE inode=? AND blockno=?',
                             (inode, blockno))
                    
                refcount = conn.get_val('SELECT refcount FROM objects WHERE id=?', (obj_id,))
                if refcount > 1:
                    log.debug('remove(inode=%d, blockno=%d): decreasing refcount for object %d',
                              inode, blockno, obj_id)                    
                    conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                                 (obj_id,))
                    to_delete = False
                else:
                    log.debug('remove(inode=%d, blockno=%d): deleting object %d',
                              inode, blockno, obj_id)     
                    conn.execute('DELETE FROM objects WHERE id=?', (obj_id,))
                    to_delete = True
        
            if to_delete:
                with without(self.lock):
                    self.removal_queue.add_thread(RemoveThread(obj_id, self.bucket))

        log.debug('remove(inode=%d, start=%d, end=%s): end',
                  inode, start_no, end_no)

    def flush(self, inode):
        """Flush buffers for `inode`"""

        # Cache entries are automatically flushed after each read()
        # and write()
        pass

    def commit(self):
        """Upload all dirty blocks
        
        When the method returns, all blocks have been registered
        in the database, but the actual uploads may still be 
        in progress.
        
        This method releases the instance `lock` passed in the
        constructor. 
        """
    
        for el in self.cache.itervalues():
            if not el.dirty:
                continue
            
            self.upload_manager.add(el, self.lock)
    
        
    def clear(self):
        """Upload all dirty data and clear cache
        
        When the method returns, all blocks have been registered
        in the database, but the actual uploads may still be 
        in progress.
        
        This method releases the instance `lock` passed in the
        constructor.         
        """

        log.debug('clear: start')
        bak = self.max_entries
        self.max_entries = 0
        self.expire()
        self.max_entries = bak
        log.debug('clear: end')

    def __del__(self):
        if len(self.cache) > 0:
            raise RuntimeError("BlockCache instance was destroyed without calling destroy()!")

class CommitThread(threading.Thread):

    def __init__(self, bcache):
        super(CommitThread, self).__init__()

        self._exc = None
        self._tb = None
        self._joined = False
        self.bcache = bcache
        
        self.stop_event = threading.Event()
        self.name = 'IO Thread'
            
                
    def run(self):
        log.debug('CommitThread: start')
        try:
            while not self.stop_event.is_set():
                did_sth = False
                stamp = time.time()
                for el in self.bcache.cache.values_rev():
                    if stamp - el.last_access < 10:
                        break
                    if (not el.dirty or
                        (el.inode, el.blockno) in self.bcache.upload_manager.in_transit):
                        continue
                            
                    # UploadManager is not threadsafe
                    with self.bcache.lock:
                        if (not el.dirty or # Object may have been accessed
                            (el.inode, el.blockno) in self.bcache.upload_manager.in_transit):
                            continue
                        self.bcache.upload_manager.add(el, self.bcache.lock)
                    did_sth = True
    
                    if self.stop_event.is_set():
                        break
                
                if not did_sth:
                    self.stop_event.wait(5)
                    
        except BaseException as exc:
            self._exc = exc
            self._tb = sys.exc_info()[2] # This creates a circular reference chain
        
        log.debug('CommitThread: end')    
        
    def stop(self):
        '''Wait for thread to finish, raise any occurred exceptions'''
        
        self._joined = True
        
        self.stop_event.set()           
        self.join()
        
        if self._exc is not None:
            # Break reference chain
            tb = self._tb
            del self._tb
            raise EmbeddedException(self._exc, tb, self.name)

    def __del__(self):
        if not self._joined:
            raise RuntimeError("Thread was destroyed without calling stop()!")
        
