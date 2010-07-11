'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from contextlib import contextmanager
from .multi_lock import MultiLock
from .backends.common import NoSuchObject
from .ordered_dict import OrderedDict
from .common import sha256_fh, TimeoutError, EmbeddedException
from .thread_group import ThreadGroup
from . import database as dbcm
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

@contextmanager
def without(lock):
    '''Execute managed block with released lock'''
    
    lock.release()
    try:
        yield
    finally:
        lock.acquire()
        
               
class CacheEntry(file):
    """An element in the block cache
    
    If `obj_id` is `None`, then the object has not yet been
    uploaded to the backend. 
    """

    __slots__ = [ 'dirty', 'obj_id', 'inode', 'blockno', 'last_access' ]

    def __init__(self, inode, blockno, obj_id, filename, mode):
        super(CacheEntry, self).__init__(filename, mode)
        self.dirty = False
        self.obj_id = obj_id
        self.inode = inode
        self.blockno = blockno
        self.last_access = 0

    def truncate(self, *a, **kw):
        if not self.dirty:
            os.rename(self.name, self.name + '.d')
            self.dirty = True
        return super(CacheEntry, self).truncate(*a, **kw)

    def write(self, *a, **kw):
        if not self.dirty:
            os.rename(self.name, self.name + '.d')
            self.dirty = True
        return super(CacheEntry, self).write(*a, **kw)

    def writelines(self, *a, **kw):
        if not self.dirty:
            os.rename(self.name, self.name + '.d')
            self.dirty = True
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
        self.upload_manager = UploadManager(bucket)
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
        self.removal_queue.join_all()
        self.upload_manager.join_all()        
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
                if el.dirty:
                    log.debug('expire: %s is dirty, trying to flush', el)
                    break
                
                del self.cache[(el.inode, el.blockno)]
                size = os.fstat(el.fileno()).st_size
                # We do not close the file, maybe it is still 
                # being uploaded
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
                    if el.dirty:
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
                dbcm.execute('DELETE FROM blocks WHERE inode=? AND blockno=?',
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
                    self.removal_queue.add(lambda : retry_exc(300, [ NoSuchObject ], 
                                                              self.bucket.delete,
                                                              's3ql_data_%d' % obj_id))

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
                    if not el.dirty:
                        continue
    
                    # UploadManager is not threadsafe
                    with self.bcache.lock:
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
                log.debug('UploadManager.add(inode=%d, blockno=%d): created new object %d',
                          el.inode, el.blockno, el.obj_id)

            else:
                need_upload = False
                log.debug('UploadManager.add(inode=%d, blockno=%d): (re)linking to %d',
                          el.inode, el.blockno, el.obj_id)
                conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?',
                             (el.obj_id,))
                
            to_delete = False
            if old_obj_id is None:
                log.debug('UploadManager.add(inode=%d, blockno=%d): no previous object',
                          el.inode, el.blockno)
                conn.execute('INSERT INTO blocks (obj_id, inode, blockno) VALUES(?,?,?)',
                             (el.obj_id, el.inode, el.blockno))    
            else:
                conn.execute('UPDATE blocks SET obj_id=? WHERE inode=? AND blockno=?',
                             (el.obj_id, el.inode, el.blockno))
                refcount = conn.get_val('SELECT refcount FROM objects WHERE id=?',
                                        (old_obj_id,))
                if refcount > 1:
                    log.debug('_UploadManager.add(inode=%d, blockno=%d): '
                              'decreased refcount for prev. obj: %d',
                              el.inode, el.blockno, old_obj_id)
                    conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                                 (old_obj_id,))
                else:
                    log.debug('UploadManager.add(inode=%d, blockno=%d): '
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
                    fn()
                    with self.transit_size_lock:
                        self.transit_size -= size
                    if to_delete:
                        retry_exc(300, [ NoSuchObject ], self.bucket.delete,
                                  's3ql_data_%d' % old_obj_id)

            elif to_delete:
                doit = lambda : retry_exc(300, [ NoSuchObject ], self.bucket.delete,
                                          's3ql_data_%d' % old_obj_id)
                
            # If we already have the minimum transit size, do not start more threads
            while self.transit_size > MIN_TRANSIT_SIZE:
                log.debug('UploadManager.add(%s): waiting for upload thread', el)
                with without(self.lock):
                    self.threads.join_one()

            log.debug('UploadManager.add(%s): starting upload thread', el)
            
            with without(lock):
                self.threads.add(doit)

        log.debug('UploadManager.add(%s): end', el)
        return size

    def join_one(self):
        self.threads.join_one()

    def join_all(self):
        self.threads.join_all()
        
    def upload_in_progress(self):
        return len(self.threads) > 0
