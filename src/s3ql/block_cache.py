'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from contextlib import contextmanager
from .multi_lock import MultiLock
from .ordered_dict import OrderedDict
from .common import sha256_fh, TimeoutError, EmbeddedException
from .thread_group import ThreadGroup
from . import database as dbcm
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

class BlockCache(object):
    """Provides access to file blocks
    
    This class manages access to file blocks. It takes care of creation,
    uploading, downloading and deduplication.
    
    In order for S3QL not to block entirely when objects need to be
    downloaded or uploaded, this class releases the global lock for
    network transactions. In these cases, a separate lock on inode and
    block number is used to prevent simultaneous access to the same block.
    """

    def __init__(self, bucket, cachedir, max_size, max_entries=768):
        log.debug('Initializing')
        self.cache = OrderedDict()
        self.cachedir = cachedir
        self.max_size = max_size
        self.max_entries = max_entries
        self.size = 0
        self.bucket = bucket
        self.mlock = MultiLock()
        self.removal_queue = RemovalQueue(self)
        self.upload_queue = UploadQueue(self)
        self.io_thread = IOThread(self)
        self.expiry_lock = threading.Lock()

    def init(self):
        log.debug('init: start')
        if not os.path.exists(self.cachedir):
            os.mkdir(self.cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        self.io_thread.start()
        log.debug('init: end')

    def destroy(self):
        log.debug('destroy: start')
        self.io_thread.stop()    
        self.clear()
        self.removal_queue.wait()
        self.upload_queue.wait()        
        os.rmdir(self.cachedir)
        log.debug('destroy: end')

    def _remove_entry(self, el):
        '''Try to remove `el' from cache
        
        The entry is only removed if it is not dirty.
        
        Both conditions are checked after the block has been locked and
        retrieved again from the cache. 
        '''

        with self.mlock(el.inode, el.blockno):
            try:
                el = self.cache[(el.inode, el.blockno)]
            except KeyError:
                log.debug('_remove_entry(%s): end (vanished)', el)
                return
            if el.dirty:
                log.debug('_remove_entry(%s): end (dirty)', el)
                return

            log.debug('_remove_entry(%s): removing from cache', el)
            del self.cache[(el.inode, el.blockno)]
            self.size -= os.fstat(el.fileno()).st_size
            el.close()
            os.unlink(el.name)

        log.debug('_remove_entry(%s): end', el)


    def get_bucket_size(self):
        '''Return total size of the underlying bucket'''

        return self.bucket.get_size()

    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.cache)

    @contextmanager
    def get(self, inode, blockno, lock):
        """Get file handle for block `blockno` of `inode`
        
        This method releases `lock' for the managed context, so the caller must
        not hold any prior database locks and must not try to acquire any
        database locks in the managed block.
        """

        log.debug('get(inode=%d, block=%d): start', inode, blockno)

        lock.release()
        if self.size > self.max_size or len(self.cache) > self.max_entries:
            self._expire()
        self.mlock.acquire(inode, blockno)

        try:
            el = self._get(inode, blockno)
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

        finally:
            self.mlock.release(inode, blockno)
            lock.acquire()

        log.debug('get(inode=%d, block=%d): end', inode, blockno)


    def _get(self, inode, blockno):
        log.debug('_get(inode=%d, block=%d): start', inode, blockno)

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
            except KeyError:
                log.debug('get(inode=%d, block=%d): creating new block', inode, blockno)
                el = CacheEntry(inode, blockno, None, filename, "w+b")

            # Need to download corresponding object
            else:
                log.debug('get(inode=%d, block=%d): downloading block', inode, blockno)
                el = CacheEntry(inode, blockno, obj_id, filename, "w+b")
                retry_exc(300, [ KeyError ], self.bucket.fetch_fh,
                          's3ql_data_%d' % obj_id, el)
                self.size += os.fstat(el.fileno()).st_size

            self.cache[(inode, blockno)] = el

        # In Cache
        else:
            log.debug('get(inode=%d, block=%d): in cache', inode, blockno)
            self.cache.to_head((inode, blockno))

        el.last_access = time.time()

        log.debug('get(inode=%d, block=%d): end', inode, blockno)
        return el

    def _expire(self):
        """Perform cache expiry"""

        # Note that we have to make sure that the cache entry is written into
        # the database before we remove it from the cache!

        log.debug('_expire: start')

        with self.expiry_lock:
            while (len(self.cache) > self.max_entries or
                   (len(self.cache) > 0  and self.size > self.max_size)):

                # Try to expire entries that are not dirty
                for el in self.cache.values_rev():
                    if el.dirty:
                        log.debug('_expire: %s is dirty, trying to flush', el)
                        break
                    self._remove_entry(el)
     
                need_size = self.size - self.max_size
                need_entries = len(self.cache) - self.max_entries
                if need_size <= 0 and need_entries <= 0:
                    break
                         
                # If there are no entries in the queue, add just enough
                if self.upload_queue.is_empty():
                    for el in self.cache.values_rev():
                        log.debug('_expire: adding %s to queue', el)
                        if el.dirty:
                            need_size -= self.upload_queue.add(el)
                        else:
                            need_size -= os.fstat(el.fileno()).st_size
                        need_entries -= 1                
                
                        if need_size < 0 and need_entries < 0:
                            break
                                        
                # Wait for the next entry  
                log.debug('_expire: waiting for queue')
                self.upload_queue.wait_for_thread()

        log.debug('_expire: end')


    def remove(self, inode, lock, start_no, end_no=None):
        """Remove blocks for `inode`
        
        If `end_no` is not specified, remove just the `start_no` block.
        Otherwise removes all blocks from `start_no` to, but not including,
         `end_no`. 
        
        This method releases `lock' for the managed context, so the caller must
        not hold any prior database locks and must not try to acquire any
        database locks in the managed context.
        """

        log.debug('remove(inode=%d, start=%d, end=%s): start',
                  inode, start_no, end_no)

        lock.release()
        
        if end_no is None:
            end_no = start_no + 1
            
        try:
            for blockno in range(start_no, end_no):
                with self.mlock(inode, blockno):
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
                        except KeyError:
                            log.debug('remove(inode=%d, blockno=%d): block does not exist',
                                      inode, blockno)
                            continue
    
                        log.debug('remove(inode=%d, blockno=%d): block only in db ', inode, blockno)
    
                    dbcm.execute('DELETE FROM blocks WHERE inode=? AND blockno=?',
                                      (inode, blockno))
                self.removal_queue.add(obj_id)

        finally:
            lock.acquire()

        log.debug('remove(inode=%d, start=%d, end=%s): end',
                  inode, start_no, end_no)

    def flush(self, inode):
        """Flush buffers for `inode`"""

        log.debug('flush(inode=%d): start', inode)

        for el in self.cache.itervalues():
            if el.inode != inode:
                continue
            if not el.dirty:
                continue
            
            el.flush()
            
        log.debug('flush(inode=%d): end', inode)

    def flush_all(self):
        """Flush buffers for all cached blocks"""

        log.debug('flush_all: start')

        for el in self.cache.itervalues():
            if not el.dirty:
                continue

            el.flush()

        log.debug('flush_all: end')
        
    def clear(self):
        """Upload all dirty data and clear cache"""

        log.debug('clear: start')
        bak = self.max_size
        # max_size=0 is not sufficient, that would keep entries with 0 size
        self.max_size = -1
        self._expire()
        self.max_size = bak
        log.debug('clear: end')

    def __del__(self):
        if len(self.cache) > 0:
            raise RuntimeError("BlockCache instance was destroyed without calling close()!")

class IOThread(threading.Thread):

    def __init__(self, bcache):
        super(IOThread, self).__init__()

        self._exc = None
        self._tb = None
        self._joined = False
        self.bcache = bcache
        
        self.stop_event = threading.Event()
        self.name = 'IO Thread'
            
    def run_protected(self):
        log.debug('IOThread: start')

        while not self.stop_event.is_set():
            did_sth = False
            stamp = time.time()
            for el in self.bcache.cache.values_rev():
                if stamp - el.last_access < 10:
                    break
                if not el.dirty:
                    continue

                self.bcache.upload_queue.add(el)
                did_sth = True

                if self.stop_event.is_set():
                    break            
            
            if not did_sth:
                self.stop_event.wait(5)

        log.debug('IOThread: end')       
                
    def run(self):
        try:
            self.run_protected()
        except BaseException as exc:
            self._exc = exc
            self._tb = sys.exc_info()[2] # This creates a circular reference chain

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
class UploadQueue(object):
    '''
    Schedules and executes object uploads to make optimum usage
    network bandwidth and CPU time.
    '''

    def __init__(self, bcache):
        self.threads = ThreadGroup(MAX_UPLOAD_THREADS)
        self.bcache = bcache
        self.transit_size = 0
        
    def add(self, el):
        '''Upload cache entry `el`
        
        Returns size of cache entry. This function may block if the queue is already full.
        '''

        log.debug('UploadQueue.add(%s): start', el)

        mlock = self.bcache.mlock
        mlock.acquire(el.inode, el.blockno)
        mlock_released = False
        try:
            # Now that we have the lock, check that the object still exists
            if (el.inode, el.blockno) not in self.bcache.cache:
                mlock.release(el.inode, el.blockno)
                mlock_released = True
                log.debug('UploadQueue.add(%s): end (entry has vanished)', el)
                return 0

            size = os.fstat(el.fileno()).st_size
            if not el.dirty:
                mlock.release(el.inode, el.blockno)
                mlock_released = True
                log.debug('UploadQueue.add(%s): end (entry not dirty)', el)
                return size

            log.debug('UploadQueue.add(%s): preparing upload', el)
            fn = self._prepare_upload(el)
            
            if fn:
                # If we already have the minimum transit size, do not start more threads
                while self.transit_size > MIN_TRANSIT_SIZE:
                    log.debug('UploadQueue.add(%s): waiting for upload thread', el)
                    self.threads.join_one()
    
                log.debug('UploadQueue.add(%s): starting upload thread', el)
                def _do():
                    try:
                        log.debug('UploadQueue.add(%s): uploading...', el)
                        fn()
                        el.dirty = False
                        os.rename(el.name + '.d', el.name)
                        self.transit_size -= size
                        log.debug('UploadQueue.add(%s): upload complete.', el)
                    finally:
                        mlock.release(el.inode, el.blockno)
                self.transit_size += size
                self.threads.add(_do)
                mlock_released = True
            else:
                log.debug('UploadQueue.add(%s): no upload required', el)
                el.dirty = False
                os.rename(el.name + '.d', el.name)
                mlock.release(el.inode, el.blockno)
                mlock_released = True


        except:
            if not mlock_released:
                mlock.release(el.inode, el.blockno)
            raise

        log.debug('UploadQueue.add(%s): end', el)
        return size

    def wait_for_thread(self):
        self.threads.join_one()

    def wait(self):
        self.threads.join_all()
        
    def is_empty(self):
        return len(self.threads) == 0

    def _prepare_upload(self, el):
        '''Prepare upload of specified cache entry
        
        Returns a function that does the required network transactions. Returns
        None if no network access is required.
        
        Caller has to take care of any necessary locking.
        '''

        log.debug('UploadQueue._prepare_upload(inode=%d, blockno=%d): start',
                  el.inode, el.blockno)

        size = os.fstat(el.fileno()).st_size

        el.seek(0)
        hash_ = sha256_fh(el)
        old_obj_id = el.obj_id
        with dbcm.write_lock() as conn:
            try:
                el.obj_id = conn.get_val('SELECT id FROM objects WHERE hash=?', (hash_,))

            except KeyError:
                need_upload = True
                el.obj_id = conn.rowid('INSERT INTO objects (refcount, hash, size) VALUES(?, ?, ?)',
                                      (1, hash_, size))
                log.debug('UploadQueue._prepare_upload(inode=%d, blockno=%d): created new object %d',
                          el.inode, el.blockno, el.obj_id)

            else:
                need_upload = False
                log.debug('UploadQueue._prepare_upload(inode=%d, blockno=%d): (re)linking to %d',
                          el.inode, el.blockno, el.obj_id)
                conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?',
                             (el.obj_id,))

            if old_obj_id is None:
                log.debug('UploadQueue._prepare_upload(inode=%d, blockno=%d): no previous object',
                          el.inode, el.blockno)
                conn.execute('INSERT INTO blocks (obj_id, inode, blockno) VALUES(?,?,?)',
                             (el.obj_id, el.inode, el.blockno))
                to_delete = False
            else:
                conn.execute('UPDATE blocks SET obj_id=? WHERE inode=? AND blockno=?',
                             (el.obj_id, el.inode, el.blockno))
                refcount = conn.get_val('SELECT refcount FROM objects WHERE id=?',
                                        (old_obj_id,))
                if refcount > 1:
                    log.debug('_UploadQueue.prepare_upload(inode=%d, blockno=%d): '
                              'decreased refcount for prev. obj: %d',
                              el.inode, el.blockno, old_obj_id)
                    conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                                 (old_obj_id,))
                    to_delete = False
                else:
                    log.debug('UploadQueue._prepare_upload(inode=%d, blockno=%d): '
                              'prev. obj %d marked for removal',
                              el.inode, el.blockno, old_obj_id)
                    conn.execute('DELETE FROM objects WHERE id=?', (old_obj_id,))
                    to_delete = True


        if need_upload:
            fn = self.bcache.bucket.prep_store_fh('s3ql_data_%d' % el.obj_id, el)
            if to_delete:
                def doit():
                    fn()
                    retry_exc(300, [ KeyError ], self.bcache.bucket.delete,
                              's3ql_data_%d' % old_obj_id)
            else:
                doit = fn
        elif to_delete:
            doit = lambda : retry_exc(300, [ KeyError ], self.bcache.bucket.delete,
                                      's3ql_data_%d' % old_obj_id)
        else:
            doit = None

        log.debug('UploadQueue._prepare_upload(inode=%d, blockno=%d): end',
                  el.inode, el.blockno)
        return doit
    
    
MAX_REMOVAL_THREADS = 25
class RemovalQueue(object):
    '''
    Schedules and executes object removals to make optimum usage network
    bandwith.
    '''

    def __init__(self, bcache):
        self.threads = ThreadGroup(MAX_REMOVAL_THREADS)
        self.bcache = bcache

    def add(self, obj_id):
        '''Remove object obj_id
        
        This function may block if the queue is already full, otherwise it
        returns immediately while the removal proceeds in the background.
        '''

        log.debug('RemovalQueue.add(%s): start', obj_id)

 
        fn = self._prepare_removal(obj_id)
        
        if fn:
            log.debug('RemovalQueue.add(%s): starting removal thread', obj_id)
            self.threads.add(fn)
        else:
            log.debug('RemovalQueue.add(%s): no retwork transaction required', obj_id)

        log.debug('RemovalQueue.add(%s): end', obj_id)

    def wait_for_thread(self):
        self.threads.join_one()

    def wait(self):
        self.threads.join_all()

    def _prepare_removal(self, obj_id):
        '''Prepare removal of specified object
        
        Returns a function that does the required network transactions. Returns
        None if no network access is required.
        '''

        log.debug('RemovalQueue._prepare_remval(%d): start', obj_id)

        with dbcm.write_lock() as conn:
            refcount = conn.get_val('SELECT refcount FROM objects WHERE id=?',
                                    (obj_id,))
            if refcount > 1:
                log.debug('RemovalQueue._prepare_removal(%d): decreased refcount', obj_id)
                conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                             (obj_id,))
                to_delete = False
            else:
                log.debug('RemovalQueue._prepare_remval(%d): refcount reached 0', obj_id)
                conn.execute('DELETE FROM objects WHERE id=?', (obj_id,))
                to_delete = True

        if to_delete:
            doit = lambda : retry_exc(300, [ KeyError ], self.bcache.bucket.delete,
                                      's3ql_data_%d' % obj_id)
        else:
            doit = None

        log.debug('RemovalQueue._prepare_remval(%d): end', obj_id)
        return doit    
