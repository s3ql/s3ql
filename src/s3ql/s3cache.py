'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from contextlib import contextmanager
from s3ql.multi_lock import MultiLock
from s3ql.ordered_dict import OrderedDict
from s3ql.common import (ExceptionStoringThread, sha256_fh, TimeoutError)
import logging
import os
import threading
import time
import re

__all__ = [ "S3Cache", 'SynchronizedS3Cache' ]

# standard logger for this module
log = logging.getLogger("S3Cache")


# This is an additional limit on the cache, in addition to the cache size. It prevents that we
# run out of file descriptors, or simply eat up too much memory for cache elements if the users
# creates thousands of 10-byte files.
# Standard file descriptor limit per process is 1024
MAX_CACHE_ENTRIES = 768


class CacheEntry(file):
    """An element in the s3 object cache
    
    If `s3key` is `None`, then the object has not yet been
    uploaded to S3. 
    """

    def __init__(self, inode, blockno, s3key, filename, mode):
        super(CacheEntry, self).__init__(filename, mode)
        self.dirty = False
        self.s3key = s3key
        self.inode = inode
        self.blockno = blockno

    def truncate(self, *a, **kw):
        self.dirty = True
        return super(CacheEntry, self).truncate(*a, **kw)

    def write(self, *a, **kw):
        self.dirty = True
        return super(CacheEntry, self).write(*a, **kw)

    def writelines(self, *a, **kw):
        self.dirty = True
        return super(CacheEntry, self).writelines(*a, **kw)

    def __str__(self):
        return ('<CacheEntry, inode=%d, blockno=%d, dirty=%s, s3key=%r>' %
                (self.inode, self.blockno, self.dirty, self.s3key))

class S3Cache(object):
    """Manages access to s3 objects
    
    Operations on s3 objects need to be synchronized between different threads,
    since otherwise we may
    
    * write into an object that is currently being expired and 
      uploaded and loose the changes
    
    * retrieve the same object twice, where data from the retrieval
      that runs longer overwrites data written after the end of the
      first retrieval.
      
    * read or write at the wrong position, if a different thread has
      moved the file cursor.
 
    For this reason, all operations on s3 files are mediated by
    an S3Cache object. Whenever the file system needs to write or read
    from an s3 object, it uses the `S3Cache.get()` context manager 
    which provides a file handle to the s3 object. The S3Cache retrieves
    and stores objects on S3 as necessary. Moreover, it provides
    methods to delete and create s3 objects, once again taking care
    of the necessary locking.

    
    Locking Procedure
    -----------------
    
    Threads may block when acquiring a Python lock and when trying to
    access the database. To prevent deadlocks, a function must not
    try to acquire any Python lock when it holds a database lock (i.e.,
    is in the middle of a transaction). This has also to be taken
    into account when calling other functions, especially from e.g.
    S3Cache.            
    """

    def __init__(self, bucket, cachedir, maxsize, dbcm):
        log.debug('Initializing')
        self.cache = OrderedDict()
        self.cachedir = cachedir
        self.maxsize = maxsize
        self.size = 0
        self.bucket = bucket
        self.mlock = MultiLock()
        self.dbcm = dbcm

        self.exp_thread = None
        self.need_expiry = threading.Event()
        self.ready_to_write = threading.Event()

    def start_expiration_thread(self):
        '''Start expiration thread'''

        log.debug('Starting background expiration thread')
        self.exp_thread = ExceptionStoringThread(self._expiry_loop, log, pass_self=True)
        self.exp_thread.run_flag = True
        self.exp_thread.start()

    def stop_expiration_thread(self):
        '''Stop background expiration thread'''

        log.debug('Waiting for background expiration thread')
        self.exp_thread.run_flag = False
        if self.exp_thread.is_alive():
            self.need_expiry.set()
        self.exp_thread.join_and_raise()

    def _expiry_loop(self, self_t):
        '''Run cache expiration loop'''

        try:
            while self_t.run_flag:
                log.debug('_expiry_loop: waiting for poke...')
                self.need_expiry.wait()
                log.debug('_expiry_loop: need_expiry has been set')
                while (self.size > 0.85 * self.maxsize or
                       len(self.cache) > 0.85 * MAX_CACHE_ENTRIES) and len(self.cache) > 0:
                    self._expire_parallel()
                    if self.size <= self.maxsize and len(self.cache) <= MAX_CACHE_ENTRIES:
                        log.debug('Cache below threshold, setting flag.')
                        self.ready_to_write.set()
                self.need_expiry.clear()
                self.ready_to_write.set()
        except:
            # Prevent deadlocks
            self.ready_to_write.set()

            def fail():
                raise RuntimeError('Expiration thread quit unexpectedly')
            self.ready_to_write.wait = fail
            self.need_expiry.set = fail

            raise

        log.debug('_expiry_loop: exiting.')

    def get_bucket_size(self):
        '''Return total size of the underlying bucket'''

        return self.bucket.get_size()

    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.cache)

    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for s3 object backing `inode` at block `blockno`
        
        This may cause other blocks to be expired from the cache in
        separate threads. The caller should therefore not hold any
        database locks when calling `get`.
        """

        # Get s3 key    
        log.debug('Getting file handle for inode %i, block %i', inode, blockno)
        with self.mlock(inode, blockno):
            try:
                el = self.cache[(inode, blockno)]

            # Not in cache
            except KeyError:
                filename = os.path.join(self.cachedir, 'inode_%d_block_%d' % (inode, blockno))
                try:
                    s3key = self.dbcm.get_val("SELECT s3key FROM blocks WHERE inode=? AND blockno=?",
                                         (inode, blockno))

                # No corresponding S3 object
                except KeyError:
                    el = CacheEntry(inode, blockno, None, filename, "w+b")
                    oldsize = 0

                # Need to download corresponding S3 object
                else:
                    el = CacheEntry(inode, blockno, s3key, filename, "w+b")
                    retry_exc(300, [ KeyError ], self.bucket.fetch_fh,
                              's3ql_data_%d' % s3key, el)

                    # Update cache size
                    el.seek(0, 2)
                    oldsize = el.tell()
                    self.size += oldsize

                self.cache[(inode, blockno)] = el

            # In Cache
            else:
                self.cache.to_head((inode, blockno))
                el.seek(0, 2)
                oldsize = el.tell()

            # Provide fh to caller
            try:
                yield el
            finally:
                # Update cachesize
                el.seek(0, 2)
                newsize = el.tell()
                self.size = self.size - oldsize + newsize

        # Wait for expiration if required
        if self.size > self.maxsize or len(self.cache) > MAX_CACHE_ENTRIES:
            log.debug('Cache size exceeded, waiting for expiration...')
            self.ready_to_write.clear()
            self.need_expiry.set()
            self.ready_to_write.wait()

        # If more than 85% used, start expiration in background
        elif self.size > 0.85 * self.maxsize or len(self.cache) > 0.85 * MAX_CACHE_ENTRIES:
            log.debug('Cache 85% full, poking expiration thread.')
            self.need_expiry.set()

    def recover(self):
        '''Register old files in cache directory'''

        if self.cache:
            raise RuntimeError('Cannot call recover() if there are already cache entries')

        for filename in os.listdir(self.cachedir):
            match = re.match('^inode_(\\d+)_block_(\\d+)$', filename)
            if match:
                (inode, blockno) = [ int(match.group(i)) for i in (1, 2) ]
                s3key = None

            else:
                raise RuntimeError('Strange file in cache directory: %s' % filename)

            try:
                s3key = self.dbcm.get_val('SELECT s3key FROM blocks WHERE inode=? AND blockno=?',
                                              (inode, blockno))
            except KeyError:
                s3key = None

            el = CacheEntry(inode, blockno, s3key, os.path.join(self.cachedir, filename), "r+b")
            el.dirty = True
            el.seek(0, 2)
            self.size += el.tell()
            self.cache[(inode, blockno)] = el

    def _prepare_upload(self, el):
        '''Prepare upload of specified cache entry
        
        Returns a function that does the required network transactions. Returns
        None if no network access is required.
        
        Caller has to take care of any necessary locking.
        '''

        log.debug('_prepare_upload(inode=%d, blockno=%d)', el.inode, el.blockno)

        if not el.dirty:
            return

        el.seek(0, 2)
        size = el.tell()
        el.seek(0)
        hash_ = sha256_fh(el)

        old_s3key = el.s3key
        with self.dbcm.transaction() as conn:
            try:
                el.s3key = conn.get_val('SELECT id FROM s3_objects WHERE hash=?', (hash_,))

            except KeyError:
                need_upload = True
                el.s3key = conn.rowid('INSERT INTO s3_objects (refcount, hash, size) VALUES(?, ?, ?)',
                                      (1, hash_, size))
                log.debug('No matching hash, will upload to new object %s', el.s3key)

            else:
                need_upload = False
                log.debug('Object %d has identical hash, relinking', el.s3key)
                conn.execute('UPDATE s3_objects SET refcount=refcount+1 WHERE id=?',
                             (el.s3key,))

            if old_s3key is None:
                log.debug('Not associated with any S3 object previously.')
                conn.execute('INSERT INTO blocks (s3key, inode, blockno) VALUES(?,?,?)',
                             (el.s3key, el.inode, el.blockno))
                to_delete = False
            else:
                log.debug('Decreasing reference count for previous s3 object %d', old_s3key)
                conn.execute('UPDATE blocks SET s3key=? WHERE inode=? AND blockno=?',
                             (el.s3key, el.inode, el.blockno))
                refcount = conn.get_val('SELECT refcount FROM s3_objects WHERE id=?',
                                        (old_s3key,))
                if refcount > 1:
                    conn.execute('UPDATE s3_objects SET refcount=refcount-1 WHERE id=?',
                                 (old_s3key,))
                    to_delete = False
                else:
                    conn.execute('DELETE FROM s3_objects WHERE id=?', (old_s3key,))
                    to_delete = True


        if need_upload and to_delete:
            def doit():
                log.debug('Uploading..')
                self.bucket.store_fh('s3ql_data_%d' % el.s3key, el)
                log.debug('No references to object %d left, deleting', old_s3key)
                retry_exc(300, [ KeyError ], self.bucket.delete, 's3ql_data_%d' % old_s3key)
        elif need_upload:
            def doit():
                log.debug('Uploading..')
                self.bucket.store_fh('s3ql_data_%d' % el.s3key, el)
        elif to_delete:
            def doit():
                log.debug('No references to object %d left, deleting', old_s3key)
                retry_exc(300, [ KeyError ], self.bucket.delete, 's3ql_data_%d' % old_s3key)
        else:
            return
        return doit

    def _expire_parallel(self):
        """Remove oldest entries from the cache.
        
        Expires the oldest entries to free at least 1 MB. Expiration is
        done for all the keys at the same time using different threads.
        However, at most 25 threads are started.
        
        The 1 MB is based on the following calculation:
         - Uploading objects takes at least 0.15 seconds due to
           network latency
         - When uploading large objects, maximum throughput is about
           6 MB/sec.
         - Hence the minimum object size for maximum throughput is 
           6 MB/s * 0.15 s ~ 1 MB
         - If the object to be transferred is smaller than that, we have
           to upload several objects at the same time, so that the total
           amount of transferred data is 1 MB.
        """

        log.debug('_expire parallel started')

        threads = list()
        freed_size = 0
        while freed_size < 1024 * 1024 and len(threads) < 25 and len(self.cache) > 0:

            # If we pop the object before having locked it, another thread 
            # may download it - overwriting the existing file!
            try:
                el = self.cache.get_last()
            except IndexError:
                break

            log.debug('Least recently used object is %s, obtaining object lock..', el)
            self.mlock.acquire(el.inode, el.blockno)

            # Now that we have the lock, check that the object still exists
            if (el.inode, el.blockno) not in self.cache:
                self.mlock.release(el.inode, el.blockno)
                continue

            # Make sure the object is in the db before removing it from the cache
            fn = self._prepare_upload(el)
            log.debug('Removing s3 object %s from cache..', el)
            del self.cache[(el.inode, el.blockno)]
            el.seek(0, os.SEEK_END)
            freed_size += el.tell()

            if fn is None:
                log.debug('expire_parallel: no network transaction required')
                el.close()
                os.unlink(el.name)
                self.mlock.release(el.inode, el.blockno)
            else:
                log.debug('expire_parallel: starting new thread for network transaction')
                # We have to be careful to include the *current*
                # el in the closure
                def do_upload(el=el, fn=fn):
                    fn()
                    el.close()
                    os.unlink(el.name)
                    self.mlock.release(el.inode, el.blockno)

                t = ExceptionStoringThread(do_upload, log)
                threads.append(t)
                t.start()

        self.size -= freed_size

        log.debug('Freed %d kb using %d expiry threads', freed_size / 1024, len(threads))
        log.debug('Waiting for expiry threads...')
        for t in threads:
            t.join_and_raise()
            #t.join()

        log.debug('_expire_parallel finished')


    def remove(self, inode, blockno=0):
        """Unlink blocks of given inode.
        
        If `blockno` is specified, unlinks only s3 objects for blocks
        >= `blockno`. If no other blocks reference the s3 objects,
        they are completely removed.
        
        As long as no s3 objects need to be removed, blocks are processed
        sequentially. If an s3 object needs to be removed, a new thread
        continues to process the remaining blocks in parallel.
        """

        log.debug('Removing blocks >= %d for inode %d', blockno, inode)

        # Remove elements from cache
        log.debug('Iterating through cache')
        for el in self.cache.itervalues():
            if el.inode != inode:
                continue
            if el.blockno < blockno:
                continue

            log.debug('Found block %d, removing', el.blockno)
            with self.mlock(el.inode, el.blockno):
                try:
                    self.cache.pop((el.inode, el.blockno))
                except KeyError:
                    log.debug('Object has already been expired.')
                    continue

            el.seek(0, 2)
            self.size -= el.tell()
            el.close()
            os.unlink(el.name)

        # Remove elements from db and S3
        # TODO: At this point we may miss objects that are in the process of
        # being uploaded, i.e. already out of the cache but not yet in the db
        log.debug('Deleting from database')
        threads = list()
        while True:
            with self.dbcm.transaction() as conn:
                try:
                    (s3key, cur_block) = conn.get_row('SELECT s3key, blockno FROM blocks '
                                                      'WHERE inode=? AND blockno >= ? LIMIT 1',
                                                      (inode, blockno))
                except KeyError:
                    break

                log.debug('Deleting block %d, s3 key %d', cur_block, s3key)
                conn.execute('DELETE FROM blocks WHERE inode=? AND blockno=?', (inode, cur_block))
                refcount = conn.get_val('SELECT refcount FROM s3_objects WHERE id=?', (s3key,))
                if refcount > 1:
                    log.debug('Decreasing refcount for s3 object %d', s3key)
                    conn.execute('UPDATE s3_objects SET refcount=refcount-1 WHERE id=?', (s3key,))
                    continue

                log.debug('Deleting s3 object %d', s3key)
                conn.execute('DELETE FROM s3_objects WHERE id=?', (s3key,))

            # Note that at this point we must make sure that any new s3 objects 
            # don't reuse the key that we have just deleted from the DB. This
            # is ensured by using AUTOINCREMENT on the id column.

            # If there are more than 25 threads, we wait for the
            # first one to finish
            if len(threads) > 25:
                log.debug('More than 25 threads, waiting..')
                threads.pop(0).join_and_raise()

            # Start a removal thread              
            t = ExceptionStoringThread(retry_exc, log,
                                       args=(300, [ KeyError ], self.bucket.delete,
                                             's3ql_data_%d' % s3key))
            threads.append(t)
            t.start()


        log.debug('Waiting for removal threads...')
        for t in threads:
            t.join_and_raise()


    def flush(self, inode):
        """Upload dirty data for `inode`"""

        # It is really unlikely that one inode will several small
        # blocks (the file would have to be terribly fragmented),
        # therefore there is no need to upload in parallel.

        log.debug('Flushing objects for inode %i', inode)
        for el in self.cache.itervalues():
            if el.inode != inode:
                continue
            if not el.dirty:
                continue

            log.debug('Flushing object %s', el)
            with self.mlock(el.inode, el.blockno):
                # Now that we have the lock, check that the object still exists
                if (el.inode, el.blockno) not in self.cache:
                    continue

                fn = self._prepare_upload(el)
                if fn:
                    fn()
                    el.dirty = False

        log.debug('Flushing for inode %d completed.', inode)

    def flush_all(self):
        """Upload all dirty data"""

        # It is really unlikely that one inode will several small
        # blocks (the file would have to be terribly fragmented),
        # therefore there is no need to upload in parallel.

        log.debug('Flushing all objects')
        for el in self.cache.itervalues():
            if not el.dirty:
                continue

            log.debug('Flushing object %s', el)
            with self.mlock(el.inode, el.blockno):
                # Now that we have the lock, check that the object still exists
                if (el.inode, el.blockno) not in self.cache:
                    continue

                fn = self._prepare_upload(el)
                if fn:
                    fn()
                    el.dirty = False
    def clear(self):
        """Upload all dirty data and clear cache"""

        log.debug('Clearing S3Cache')

        if self.exp_thread and self.exp_thread.is_alive():
            bak = self.maxsize
            self.maxsize = 0
            try:
                self.ready_to_write.clear()
                self.need_expiry.set()
                self.ready_to_write.wait()
            finally:
                self.maxsize = bak
        else:
            while len(self.cache) > 0:
                self._expire_parallel()


    def __del__(self):
        if len(self.cache) > 0:
            raise RuntimeError("s3ql.S3Cache instance was destroyed without calling clear()!")


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



