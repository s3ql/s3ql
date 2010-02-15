'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from contextlib import contextmanager
from s3ql.multi_lock import MultiLock
from s3ql.ordered_dict import OrderedDict
from s3ql.common import (ExceptionStoringThread, sha256_fh, EmbeddedException, retry_exc)
import logging
import os
import threading
import time
import sys
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

    
    Attributes:
    -----------
    
    Note: None of the attributes may be accessed from outside the class,
    since only the instance methods can provide the required synchronization. 

    :cache:       `OrderedDict` of `CacheEntry` instances
    :mlock:       `MultiLock` instance for locking on ``(inode, blockno)`` tuples
    :maxsize:     Maximum size to which the cache can grow
    :size:        Current size of the cache
    :timeout:     Maximum time to wait for changes in S3 to propagate
    :dbcm:        ConnectionManager instance, to manage access to the database
                  from different threads
    :expiry_lock: Serializes calls to expire()
                  
    The `expect_mismatch` attribute is only for unit testing instrumentation
    and suppresses warnings if a mismatch between local and remote hash
    is encountered.                   
    """

    def __init__(self, bucket, cachedir, cachesize, dbcm, timeout=60):
        log.debug('Initializing')
        self.cache = OrderedDict()
        self.cachedir = cachedir
        self.maxsize = cachesize
        self.size = 0
        self.bucket = bucket
        self.mlock = MultiLock()
        self.dbcm = dbcm
        self.timeout = timeout
        self.expect_mismatch = False
        self.expiry_lock = threading.Lock()
        self.exp_thread = None

    def start_background_expiration(self):
        '''Start background expiration thread.
        
        This thread will try to keep the cache size less than
        85% of the maximum.
        '''

        self.exp_thread = BackgroundExpirationThread(self, int(0.85 * self.maxsize),
                                                     int(0.85 * MAX_CACHE_ENTRIES))
        self.exp_thread.start()

    def stop_background_expiration(self):
        '''Stop background expiration thread'''

        t = self.exp_thread
        t.keep_running = False
        log.debug('Waiting for background expiration thread')
        t.join()
        if t.exc is not None:
            # Break reference chain
            tb = t.tb
            del t.tb
            raise EmbeddedException(t.exc, tb, t.name)

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

        # TODO: Instead of calling expire, we should wait for
        # the permanent expiration thread to have reduced
        # the cache size sufficiently.
        self.expire(self.maxsize, MAX_CACHE_ENTRIES)


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

    def expire(self, max_size, max_files):
        '''Expire cache. 
        
        Removes entries until the cache size is below `max_size` 
        Serializes concurrent calls by different threads.
        '''

        while (self.size > max_size or
               len(self.cache) > max_files):
            with self.expiry_lock:
                # Other threads may have expired enough objects already
                if (self.size > max_size or
                    len(self.cache) > max_files):
                    self._expire_parallel()


    def _upload_object(self, el):
        '''Upload specified cache entry
        
        Caller has to take care of any necessary locking.
        '''

        log.debug('_upload_object(inode=%d, blockno=%d)', el.inode, el.blockno)
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

        if need_upload:
            log.debug('Uploading..')
            self.bucket.store_fh('s3ql_data_%d' % el.s3key, el)

        if to_delete:
            log.debug('No references to object %d left, deleting', old_s3key)
            retry_exc(300, [ KeyError ], self.bucket.delete_key, 's3ql_data_%d' % old_s3key)


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

        # TODO: We really want to do compression/encryption and upload in parallel.
        # This is probably best implemented by moving the entire _expire_parallel
        # function into the dedicated expiration thread. It can then sequentially compress,
        # and upload in a separate thread while compressing the next object, while
        # also keeping track of how many uploads we are doing at the same time. 

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

            try:
                del self.cache[(el.inode, el.blockno)]
            except KeyError:
                log.debug('Object has already been expired in another thread')
                self.mlock.release(el.inode, el.blockno)
                continue

            log.debug('Removing s3 object %s from cache..', el)
            el.seek(0, 2)
            freed_size += el.tell()

            if not el.dirty:
                el.close()
                os.unlink(el.name)
                self.mlock.release(el.inode, el.blockno)
                continue

            # We have to be careful to include the *current*
            # el in the closure
            def do_upload(el=el):
                self._upload_object(el)
                el.close()
                os.unlink(el.name)
                self.mlock.release(el.inode, el.blockno)

            t = ExceptionStoringThread(do_upload)
            threads.append(t)
            t.start()

        self.size -= freed_size

        log.debug('Freed %d kb using %d expiry threads', freed_size / 1024, len(threads))
        log.debug('Waiting for expiry threads...')
        for t in threads:
            t.join_and_raise()

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
                    log.debug('Already removed by different thread')
                    continue

            el.seek(0, 2)
            self.size -= el.tell()
            el.close()
            os.unlink(el.name)

        # Remove elements from db and S3
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
            t = ExceptionStoringThread(retry_exc,
                                       args=(self.bucket.delete_key, 's3ql_data_%d' % s3key))
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
                self._upload_object(el)
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
                self._upload_object(el)
                el.dirty = False

    def clear(self):
        """Upload all dirty data and clear cache"""

        log.debug('Clearing S3Cache')

        while len(self.cache) > 0:
            self._expire_parallel()


    def __del__(self):
        if len(self.cache) > 0:
            raise RuntimeError("s3ql.S3Cache instance was destroyed without calling clear()!")


class BackgroundExpirationThread(threading.Thread):

    def __init__(self, cache, max_size, max_files):
        super(BackgroundExpirationThread, self).__init__(name='Expiry-Thread')
        self.keep_running = True
        self.cache = cache
        self.max_size = max_size
        self.max_files = max_files
        self.exc = None
        self.tb = None
        self.daemon = True #threading.thread attribute

    def run(self):
        log.debug('Starting background expiration thread')
        try:
            while self.keep_running:
                self.cache.expire(self.max_size, self.max_files)
                time.sleep(1)
        except BaseException as exc:
            self.exc = exc
            self.tb = sys.exc_info()[2] # This creates a circular reference chain


class SynchronizedS3Cache(S3Cache):
    # Argument number difffers from overridden method
    #pylint: disable-msg=W0221

    def __init__(self, *a, **kw):
        super(SynchronizedS3Cache, self).__init__(*a, **kw)
        self.lock = threading.RLock()

    def clear(self, *a, **kw):
        with self.lock:
            return super(SynchronizedS3Cache, self).clear(*a, **kw)

    def expire(self, *a, **kw):
        with self.lock:
            return super(SynchronizedS3Cache, self).expire(*a, **kw)

    def flush(self, *a, **kw):
        with self.lock:
            return super(SynchronizedS3Cache, self).flush(*a, **kw)

    @contextmanager
    def get(self, *a, **kw):
        with self.lock:
            with super(SynchronizedS3Cache, self).get(*a, **kw) as fh:
                yield fh

    def get_bucket_size(self, *a, **kw):
        with self.lock:
            return super(SynchronizedS3Cache, self).get_bucket_size(*a, **kw)

    def remove(self, *a, **kw):
        with self.lock:
            return super(SynchronizedS3Cache, self).remove(*a, **kw)
