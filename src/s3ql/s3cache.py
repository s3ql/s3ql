'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from contextlib import contextmanager
from llfuse import FUSEError
import errno
from s3ql.multi_lock import MultiLock
from s3ql.ordered_dict import OrderedDict
from s3ql.common import (ExceptionStoringThread, sha256_fh, waitfor)
import logging
import os
import threading
import time
import psyco

 
__all__ = [ "S3Cache" ]

# standard logger for this module
log = logging.getLogger("S3Cache") 


# This is an additional limit on the cache, in addition to the cache size. It prevents that we
# run out of file descriptors, or simply eat up too much memory for cache elements if the users
# creates thousands of 10-byte files.
# Standard file descriptor limit per process is 1024
MAX_CACHE_ENTRIES = 768


class CacheEntry(file):
    """An element in the s3 cache.
    
    Additional Attributes:
    ----------------------

    :dirty:    Has the file been modified
    :s3key:    s3 key
    """    
    
    def __init__(self, s3key, filename, mode):
        super(CacheEntry, self).__init__(filename, mode)
        self.dirty = False
        self.s3key = s3key
        
    def truncate(self, *a, **kw):
        self.dirty = True
        return super(CacheEntry, self).truncate(*a, **kw)
    
    def write(self, *a, **kw):
        self.dirty = True
        return super(CacheEntry, self).write(*a, **kw)       
 
    def writelines(self, *a, **kw):
        self.dirty = True
        return super(CacheEntry, self).writelines(*a, **kw)
    
        
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
    from an s3 object, it uses a context manager provided by the S3Cache
    object which returns a file handle to the s3 object. The S3Cache retrieves
    and stores objects on S3 as necessary. Moreover, it provides
    methods to delete and create s3 objects, once again taking care
    of the necessary locking.

    
    Locking Procedure
    -----------------
    
    Whenever locking is required, first a global lock is acquired. Under
    the global lock, the required s3 key is looked up (or created) in the
    SQLite database. With the s3 key known, a key-specific lock is
    acquired and then the global lock released. 
    
    However, threads may also block when trying to access the database, so
    we have to be especially careful not to hold a database lock when
    we are trying to get a global or s3 key lock (otherwise a 
    deadlock becomes possible).
    
    
    Attributes:
    -----------
    
    Note: None of the attributes may be accessed from outside the class,
    since only the instance methods can provide the required synchronization. 

    :keys:        OrderedDict of keys that are currently in cache
    :bucket:      Bucket object to access AWS
    :maxsize:     Maximum size to which the cache can grow
    :size:        Current size of the cache
    :cachedir:    Where to put the cache files
    :s3_lock:     MultiLock to synchronize access to individual s3 objects
    :global_lock: Global lock    
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
        self.keys = OrderedDict()
        self.cachedir = cachedir
        self.maxsize = cachesize
        self.size = 0
        self.bucket = bucket
        self.s3_lock = MultiLock()
        self.global_lock = threading.Lock()
        self.dbcm = dbcm
        self.timeout = timeout 
        self.expect_mismatch = False  
        self.expiry_lock = threading.Lock()        

        
    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.keys)
    
    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for s3 object backing `inode` at block `blockno`
        
        This may cause other blocks to be expired from the cache in
        separate threads. The caller should therefore not hold any
        database locks when calling `get`.
        """
        # Debug logging commented out, this function is called too often.
        
        # Get s3 key    
        log.debug('Getting file handle for inode %i, block %i', inode, blockno)
        with self.global_lock:
            with self.dbcm() as conn:
                try:
                    s3key = conn.get_val("SELECT s3key FROM blocks WHERE inode=? AND blockno=?", 
                                        (inode, blockno))
                    log.debug('s3key is %s', s3key)
                except KeyError:
                    # Create and add to cache
                    log.debug('creating new s3 object')
                    with conn.transaction():
                        s3key = "s3ql_data_%d_%d" % (inode, blockno) # This is a unique new key
                        conn.execute("INSERT INTO s3_objects (key, refcount) VALUES(?, ?)",
                                     (s3key, 1))
                        conn.execute("INSERT INTO blocks (inode, blockno, s3key) VALUES(?,?,?)",
                                    (inode, blockno, s3key))
                        
                    self.keys[s3key] = CacheEntry(s3key, self.cachedir + s3key, "w+b")

            log.debug('Acquiring object lock')   
            self.s3_lock.acquire(s3key)

       
        # Get s3 object
        try:
            try:
                el = self.keys[s3key]
            except KeyError:
                log.debug('Object %s not cached, retrieving from s3', s3key)
                hash_ = self.dbcm.get_val("SELECT hash FROM s3_objects WHERE key=?", (s3key,))
                el = CacheEntry(s3key, self._download_object(s3key, hash_), 'r+b')
                self.keys[s3key] = el
                
                # Update cache size
                el.seek(0, 2)
                oldsize = el.tell()
                self.size += oldsize
                
            else:
                log.debug('Using cached object')
                self.keys.to_head(s3key)
            
                el.seek(0, 2)
                oldsize = el.tell()
                
            try:
                yield el
            finally:
                # Update cachesize
                el.seek(0, 2)
                newsize = el.tell()
                self.size = self.size - oldsize + newsize 
            
        finally:
            log.debug('Releasing object lock')
            self.s3_lock.release(s3key)
            
        self.expire()    
            
    def _download_object(self, s3key, hash_):
        """Download s3 object into the cache. Return path.
        
        s3_lock must be acquired before this method is called. 
        """

        log.debug('Attempting to download object %s from S3', s3key)
        cachepath = self.cachedir + s3key
        
        waited = 0
        waittime = 0.2
        while waited < self.timeout:
            try:
                meta = self.bucket.lookup_key(s3key)
            except KeyError:
                if not self.expect_mismatch:
                    log.warn("Changes in s3 object %s have not yet propagated. Waiting and retrying...\n"
                             "Try to increase the cache size to avoid this.", s3key)
                log.debug('Key does not exist in S3: %s, waiting...', s3key)
            else:
                if meta['hash'] == hash_:
                    break # Object is ready to be fetched
                
                if not self.expect_mismatch:
                    log.warn("Changes in s3 object %s have not yet propagated. Waiting and retrying...\n"
                             "Try to increase the cache size to avoid this.", s3key)
                
            time.sleep(waittime)
            waited += waittime
            waittime *= 1.5

        # If still not found
        if waited >= self.timeout:
            if not self.expect_mismatch:
                log.warn("Timeout when waiting for propagation of %s in Amazon S3.\n"
                         'Setting a higher timeout with --s3timeout may help.' % s3key)
            raise FUSEError(errno.EIO)
            
        self.bucket.fetch_fh(s3key, open(cachepath, 'wb')) 
        log.debug('Object %s fetched successfully.', s3key)
        
        return cachepath      
        
    def expire(self):
        '''Expire cache. 
        
        Removes entries until the cache size is below self.maxsize.
        Serializes concurrent calls by different threads.
        '''
        
        log.debug('Expiring cache')

        while (self.size > self.maxsize or
               len(self.keys) > MAX_CACHE_ENTRIES):
            with self.expiry_lock:
                # Other threads may have expired enough objects already
                if (self.size > self.maxsize or
                    len(self.keys) > MAX_CACHE_ENTRIES):
                    self._expire_parallel()
            
        log.debug("Expiration end")
    
            
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
        while freed_size < 1024*1024 and len(threads) < 25 and len(self.keys) > 0:
            t = ExpireEntryThread(self)
            t.start()
            t.size_ready.wait()
            freed_size += t.size
            threads.append(t)
            
        self.size -= freed_size
        
        log.debug('Started expiry threads for %d objects, totaling %d kb.',
                  len(threads), freed_size/1024)
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

        # We can share the connection among threads, because
        # we ensure that it is only used by one thread at
        # a time. 
        with self.dbcm() as dbconn:
            threads = list()
            blocks_remaining = True
            while blocks_remaining:
                
                # If there are more than 25 threads, we wait for the
                # first one to finish
                if len(threads) > 25:
                    threads.pop(0).join_and_raise()
                    
                # Start a removal thread
                t = UnlinkBlocksThread(self, dbconn, inode, blockno)
                threads.append(t)            
                t.start()
                
                # Wait until the thread has determined if there are blocks
                # left and copy that information for the while loop.
                t.ready.wait()
                blocks_remaining = t.blocks_remaining
            
        log.debug('Waiting for removal threads...')
        for t in threads:
            t.join_and_raise()
                    
        
    def flush(self, inode):
        """Upload dirty data for `inode`.
        
        """
        
        # It is really unlikely that one inode will several small
        # blocks (the file would have to be terribly fragmented),
        # therefore there is no need to upload in parallel.
        
        # Determine s3 objects from this inode
        log.debug('Flushing objects for inode %i', inode) 
        with self.dbcm() as conn:
            to_flush = [ self.keys[s3key] for (s3key,) 
                        in conn.query("SELECT s3key FROM blocks WHERE inode=?", (inode,))
                        if s3key in self.keys ]
               
        # Flush if required
        for el in to_flush:
            if not el.dirty:
                log.debug('Object %s is not dirty', el.s3key)
                continue
        
            log.info('Flushing object %s', el.s3key)
            
            with self.s3_lock(el.s3key):
                el.flush()    
                el.seek(0)
                hash_ = sha256_fh(el)
                self.bucket.store_fh(el.s3key, el, { 'hash': hash_ })
                self.dbcm.execute("UPDATE s3_objects SET hash=?, size=? WHERE key=?",
                            (hash_, el.tell(), el.s3key))
                el.dirty = False
                
        log.debug('Flushing for inode %d completed.', inode)

    def clear(self):
        """Upload all dirty data and clear cache"""      
         
        log.debug('Clearing S3Cache') 
            
        while len(self.keys) > 0:
            self._expire_parallel()
 
                
    def __del__(self):
        if self.keys:
            raise RuntimeError("s3ql.s3Cache instance was destroyed without calling close()!")
        
        
class ExpireEntryThread(ExceptionStoringThread):
    '''Expire a cache entry. Store the space that is going
       to be freed in self.size, then signal on self.size_ready.
    '''
    
    def __init__(self, s3cache):
        super(ExpireEntryThread, self).__init__(target=self.expire)
        self.size = None
        self.size_ready = threading.Event()
        self.s3cache = s3cache 
        
    def expire(self):
        '''Expire oldest cache entry'''
        
        s3cache = self.s3cache
        log.debug('Expiration thread started.')
        
        with s3cache.global_lock:
            # If we pop the object before having locked it, another thread 
            # may download it - overwriting the existing file!
            try:
                el = s3cache.keys.get_last()
            except IndexError:
                log.debug('No objects in cache. Returning.')
                return
            
            log.debug('Least recently used object is %s, obtaining object lock..', el.s3key)
            s3cache.s3_lock.acquire(el.s3key)  
            
        try:
            try:
                del s3cache.keys[el.s3key]
            except KeyError:
                # Another thread already expired it, we need to try again
                log.debug('Object has already been expired in another thread, returning.')
                return

            el.seek(0, 2)
            self.size = el.tell()
            log.debug('Signaling S3Cache that size is ready to be read.')
            self.size_ready.set()
            
            if el.dirty:
                log.debug('Committing dirty object %s', el.s3key)
                el.seek(0)
                hash_ = sha256_fh(el)
                s3cache.bucket.store_fh(el.s3key, el, { 'hash': hash_ })
                s3cache.dbcm.execute("UPDATE s3_objects SET hash=?, size=? WHERE key=?",
                                     (hash_, self.size, el.s3key))
   
            log.debug('Removing s3 object %s from cache..', el.s3key)
            el.close()   
            os.unlink(el.name)
        finally:
            s3cache.s3_lock.release(el.s3key)
        
        log.debug('Expiration thread finished.')
        
class UnlinkBlocksThread(ExceptionStoringThread):
    '''Removes blocks of given inode 
    
    Removes all blocks >= `blockno`. If a block is the last one
    that referred to an s3 object, the thread sets `blocks_remaining`
    to indicate that there are blocks left to unlink and proceeds
    with removing the s3 object from AWS.
    
    self.ready is used to signal when self.blocks_remaining has
    been set.
    
    The database connection `dbconn` can be reused as soon as
    self.ready is set. In this class, it's usage is also protected
    by s3cache.global_lock.
    '''
    
    def __init__(self, s3cache, dbconn, inode, blockno):
        super(UnlinkBlocksThread, self).__init__(target=self.remove)
        self.ready = threading.Event() # Signals when blocks_remaining has been set
        self.s3cache = s3cache
        self.blocks_remaining = False
        self.inode = inode
        self.blockno = blockno         
        self.dbconn = dbconn
        
    def remove(self):
        cache = self.s3cache
        conn = self.dbconn
        inode = self.inode
        blockno = self.blockno

        try:
            log.debug('Unlinking blocks >= %d from inode %i', blockno, inode)
            with cache.global_lock:
                with conn.transaction():
                    
                    while True: # Loop until we need to delete something from S3
                        try:
                            (s3key, cur_off) = \
                                conn.get_row("SELECT s3key,blockno FROM blocks WHERE inode=? "
                                            "AND blockno >= ? LIMIT 1", (inode, blockno))
                        except KeyError:    # No keys left
                            self.blocks_remaining = False
                            return
                        
                        # Remove from table
                        log.debug('Removing object %s from table', s3key)
                        conn.execute("DELETE FROM blocks WHERE inode=? AND blockno=?", 
                                     (inode, cur_off))
                        (refcount, hash_) = \
                            conn.get_row("SELECT refcount, hash FROM s3_objects WHERE key=?",
                                         (s3key,))
                        
                        refcount -= 1
                        if refcount > 0: # We don't need to remove the s3 object
                            conn.execute("UPDATE s3_objects SET refcount=? WHERE key=?",
                                        (refcount, s3key))
                        elif hash_ is None:
                            # Not committed to S3 yet, need to remove from db only 
                            log.debug('Removing object %s from db and cache', s3key)
                            conn.execute("DELETE FROM s3_objects WHERE key=?", (s3key,))
                            
                            el = cache.keys.pop(s3key, None)
                            if el is not None: # in cache
                                el.seek(0, 2)
                                cache.size -= el.tell()
                                el.close()  
                                os.unlink(el.name) 
                                
                        else:
                            # Need to remove from S3
                            break
                            
                    # Need to delete s3 object
                    self.blocks_remaining = True
                    self.ready.set()
                     
                    conn.execute("DELETE FROM s3_objects WHERE key=?", (s3key,))
                cache.s3_lock.acquire(s3key) # In case another thread wants to recreate this key
                    
            try:
                log.debug('Removing object %s from s3', s3key)
                el = cache.keys.pop(s3key, None)
                if el is not None: # in cache
                    el.seek(0, 2)
                    cache.size -= el.tell()
                    el.close()  
                    os.unlink(el.name) 
                
                # Remove from s3
                try:
                    cache.bucket.delete_key(s3key)
                except KeyError:
                    # Has not propagated yet
                    if not waitfor(cache.timeout, cache.bucket.has_key, s3key):
                        log.warn("Timeout when waiting for propagation of %s in Amazon S3.\n"
                                 'Setting a higher timeout with --s3timeout may help.' % s3key)
                        raise FUSEError(errno.EIO) 
                    cache.bucket.delete_key(s3key)

            finally:
                cache.s3_lock.release(s3key)
        finally:
            # Make sure this is set at the end to prevent deadlocks
            self.ready.set()


                
# Optimize logger calls
psyco.bind(logging.getLogger)        
psyco.bind(logging.Logger.debug)        