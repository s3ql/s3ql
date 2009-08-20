#
#    Copyright (C) 2008-2009  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
from contextlib import contextmanager
from s3ql import fs
from s3ql.multi_lock import MultiLock
from s3ql.ordered_dict import OrderedDict
from s3ql.common import ExceptionStoringThread
import errno
import logging
import os
import threading
import time

# Pylint has trouble to recognise the type of elements in the OrderedDict
#pylint: disable-msg=E1103
 
__all__ = [ "S3Cache" ]

# standard logger for this module
log = logging.getLogger("S3Cache") 


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
    sqlite database. With the s3 key known, a key-specific lock is
    acquired and then the global lock released. 
    
    
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
                  
    
    The `expect_mismatch` attribute is only for unit testing instrumentation
    and suppresses warnings if a mismatch between local and remote etag
    is encountered.                   
    
    
    Cache Management
    ----------------
    
    Note that every s3 object in the cache occupies a file
    descriptor. The maximum number of objects in the cache is
    therefore hardcoded to 300 (so that we do not run out of
    file descriptors). If the block size of the filesystem is
    small, and the cache size is large, it can therefore happen
    that the cache is expired before it reaches maximum size.
    """
    
    def __init__(self, bucket, cachedir, cachesize, dbcm, timeout=60):
        log.debug('Initializing')
        self.keys = OrderedDict()
        self.cachedir = cachedir
        self.maxsize = cachesize
        self.size = 0
        self.bucket = bucket
        self.s3_lock = MultiLock()
        self.global_lock = threading.RLock()
        self.dbcm = dbcm
        self.timeout = timeout 
        self.expect_mismatch = False          

        
    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for s3 object backing `inode` at block `blockno`
        
        """
                 
        # Get s3 key    
        #log.debug('Getting file handle for inode %i, block %i', inode, blockno)
        with self.global_lock:
            with self.dbcm() as conn:
                try:
                    s3key = conn.get_val("SELECT s3key FROM blocks WHERE inode=? AND blockno=?", 
                                        (inode, blockno))
                    #log.debug('s3key is %s', s3key)
                except StopIteration:
                    # Create and add to cache
                    log.debug('creating new s3 object')
                    with conn.transaction():
                        s3key = "s3ql_data_%d_%d" % (inode, blockno) # This is a unique new key
                        conn.execute("INSERT INTO s3_objects (key, refcount) VALUES(?, ?)",
                                     (s3key, 1))
                        conn.execute("INSERT INTO blocks (inode, blockno, s3key) VALUES(?,?,?)",
                                    (inode, blockno, s3key))
                        
                    self.keys[s3key] = CacheEntry(s3key, self.cachedir + s3key, "w+b")

            #log.debug('Acquiring object lock')   
            self.s3_lock.acquire(s3key)

       
        # Get s3 object
        try:
            try:
                el = self.keys[s3key]
            except KeyError:
                log.debug('Object %s not cached, retrieving from s3', s3key)
                etag = self.dbcm.get_val("SELECT etag FROM s3_objects WHERE key=?", (s3key,))
                el = CacheEntry(s3key, self._download_object(s3key, etag), 'r+b')
                self.keys[s3key] = el
                
                # Update cache size
                el.seek(0, 2)
                oldsize = el.tell()
                self.size += oldsize
                
            else:
                #log.debug('Using cached object')
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
            #log.debug('Releasing object lock')
            self.s3_lock.release(s3key)
            
        self.expire()    
            
    def _download_object(self, s3key, etag):
        """Download s3 object from amazon into the cache. Return path.
        
        Not synchronized. 
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
                if meta.etag == etag:
                    break # Object is ready to be fetched
                
                if not self.expect_mismatch:
                    log.warn("Changes in s3 object %s have not yet propagated. Waiting and retrying...\n"
                             "Try to increase the cache size to avoid this.", s3key)
                log.debug('Stored etag: %s, Received etag: %s', repr(etag), repr(meta.etag))
                
            time.sleep(waittime)
            waited += waittime
            waittime *= 1.5

        # If still not found
        if waited >= self.timeout:
            if not self.expect_mismatch:
                log.error("Timeout when waiting for propagation of %s!" 
                          "Filesystem is probably corrupted (or S3 is having problems), "
                          "run fsck.s3ql as soon as possible.", s3key)
            raise fs.FUSEError(errno.EIO, fatal=True)
            
        self.bucket.fetch_to_file(s3key, cachepath) 
        log.debug('Object %s fetched successfully.', s3key)
        
        return cachepath      
        
    def expire(self):
        '''Perform cache expiry 
        
        Removes entries until there are at most 300 open files
        and the cache size is below self.maxsize or the number
        of cache entries is below the number of active threads.
         
        We want to leave at least as many object in the cache as
        there are active threads, otherwise 
        we may end up storing & fetching the same object several times
        just for one read() or write() call.
        Note that active_count() unfortunately also includes possible
        ExpireEntryThreads, but this should have no negative effects
        (since at worst we will expire to little at this run)
        '''
        
        log.debug('Expiring cache')
        
        while (self.size > self.maxsize or len(self.keys) > 300) and \
              len(self.keys) > threading.active_count():
            self._expire_parallel()
            
        log.debug("Expiration end")
    
            
    def _expire_parallel(self):
        """Remove oldest entries from the cache.
        
        Expires the oldest entries to free at least 1 MB. Expiration is
        done for all the keys at the same time using different threads.
        However, at most 25 threads are started and we will at least
        as many objects in the cache as there are active threads when
        this function is called (see description of expire() for more info).
        
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
        
        # We don't want to include the threads that we start ourself
        threadcnt = threading.active_count()
        
        threads = list()
        freed_size = 0
        while ( freed_size < 1024*1024
                and len(self.keys) > threadcnt
                and len(threads) < 25 ):
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
        """Unlink s3 objects of given inode.
        
        If `blockno` is specified, unlinks only s3 objects for blocks
        >= `blockno`. If no other blocks reference the s3 objects,
        they are completely removed.
        """        
        
        # Run through keys
        log.debug('Removing s3 objects for inode %i, starting at block %i', inode, blockno)
        while True:
            with self.global_lock:
                with self.dbcm() as conn:
                    try:
                        (s3key, cur_off) = \
                            conn.get_row("SELECT s3key,blockno FROM blocks WHERE inode=? "
                                        "AND blockno >= ? LIMIT 1", (inode, blockno))
                    except StopIteration:    # No keys left
                        break
                    
                    # Remove from table
                    log.debug('Removing object %s from table', s3key)
                    with conn.transaction():
                        conn.execute("DELETE FROM blocks WHERE inode=? AND blockno=?", 
                                    (inode, cur_off))
                        refcount = conn.get_val("SELECT refcount FROM s3_objects WHERE key=?",
                                               (s3key,))
                        refcount -= 1
                        if refcount == 0:
                            conn.execute("DELETE FROM s3_objects WHERE key=?", (s3key,))
                        else:
                            conn.execute("UPDATE s3_objects SET refcount=? WHERE key=?",
                                    (refcount, s3key))
                            # No need to do actually remove the object
                            continue
                    
                # Remove from AWS
                self.s3_lock.acquire(s3key)
            
            log.debug('Removing object %s from S3', s3key)
            # TODO: We should just mark it dirty in the cache with size 0
            try:
                el = self.keys.pop(s3key, None)
                if el is not None: # In cache
                    el.seek(0, 2)
                    self.size -= el.tell()
                    el.close()  
                    os.unlink(el.name) 
                
                # Remove from s3
                try:
                    # The object may not have been committed yet
                    self.bucket.delete_key(s3key)
                except KeyError:  
                    pass 
            finally:
                self.s3_lock.release(s3key)
        
        
    def flush(self, inode):
        """Upload dirty data for `inode`.
        
        """
        
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
                el.seek(0, 2)
                etag = self.bucket.store_from_file(el.s3key, el.name)
                self.dbcm.execute("UPDATE s3_objects SET etag=?, size=? WHERE key=?",
                            (etag, el.tell(), el.s3key))
                el.dirty = False
                
        log.debug('Flushing for inode %d completed.', inode)

    def close(self):
        """Uploads all dirty data and cleans the cache.
             
        """       

        log.debug('Closing S3Cache') 
            
        with self.global_lock:
            while len(self.keys):
                el = self.keys.pop_last()
                
                
                if el.dirty:
                    el.flush()    
                    el.seek(0, 2)
                    etag = self.bucket.store_from_file(el.s3key, el.name)
                    self.dbcm.execute("UPDATE s3_objects SET etag=?, size=? WHERE key=?",
                                (etag, el.tell(), el.s3key))

                el.close()
                os.unlink(el.name)
 
                
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
        '''Expire oldest cache entry.
        
        '''
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
            el.close()
            
            if el.dirty:
                log.info('Committing dirty s3 object %s...', el.s3key)
                etag = s3cache.bucket.store_from_file(el.s3key, el.name)
                s3cache.dbcm.execute("UPDATE s3_objects SET etag=?, size=? WHERE key=?",
                                  (etag, self.size, el.s3key))
   
            log.debug('Removing s3 object %s from cache..', el.s3key)
                
            os.unlink(el.name)
        finally:
            s3cache.s3_lock.release(el.s3key)
        
        log.debug('Expiration thread finished.')
        