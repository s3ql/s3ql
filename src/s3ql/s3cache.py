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
    :bgcommit:    Enable background commit mode
    :commit_thread: Background committer thread
    :shutdown:    Flag for committer thread to shut down
                  
    
    The `expect_mismatch` attribute is only for unit testing instrumentation
    and suppresses warnings if a mismatch between local and remote etag
    is encountered.                   
    
    Background Commit Mode
    ----------------------
    
    In background commit mode, expire(), close() and flush() do not actually 
    commit any data to S3. Instead, __init__() starts a background thread that
    periodically performs cache expiration. This means that the cache can grow without
    bounds and uncommitted changes may reside in the cache even after
    the file system has been unmounted. 
    
    
    Cache Management
    ----------------
    
    Note that every s3 object in the cache occupies a file
    descriptor. The maximum number of objects in the cache is
    therefore hardcoded to 300 (so that we do not run out of
    file descriptors). If the block size of the filesystem is
    small, and the cache size is large, it can therefore happen
    that the cache is expired before it reaches maximum size.
    
    Note that this maximum is enforced even in background commit
    mode, i.e. if 300 objects are in cache, a writer to a new
    object has to wait until one of the objects has been
    expired and committed to S3.
    """
    
    def __init__(self, bucket, cachedir, cachesize, dbcm, timeout=60, bgcommit=False):
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
        self.bgcommit = bgcommit
        self.shutdown = threading.Event()
        self.shutdown.clear()
        
        # Reconstruct cache
        self._recover_cache()
             
        if bgcommit:
            # Start commit thread
            self.commit_thread = ExceptionStoringThread(target=self._commit)
            self.commit_thread.start()
        
            
    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for s3 object backing `inode` at block `blockno`
        
        """
                 
        # Get s3 key    
        log.debug('Getting file handle for inode %i, block %i', inode, blockno)
        with self.global_lock:
            with self.dbcm() as conn:
                try:
                    s3key = conn.get_val("SELECT s3key FROM blocks WHERE inode=? AND blockno=?", 
                                        (inode, blockno))
                    log.debug('s3key is %s', s3key)
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

            log.debug('Acquiring object lock')   
            self.s3_lock.acquire(s3key)

       
        # Get s3 object
        try:
            try:
                el = self.keys[s3key]
            except KeyError:
                log.debug('Object not cached, retrieving from s3')
                etag = self.dbcm.get_val("SELECT etag FROM s3_objects WHERE key=?", (s3key,))
                el = CacheEntry(s3key, self._download_object(s3key, etag), 'r+b')
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
      
    def _recover_cache(self):
        '''Read cache directory and register contents 
        
        Not synchronized. Must be called before other threads are running.
        '''
        
        log.debug('Recovering cache files...')
        for s3key in os.listdir(self.cachedir):
            assert isinstance(s3key, unicode)
            log.debug('Recovering %s', s3key)
            el = CacheEntry(s3key, self.cachedir + s3key, "r+b")
            el.dirty = True
            self.keys[s3key] = el
            
            el.seek(0, 2)
            self.size += el.tell()
              
        log.debug('Cache recovery complete.')      
            
    def _download_object(self, s3key, etag):
        """Download s3 object from amazon into the cache. Return path.
        
        Not synchronized. 
        """

        log.debug('Attempting to download object %s from S3', s3key)
        cachepath = self.cachedir + s3key
        meta = self.bucket.lookup_key(s3key)

        # Check etag
        if meta.etag != etag:
            if not self.expect_mismatch:
                log.warn("Changes in %s have apparently not yet propagated. Waiting and retrying...\n"
                         "Try to increase the cache size to avoid this.", s3key)
            log.debug('Stored etag: %s, Received etag: %s', repr(etag), repr(meta.etag))
            waited = 0
            waittime = 0.2
            while meta.etag != etag and \
                    waited < self.timeout:
                time.sleep(waittime)
                waited += waittime
                waittime *= 1.5
                log.info('Retrying to fetch object %s from S3 (%d sec to timeout.)..', s3key,
                         self.timeout - waited)
                meta = self.bucket.lookup_key(s3key)

            # If still not found
            if meta.etag != etag:
                if not self.expect_mismatch:
                    log.error("etag for %s doesn't match metadata!" 
                              "Filesystem is probably corrupted (or S3 is having problems), "
                              "run fsck.s3ql as soon as possible.", s3key)
                raise fs.FUSEError(errno.EIO, fatal=True)
            
        self.bucket.fetch_to_file(s3key, cachepath) 
        log.debug('Object %s fetched successfully.', s3key)
        return cachepath      
        
    def expire(self):
        '''Perform cache expiry 
        
        In background commit mode, the cache is only flushed if there
        are more than 300 open files.
        
        Otherwise, the cache is flushed until it its size is
        below self.maxsize or there is only one object left to flush.
        '''
        
        if self.bgcommit:
            # Only commit to limit number of open fds
            while len(self.keys) > 300:
                self._expire_entry()
        else:       
            log.debug('Expiring cache')
            
            # We want to leave at least one object in the cache, otherwise
            # we may end up storing & fetching the same object several times
            # just for one read() or write() call
            while (self.size > self.maxsize or len(self.keys) > 300) and len(self.keys) > 1:
                self._expire_entry()
                
            log.debug("Expiration end")
          
    def _commit(self):
        '''Run commit thread.
        
        Sleep for a while; Expire the cache if necessary; continue forever. 
        '''           
        
        # Pylint ignore since we deliberately override the main logger
        log = logging.getLogger("S3Cache.Committer") #pylint: disable-msg=W0621
        log.debug('Started.')
        while True:
            while (self.size > self.maxsize or len(self.keys) > 300) and len(self.keys) > 1:
                if self.shutdown.is_set():
                    log.debug('Received shutdown signal, exiting.')
                    return
                
                log.debug('Expiring entry..')
                self._expire_entry()
                
            # Wait for 5 seconds or until shutdown is set
            log.debug('Nothing more to expire. Sleeping...')
            self.shutdown.wait(10)
            
            if self.shutdown.is_set():
                log.debug('Received shutdown signal, exiting.')
                return
            
    
            
    def _expire_entry(self):
        """Remove the oldest entry from the cache.
        """
         
        log.debug('Trying to expire oldest cache entry..') 
        
        with self.global_lock:
            # If we pop the object before having locked it, another thread 
            # may download it - overwriting the existing file!
            try:
                el = self.keys.get_last()
            except IndexError:
                log.debug('No objects in cache. Returning.')
                return
            
            log.debug('Least recently used object is %s, obtaining object lock..', el.s3key)
            self.s3_lock.acquire(el.s3key)  
            
        try:
            try:
                del self.keys[el.s3key]
            except KeyError:
                # Another thread already expired it, we need to try again
                log.debug('Object has already been expired in another thread, returning.')
                return

            el.seek(0, 2)
            size = el.tell()
            el.close()
            
            if el.dirty:
                log.debug('Committing dirty s3 object %s...', el.s3key)
                etag = self.bucket.store_from_file(el.s3key, el.name)
                self.dbcm.execute("UPDATE s3_objects SET etag=?, size=? WHERE key=?",
                                  (etag, size, el.s3key))
   
            log.debug('Removing s3 object %s from cache..', el.s3key)
                
            # Update cachesize
            self.size -= size
            
            os.unlink(el.name)

        finally:
            self.s3_lock.release(el.s3key)
                
        

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
        """Upload dirty data for `inode`  unless in bgcommit mode
        
        No locking required. 
        """
        
        if self.bgcommit:
            log.debug('Skipping flush for inode %d since in bgcommit mode', inode)
            return
        
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
        
            log.debug('Flushing object %s', el.s3key)
            
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
             
        If background commit is activated, the committing thread is 
        stopped and all committed data is cleaned from the cache. 
        Uncommitted data remains in the cache.
        """       

        log.debug('Closing S3Cache') 
        
        if self.shutdown.is_set():
            raise RuntimeError("close was called more than once!")
        
        self.shutdown.set()
        if self.bgcommit:
            log.debug('Waiting for background thread to exit..')
            self.commit_thread.join_and_raise()
            log.debug('Background thread has exited.')
            
        with self.global_lock:
            while len(self.keys):
                el = self.keys.pop_last()
                
                
                if not el.dirty:
                    el.close()
                    os.unlink(el.name)
                elif self.bgcommit:
                    el.close()
                else:
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