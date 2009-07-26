#
#    Copyright (C) 2008-2009  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from contextlib import contextmanager
from s3ql import fs
from s3ql.multi_lock import MultiLock
from s3ql.ordered_dict import OrderedDict
import errno
import logging
import os
import threading
import time

__all__ = [ "S3Cache" ]

# standard logger for this module
log = logging.getLogger("S3Cache")


class CacheEntry(object):
    """An element in the s3 cache.
    
    Attributes:
    -----------

    :fh:        File handle to this object
    :dirty:     Has the file been changed?
    :name:      s3 key
    
    """
    __slots__ = [ "fh", "dirty", "name" ]
    
    def __init__(self, name, fh, dirty=False):
        self.fh = fh
        self.name = name
        self.dirty = dirty    
    
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
    an S3Cache object. Whenever the fs needs to write or read
    from an s3key, it uses a context manager provided by the S3Cache
    object which returns a file handle to the s3 object. The S3Cache retrieves
    and stores objects on S3 as necessary. Moreover, it provides
    methods to delete and create s3 objects, once again taking care
    of the necessary locking.
    
    Since the S3Cache methods are designed to be called from
    different threads, they all take a database cursor as an argument.
    The caller has to make sure that different threads use different
    database connections.
    
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
    :cachedir:    Where to put the cache files
    :blocksize:   Filesystem blocksize, used to calculate current cache size
    :s3_lock:     MultiLock to synchronize access to individual s3 objects
    :global_lock: Global lock    
    :timeout:     Maximum time to wait for changes in S3 to propagate
    """
    
    def __init__(self, bucket, cachedir, cachesize, blocksize):
        log.debug('Initializing')
        self.keys = OrderedDict()
        self.cachedir = cachedir
        self.maxsize = cachesize
        self.blocksize = blocksize
        self.bucket = bucket
        self.s3_lock = MultiLock()
        self.global_lock = threading.Lock()
        self.timeout = 300 

    @contextmanager
    def get(self, inode, offset, cur, markdirty=False):
        """Get filehandle for s3 object backing `inode` at `offset`
        
        Uses database cursor `cur`.
         
        This is a context manager function, so the intended usage is
        
        with s3cache.s3_object(inode, offset) as fh:
            fh.write(...)
            
        Note that offset has to be the starting offset. If caller is
        going to write into the fh, he has to set `markdirty` so that
        the changes are propagated back into S3.
        
        """
        
        # Get s3 key
        log.debug('Getting filehandle for inode %i, offset %i', inode, offset)
        with self.global_lock:
            try:
                s3key = cur.get_val("SELECT s3key FROM inode_s3key WHERE inode=? AND offset=?", 
                                    (inode, offset))
                log.debug('s3key is %s', s3key)
            except StopIteration:
                # Create and add to cache
                log.debug('creating new s3 object')
                cur.execute("SAVEPOINT 'S3Cache.get'")
                try:
                    s3key = "s3ql_data_%d_%d" % (inode, offset) # This is a unique new key
                    cur.execute("INSERT INTO s3_objects (id,refcount) VALUES(?,?)", (s3key,1))
                    cur.execute("INSERT INTO inode_s3key (inode, offset, s3key) VALUES(?,?,?)",
                                (inode, offset, s3key))
                except:
                    cur.execute("ROLLBACK TO 'S3Cache.get'")
                    raise
                finally:
                    cur.execute("RELEASE 'S3Cache.get'")
                    
                self.keys[s3key] = CacheEntry(s3key, open(self.cachedir + s3key, "w+b"))
            
            log.debug('Acquiring object lock')    
            self.s3_lock.acquire(s3key)

        # Get s3 object
        try:
            if s3key not in self.keys:
                log.debug('Object not cached, retrieving from s3')
                self.expire_cache()
                etag = cur.get_val("SELECT etag FROM s3_objects WHERE id=?", (s3key,))
                el = CacheEntry(s3key, self._download_object(s3key, etag))
                self.keys[s3key] = el
            else:
                log.debug('Using cached object')
                el = self.keys[s3key]
                self.keys.to_head(s3key)
                       
            # Now the fh is made available 
            if markdirty:
                el.dirty = True
                
            yield el.fh
        finally:
            log.debug('Releasing object lock')
            self.s3_lock.release(s3key)
            
            
    def _download_object(self, s3key, etag):
        """Downloads an s3 object from amazon into the cache.
        
        Not synchronized. 
        """
        
        cachepath = self.cachedir + s3key
        meta = self.bucket.lookup_key(s3key)

        # Check etag
        if meta.etag != etag:
            log.warn("Changes in %s have apparently not yet propagated. Waiting and retrying...\n"
                     "Try to increase the cache size to avoid this.", s3key)
            waited = 0
            waittime = 0.01
            while meta.etag != etag and \
                    waited < self.timeout:
                time.sleep(waittime)
                waited += waittime
                waittime *= 1.5
                meta = self.bucket.lookup_key(s3key)

            # If still not found
            if meta.etag != etag:
                log.error("etag for %s doesn't match metadata!" 
                          "Filesystem is probably corrupted (or S3 is having problems), "
                          "run fsck.s3ql as soon as possible.", s3key)
                raise fs.FUSEError(errno.EIO, fatal=True)
            
        self.bucket.fetch_to_file(s3key, cachepath) 
        return open(cachepath, "r+b")        
        
                  
    def expire_cache(self, cur):
        """Performs cache expiry.

        If the cache is bigger than `self.cachesize`, the oldest
        entries are flushed until at least `self.blocksize`
        bytes are available (or there are no objects left to flush).
        
        Uses database cursor `cur`.
        """
        log.debug('Expiring cache')
        while (len(self.keys)+1) * self.blocksize > self.maxsize and self.keys:

            with self.global_lock:
                el = self.keys.pop_last()
                self.s3_lock.acquire(el.name)
           
            try:
                log.debug('Expiring s3 object %s', el.name)
                el.fh.close()
                if el.dirty:
                    etag = self.bucket.store_from_file(el.name, self.cachedir + el.name)
                    cur.execute("UPDATE s3_objects SET etag=?, last_modified=? "
                                "WHERE id=?", (etag, time.time(), el.name))
                os.unlink(self.cachedir + el.name)

            finally:
                self.s3_lock.release(el.name)


    def remove(self, inode, cur, offset=0):
        """Unlinks all s3 objects from the given inode.
        
        If `offset` is specified, unlinks only s3 objects starting at
        positions >= `offset`. If no other inodes reference the s3 objects,
        they are completely removed.
        
        Uses database cursor `cur`.
        """        
        # Run through keys
        log.debug('Removing s3 objects for inode %i, starting at offset %i', inode, offset)
        while True:
            with self.global_lock:
                try:
                    (s3key, cur_off) = cur.get_row("SELECT s3key,offset FROM inode_s3key WHERE inode=? AND offset >= ?", 
                                                   (inode, offset))
                except StopIteration:    # No keys left
                    break
                
                # Remove from table
                log.debug('Removing object %s from table', s3key)
                cur.execute("SAVEPOINT 'S3Cache.remove'")
                try:
                    cur.execute("DELETE FROM inode_s3key WHERE inode=? AND offset=?", 
                                (inode, cur_off))
                    refcount = cur.get_val("SELECT refcount FROM s3_objects WHERE id=?",
                                           (s3key,))
                    refcount -= 1
                    if refcount == 0:
                        cur.execute("DELETE FROM s3_objects WHERE id=?", (s3key,))
                    else:
                        cur.execute("UPDATE s3_objects SET refcount=? WHERE id=?",
                                (refcount, s3key))
                        # No need to do actually remove the object
                        continue
                except:
                    cur.execute("ROLLBACK TO 'S3Cache.remove'")
                    raise
                finally:
                    cur.execute("RELEASE 'S3Cache.remove'")
                    
                # Remove from AWS
                self.s3_lock.acquire(s3key)
            
            log.debug('Removing object %s from S3', s3key)
            #pylint: disable-msg=W0704
            # - the except doesn't do anything deliberately
            try:
                el = self.keys.pop(s3key, None)
                if el is not None: # In cache
                    el.fh.close()   #pylint: disable-msg=E1103
                    os.unlink(self.cachedir + el.name)  #pylint: disable-msg=E1103
                
                # Remove from s3
                try:
                    # The object may not have been committed yet
                    self.bucket.delete_key(s3key)
                except KeyError: 
                    pass 
            finally:
                self.s3_lock.release(s3key)
                
            

        
        
    def flush(self, inode, cur):
        """Uploads all dirty data from `inode` to S3
        
        No locking required. Uses database cursor `cur`.
        """
        # Determine s3 objects from this inode
        log.debug('Flushing objects for inode %i', inode) 
        to_flush = [ self.keys[s3key] for (s3key,) 
                    in cur.execute("SELECT s3key FROM inode_s3key WHERE inode=?", (inode,))
                    if s3key in self.keys ]
        
        # Flush if required
        for el in to_flush:
            if not el.dirty:
                log.debug('Object %s is not dirty', el.name)
                continue
            
            log.debug('Flushing object %s', el.name)
            
            # We have to set this *before* uploading, otherwise we loose changes
            # during the upload
            el.dirty = False
                
            try:
                el.fh.flush()    
                etag = self.bucket.store_from_file(el.name, self.cachedir + el.name)
                cur.execute("UPDATE s3_objects SET etag=?, last_modified=? WHERE id=?",
                            (etag, time.time(), el.name))
            except:
                el.dirty = True
                raise


    def close(self, cur):
        """Uploads all dirty data and cleans the cache.
        
        Uses database cursor `cur`.         
        """       
        log.debug('Closing S3Cache') 
        with self.global_lock:
            while len(self.keys):
                el = self.keys.pop_last()
                
                if el.dirty:
                    el.fh.flush()    
                    etag = self.bucket.store_from_file(el.name, self.cachedir + el.name)
                    cur.execute("UPDATE s3_objects SET etag=?, last_modified=? WHERE id=?",
                                (etag, time.time(), el.name,))

                el.fh.close()
                os.unlink(self.cachedir + el.name)
                
    def __del__(self):
        if self.keys:
            raise RuntimeError("s3ql.s3Cache instance was destroyed without calling close()!")