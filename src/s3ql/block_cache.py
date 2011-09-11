'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from .database import NoSuchRowError
from .multi_lock import MultiLock
from .ordered_dict import OrderedDict
from contextlib import contextmanager
from llfuse import lock, lock_released
import logging
import os
import shutil
import time


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

    __slots__ = [ 'dirty', 'block_id', 'inode', 'blockno', 'last_access',
                  'modified_after_upload' ]

    def __init__(self, inode, blockno, block_id, filename):
        # Open unbuffered, so that os.fstat(fh.fileno).st_size is
        # always correct (we can expect that data is read and
        # written in reasonable chunks)
        super(CacheEntry, self).__init__(filename, "w+b", buffering=0)
        self.dirty = False
        self.modified_after_upload = False
        self.block_id = block_id
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
        return ('<CacheEntry, inode=%d, blockno=%d, dirty=%s, block_id=%r>' % 
                (self.inode, self.blockno, self.dirty, self.block_id))

class BlockCache(object):
    """Provides access to file blocks
    
    This class manages access to file blocks. It takes care of creation,
    uploading, downloading and deduplication.
 
    This class uses the llfuse global lock. Methods which release the lock have
    are marked as such in their docstring.

            
    Attributes:
    -----------
    
    :mlock: locks on (inode, blockno) during `get`, so that we do not
            download the same object with more than one thread.
    """

    def __init__(self, bucket_pool, db, cachedir, max_size, 
                 upload_manager, max_entries=768):
        log.debug('Initializing')
        self.cache = OrderedDict()
        self.cachedir = cachedir
        self.max_size = max_size
        self.max_entries = max_entries
        self.size = 0
        self.db = db
        self.bucket_pool = bucket_pool
        self.mlock = MultiLock()
        self.upload_manager = upload_manager

    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.cache)

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

        if self.size > self.max_size or len(self.cache) > self.max_entries:
            self.expire()

        # Need to release global lock to acquire mlock to prevent deadlocking
        lock.release()
        with self.mlock(inode, blockno):
            lock.acquire()
            
            try:
                el = self.cache[(inode, blockno)]
    
            # Not in cache
            except KeyError:
                filename = os.path.join(self.cachedir,
                                        'inode_%d_block_%d' % (inode, blockno))
                try:
                    block_id = self.db.get_val('SELECT block_id FROM inode_blocks_v '
                                               'WHERE inode=? AND blockno=?', (inode, blockno))                    
    
                # No corresponding object
                except NoSuchRowError:
                    log.debug('get(inode=%d, block=%d): creating new block', inode, blockno)
                    el = CacheEntry(inode, blockno, None, filename)
    
                # Need to download corresponding object
                else:
                    log.debug('get(inode=%d, block=%d): downloading block', inode, blockno)
                    # At the moment, objects contain just one block
                    el = CacheEntry(inode, blockno, block_id, filename)
                    obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
                    
                    try:
                        with lock_released:
                            with self.bucket_pool() as bucket:
                                with bucket.open_read('s3ql_data_%d' % obj_id) as fh:
                                    shutil.copyfileobj(fh, el)
                    except:
                        os.unlink(filename)
                        raise
                        
                    # Writing will have set dirty flag
                    el.dirty = False
                    os.rename(el.name + '.d', el.name)
                    
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

        while (len(self.cache) > self.max_entries or
               (len(self.cache) > 0  and self.size > self.max_size)):

            need_size = self.size - self.max_size
            need_entries = len(self.cache) - self.max_entries
            
            # Try to expire entries that are not dirty
            for el in self.cache.values_rev():
                if el.dirty:
                    if (el.inode, el.blockno) not in self.upload_manager.in_transit:
                        log.debug('expire: %s is dirty, trying to flush', el)
                        break
                    else:
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
                        freed = self.upload_manager.add(el) # Releases global lock
                        need_size -= freed
                    else:
                        need_size -= os.fstat(el.fileno()).st_size
                    need_entries -= 1                
            
                    if need_size <= 0 and need_entries <= 0:
                        break
                                    
            # Wait for the next entry  
            log.debug('expire: waiting for upload threads..')
            self.upload_manager.join_one() # Releases global lock

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

                if el.block_id is None:
                    log.debug('remove(inode=%d, blockno=%d): block only in cache',
                              inode, blockno)
                    continue

                log.debug('remove(inode=%d, blockno=%d): block in cache and db', inode, blockno)
                block_id = el.block_id

            else:
                try:
                    block_id = self.db.get_val('SELECT block_id FROM inode_blocks_v '
                                               'WHERE inode=? AND blockno=?', (inode, blockno))   
                except NoSuchRowError:
                    log.debug('remove(inode=%d, blockno=%d): block does not exist',
                              inode, blockno)
                    continue

                log.debug('remove(inode=%d, blockno=%d): block only in db ', inode, blockno)

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
                continue            
        
            # Delete object
            self.upload_manager.remove(obj_id, (inode, blockno))

        log.debug('remove(inode=%d, start=%d, end=%s): end', inode, start_no, end_no)

    def flush(self, inode):
        """Flush buffers for `inode`"""

        # Cache entries are automatically flushed after each read() and write()
        pass

    def commit(self):
        """Upload all dirty blocks
        
        This method releases the global lock.
        """
    
        in_transit = set()
        
        for el in self.cache.itervalues():
            if not el.dirty:
                continue
            
            if (el.inode, el.blockno) in self.upload_manager.in_transit:
                if not el.modified_after_upload:
                    continue
                
                # We need to wait for the current upload to complete
                in_transit.add(el)
            else:
                self.upload_manager.add(el) # Releases global lock
    
        while in_transit:
            log.warn('commit(): in_transit: %s', in_transit)
            self.upload_manager.join_one()
            finished = in_transit.difference(self.upload_manager.in_transit)
            in_transit = in_transit.intersection(self.upload_manager.in_transit)    
                    
            for el in finished:
                # Object may no longer be dirty or already in transit
                # if a different thread initiated the object while
                # the global lock was released in a previous iteration.
                if el.dirty:
                    continue
                if  (el.inode, el.blockno) in self.upload_manager.in_transit:
                    continue
                
                self.upload_manager.add(el) # Releases global lock
                
        # Now wait for all uploads to complete
        while self.upload_manager.upload_in_progress():
            self.upload_manager.join_one() # Releases global lock
          
    def clear(self):
        """Upload all dirty data and clear cache
        
        This method releases the global lock.
        """

        log.debug('clear: start')
        bak = self.max_entries
        self.max_entries = 0
        self.expire() # Releases global lock
        self.max_entries = bak

        # Now wait for all uploads to complete
        while self.upload_manager.upload_in_progress():
            self.upload_manager.join_one() # Releases global lock
            
        log.debug('clear: end')


    def __del__(self):
        if len(self.cache) > 0:
            raise RuntimeError("BlockCache instance was destroyed without calling destroy()!")

