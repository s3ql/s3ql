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
from .common import EmbeddedException, ExceptionStoringThread
from .thread_group import ThreadGroup
from .upload_manager import UploadManager, RemoveThread, retry_exc
from .database import NoSuchRowError
from llfuse import lock, lock_released
import logging
import os
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

    def __init__(self, inode, blockno, block_id, filename, mode):
        super(CacheEntry, self).__init__(filename, mode)
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

MAX_REMOVAL_THREADS = 25
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
    :encountered_errors: This attribute is set if some non-fatal errors
            were encountered during asynchronous operations (for
            example, an object that was supposed to be deleted did
            not exist).
    """

    def __init__(self, bucket, db, cachedir, max_size, max_entries=768):
        log.debug('Initializing')
        self.cache = OrderedDict()
        self.cachedir = cachedir
        self.max_size = max_size
        self.max_entries = max_entries
        self.size = 0
        self.db = db
        self.bucket = bucket
        self.mlock = MultiLock()
        self.removal_queue = ThreadGroup(MAX_REMOVAL_THREADS)
        self.upload_manager = UploadManager(bucket, db, self.removal_queue)
        self.commit_thread = CommitThread(self)
        self.encountered_errors = False

    def init(self):
        log.debug('init: start')
        if not os.path.exists(self.cachedir):
            os.mkdir(self.cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        self.commit_thread.start()
        log.debug('init: end')

    def destroy(self):
        log.debug('destroy: start')
        
        # If there were errors, we still want to try to finalize
        # as much as we can
        try:
            self.commit_thread.stop()
        except Exception as exc:
            self.encountered_errors = True
            if isinstance(exc, EmbeddedException):
                log.error('CommitThread encountered exception.')
            else:
                log.exception('Error when stopping commit thread')
            
        try:
            self.clear()
        except:
            self.encountered_errors = True
            log.exception('Error when clearing cache')
            
        while True:
            try:
                self.upload_manager.join_all()
            except Exception as exc:
                self.encountered_errors = True
                if isinstance(exc, EmbeddedException):
                    log.error('UploadManager encountered exception.')
                else:
                    log.exception('Error when joining UploadManager')
                    break
            else:
                break
            
        while True:
            try:
                self.removal_queue.join_all()
            except Exception as exc:
                self.encountered_errors = True
                if isinstance(exc, EmbeddedException):
                    log.error('RemovalQueue encountered exception.')
                else:
                    log.exception('Error when waiting for removal queue:')
                    break
            else:
                break
                            
        if self.upload_manager.encountered_errors:
            self.encountered_errors = True
            
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
        
        This method releases the global lock.
        
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
                    if blockno == 0:
                        block_id = self.db.get_val('SELECT block_id FROM inodes WHERE id=?', (inode,))
                    else:
                        block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
                                                   'WHERE inode=? AND blockno=?', (inode, blockno))                    
    
                # No corresponding object
                except NoSuchRowError:
                    log.debug('get(inode=%d, block=%d): creating new block', inode, blockno)
                    el = CacheEntry(inode, blockno, None, filename, "w+b")
    
                # Need to download corresponding object
                else:
                    log.debug('get(inode=%d, block=%d): downloading block', inode, blockno)
                    # At the moment, objects contain just one block
                    el = CacheEntry(inode, blockno, block_id, filename, "w+b")
                    obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
                    with lock_released:
                        try:
                            if self.bucket.read_after_create_consistent():
                                self.bucket.fetch_fh('s3ql_data_%d' % obj_id, el)
                            else:
                                retry_exc(300, [ NoSuchObject ], self.bucket.fetch_fh,
                                          's3ql_data_%d' % obj_id, el)
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
            el.flush()
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
        
        Note: if `get` and `remove` are called concurrently, then it
        is possible that a block that has been requested with `get` and
        passed to `remove` for deletion will not be deleted.
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

                if el.obj_id is None:
                    log.debug('remove(inode=%d, blockno=%d): block only in cache',
                              inode, blockno)
                    continue

                log.debug('remove(inode=%d, blockno=%d): block in cache and db', inode, blockno)
                block_id = el.block_id

            else:
                try:
                    if blockno == 0:
                        block_id = self.db.get_val('SELECT block_id FROM inodes WHERE id=?', (inode,))
                    else:
                        block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
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
            try:
                # Releases global lock:
                self.removal_queue.add_thread(RemoveThread(obj_id, self.bucket,
                                                           (inode, blockno),
                                                           self.upload_manager))
            except EmbeddedException as exc:
                exc = exc.exc_info[1]
                if isinstance(exc, NoSuchObject):
                    log.warn('Backend seems to have lost object %s', exc.key)
                    self.encountered_errors = True
                else:
                    raise

        log.debug('remove(inode=%d, start=%d, end=%s): end', inode, start_no, end_no)

    def flush(self, inode):
        """Flush buffers for `inode`"""

        # Cache entries are automatically flushed after each read()
        # and write()
        pass

    def commit(self):
        """Upload all dirty blocks
        
        This method uploads all dirty blocks. The object itself may
        still be in transit when the method returns, but the
        blocks table is guaranteed to refer to the correct objects.
        
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
                
                        
    def clear(self):
        """Upload all dirty data and clear cache
        
        When the method returns, all blocks have been registered
        in the database, but the actual uploads may still be 
        in progress.
        
        This method releases the global lock.
        """

        log.debug('clear: start')
        bak = self.max_entries
        self.max_entries = 0
        self.expire() # Releases global lock
        self.max_entries = bak
        log.debug('clear: end')

    def __del__(self):
        if len(self.cache) > 0:
            raise RuntimeError("BlockCache instance was destroyed without calling destroy()!")

class CommitThread(ExceptionStoringThread):
    '''
    Periodically upload dirty blocks.
    
    This class uses the llfuse global lock. When calling objects
    passed in the constructor, the global lock is acquired first.
    '''    
    

    def __init__(self, bcache):
        super(CommitThread, self).__init__()
        self.bcache = bcache 
        self.stop_event = threading.Event()
        self.name = 'CommitThread'
                    
    def run_protected(self):
        log.debug('CommitThread: start')
 
        while not self.stop_event.is_set():
            did_sth = False
            stamp = time.time()
            for el in self.bcache.cache.values_rev():
                if stamp - el.last_access < 10:
                    break
                if (not el.dirty or
                    (el.inode, el.blockno) in self.bcache.upload_manager.in_transit):
                    continue
                        
                # Acquire global lock to access UploadManager instance
                with lock:
                    if (not el.dirty or # Object may have been accessed
                        (el.inode, el.blockno) in self.bcache.upload_manager.in_transit):
                        continue
                    self.bcache.upload_manager.add(el)
                did_sth = True

                if self.stop_event.is_set():
                    break
            
            if not did_sth:
                self.stop_event.wait(5)

        log.debug('CommitThread: end')    
        
    def stop(self):
        '''Wait for thread to finish, raise any occurred exceptions.
        
        This  method releases the global lock.
        '''
        
        self.stop_event.set()
        with lock_released:
            self.join_and_raise()
        