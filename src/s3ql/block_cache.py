'''
block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from . import BUFSIZE
from .database import NoSuchRowError
from .multi_lock import MultiLock
from .logging import logging # Ensure use of custom logger class
from collections import OrderedDict
from contextlib import contextmanager
from llfuse import lock, lock_released
from queue import Queue, Empty as QueueEmpty, Full as QueueFull
import os
import hashlib
import shutil
import threading
import time

# standard logger for this module
log = logging.getLogger(__name__)

# Special queue entry that signals threads to terminate
QuitSentinel = object()

# Special queue entry that signals that removal queue should
# be flushed
FlushSentinel = object()

class NoWorkerThreads(Exception):
    '''
    Raised when trying to enqueue an object, but there
    are no active consumer threads.
    '''

    pass

class Distributor(object):
    '''
    Distributes objects to consumers.
    '''

    def __init__(self):
        super().__init__()

        self.slot = None
        self.cv = threading.Condition()

        #: Number of threads waiting to consume an object
        self.readers = 0

    def put(self, obj, timeout=None):
        '''Offer *obj* for consumption

        The method blocks until another thread calls `get()` to consume the
        object.

        Return `True` if the object was consumed, and `False` if *timeout* was
        exceeded without any activity in the queue (this means an individual
        invocation may wait for longer than *timeout* if objects from other
        threads are being consumed).
        '''

        if obj is None:
            raise ValueError("Can't put None into Queue")

        with self.cv:
            # Wait until a thread is ready to read
            while self.readers == 0 or self.slot is not None:
                log.debug('waiting for reader..')
                if not self.cv.wait(timeout):
                    log.debug('timeout, returning')
                    return False

            log.debug('got reader, enqueueing %s', obj)
            self.readers -= 1
            assert self.slot is None
            self.slot = obj
            self.cv.notify_all() # notify readers

        return True

    def get(self):
        '''Consume and return an object

        The method blocks until another thread offers an object by calling the
        `put` method.
        '''
        with self.cv:
            self.readers += 1
            self.cv.notify_all()
            while self.slot is None:
                log.debug('waiting for writer..')
                self.cv.wait()
            tmp = self.slot
            self.slot = None
            self.cv.notify_all()

        return tmp


class SimpleEvent(object):
    '''
    Like threading.Event, but without any internal flag. Calls
    to `wait` always block until some other thread calls
    `notify` or `notify_all`.
    '''

    def __init__(self):
        super().__init__()
        self.__cond = threading.Condition(threading.Lock())

    def notify_all(self):
        self.__cond.acquire()
        try:
            self.__cond.notify_all()
        finally:
            self.__cond.release()

    def notify(self):
        self.__cond.acquire()
        try:
            self.__cond.notify()
        finally:
            self.__cond.release()

    def wait(self):
        self.__cond.acquire()
        try:
            self.__cond.wait()
        finally:
            self.__cond.release()


class CacheEntry(object):
    """An element in the block cache

    Attributes:
    -----------

    :dirty:    entry has been changed since it was last uploaded.
    :size:     current file size
    :pos: current position in file
    """

    __slots__ = [ 'dirty', 'inode', 'blockno', 'last_access',
                  'size', 'pos', 'fh', 'removed' ]

    def __init__(self, inode, blockno, filename):
        super().__init__()
        # Writing 100MB in 128k chunks takes 90ms unbuffered and
        # 116ms with 1 MB buffer. Reading time does not depend on
        # buffer size.
        self.fh = open(filename, "w+b", 0)
        self.dirty = False
        self.inode = inode
        self.blockno = blockno
        self.last_access = 0
        self.pos = 0
        self.size = os.fstat(self.fh.fileno()).st_size

    def read(self, size=None):
        buf = self.fh.read(size)
        self.pos += len(buf)
        return buf

    def flush(self):
        self.fh.flush()

    def seek(self, off):
        if self.pos != off:
            self.fh.seek(off)
            self.pos = off

    def tell(self):
        return self.pos

    def truncate(self, size=None):
        self.dirty = True
        self.fh.truncate(size)
        if size is None:
            if self.pos < self.size:
                self.size = self.pos
        elif size < self.size:
            self.size = size

    def write(self, buf):
        self.dirty = True
        self.fh.write(buf)
        self.pos += len(buf)
        self.size = max(self.pos, self.size)

    def close(self):
        self.fh.close()

    def unlink(self):
        os.unlink(self.fh.name)

    def __str__(self):
        return ('<%sCacheEntry, inode=%d, blockno=%d>'
                % ('Dirty ' if self.dirty else '', self.inode, self.blockno))


class CacheDict(OrderedDict):
    '''
    An ordered dictionary designed to store CacheEntries.

    Attributes:

    :max_size: maximum size to which cache can grow
    :max_entries: maximum number of entries in cache
    :size: current size of all entries together
    '''

    def __init__(self, max_size, max_entries):
        super().__init__()
        self.max_size = max_size
        self.max_entries = max_entries
        self.size = 0

    def remove(self, key):
        '''Remove *key* from disk and cache, update size'''

        el = self.pop(key)
        el.close()
        el.unlink()
        self.size -= el.size

    def is_full(self):
        return (self.size > self.max_size
                or len(self) > self.max_entries)

class BlockCache(object):
    """Provides access to file blocks

    This class manages access to file blocks. It takes care of creation,
    uploading, downloading and deduplication.

    This class uses the llfuse global lock. Methods which release the lock have
    are marked as such in their docstring.

    Attributes
    ----------

    :path: where cached data is stored
    :cache: ordered dictionary of cache entries
    :mlock: MultiLock to synchronize access to objects and cache entries
    :in_transit: set of cache entries that are currently being uploaded
    :to_upload: distributes objects to upload to worker threads
    :to_remove: distributes objects to remove to worker threads
    :transfer_complete: signals completion of an object upload
    :upload_threads: list of threads processing upload queue
    :removal_threads: list of threads processing removal queue
    :db: Handle to SQL DB
    :backend_pool: BackendPool instance
    """

    def __init__(self, backend_pool, db, cachedir, max_size, max_entries=768):
        log.debug('Initializing')

        self.path = cachedir
        self.db = db
        self.backend_pool = backend_pool
        self.cache = CacheDict(max_size, max_entries)
        self.mlock = MultiLock()
        self.in_transit = set()
        self.upload_threads = []
        self.removal_threads = []
        self.transfer_completed = SimpleEvent()

        # Will be initialized once threads are available
        self.to_upload = None
        self.to_remove = None

        if not os.path.exists(self.path):
            os.mkdir(self.path)

    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.cache)

    def init(self, threads=1):
        '''Start worker threads'''

        self.to_upload = Distributor()
        for _ in range(threads):
            t = threading.Thread(target=self._upload_loop)
            t.start()
            self.upload_threads.append(t)

        self.to_remove = Queue(1000)
        for _ in range(10):
            t = threading.Thread(target=self._removal_loop)
            t.daemon = True # interruption will do no permanent harm
            t.start()
            self.removal_threads.append(t)

    def _lock_obj(self, obj_id, release_global=False):
        '''Acquire lock on *obj*id*'''

        if release_global:
            with lock_released:
                self.mlock.acquire(obj_id)
        else:
            self.mlock.acquire(obj_id)

    def _unlock_obj(self, obj_id, release_global=False, noerror=False):
        '''Release lock on *obj*id*'''

        if release_global:
            with lock_released:
                self.mlock.release(obj_id, noerror=noerror)
        else:
            self.mlock.release(obj_id, noerror=noerror)

    def _lock_entry(self, inode, blockno, release_global=False):
        '''Acquire lock on cache entry'''

        if release_global:
            with lock_released:
                self.mlock.acquire((inode, blockno))
        else:
            self.mlock.acquire((inode, blockno))

    def _unlock_entry(self, inode, blockno, release_global=False,
                      noerror=False):
        '''Release lock on cache entry'''

        if release_global:
            with lock_released:
                self.mlock.release((inode, blockno), noerror=noerror)
        else:
            self.mlock.release((inode, blockno), noerror=noerror)

    def destroy(self):
        '''Clean up and stop worker threads

        This method should be called without the global lock held.
        '''

        log.debug('clearing cache...')
        try:
            with lock:
                self.clear()
        except NoWorkerThreads:
            log.error('Unable to flush cache, no upload threads left alive')

        # Signal termination to worker threads. If some of them
        # terminated prematurely, continue gracefully.
        log.debug('Signaling upload threads...')
        try:
            for t in self.upload_threads:
                self._queue_upload(QuitSentinel)
        except NoWorkerThreads:
            pass

        log.debug('Signaling removal threads...')
        try:
            for t in self.removal_threads:
                self._queue_removal(QuitSentinel)
        except NoWorkerThreads:
            pass

        log.debug('waiting for upload threads...')
        for t in self.upload_threads:
            t.join()

        log.debug('waiting for removal threads...')
        for t in self.removal_threads:
            t.join()

        assert len(self.in_transit) == 0
        try:
            while self.to_remove.get_nowait() is QuitSentinel:
                pass
        except QueueEmpty:
            pass
        else:
            log.error('Could not complete object removals, '
                      'no removal threads left alive')

        self.to_upload = None
        self.to_remove = None
        self.upload_threads = None
        self.removal_threads = None

        os.rmdir(self.path)

        log.debug('cleanup done.')


    def _upload_loop(self):
        '''Process upload queue'''

        while True:
            tmp = self.to_upload.get()

            if tmp is QuitSentinel:
                break

            self._do_upload(*tmp)


    def _do_upload(self, el, obj_id):
        '''Upload object'''

        def do_write(fh):
            el.seek(0)
            while True:
                buf = el.read(BUFSIZE)
                if not buf:
                    break
                fh.write(buf)
            return fh

        try:
            with self.backend_pool() as backend:
                if log.isEnabledFor(logging.DEBUG):
                    time_ = time.time()
                    obj_size = backend.perform_write(do_write, 's3ql_data_%d'
                                                     % obj_id).get_obj_size()
                    time_ = time.time() - time_
                    rate = el.size / (1024 ** 2 * time_) if time_ != 0 else 0
                    log.debug('_do_upload(%s): uploaded %d bytes in %.3f seconds, %.2f MiB/s',
                              obj_id, el.size, time_, rate)
                else:
                    obj_size = backend.perform_write(do_write, 's3ql_data_%d'
                                                     % obj_id).get_obj_size()

            with lock:
                self.db.execute('UPDATE objects SET size=? WHERE id=?', (obj_size, obj_id))
                el.dirty = False

        except Exception as exc:
            log.debug('upload of %d failed: %s', obj_id, exc)
            # At this point we have to remove references to this storage object
            # from the objects and blocks table to prevent future cache elements
            # to be de-duplicated against this (missing) one. However, this may
            # already have happened during the attempted upload. The only way to
            # avoid this problem is to insert the hash into the blocks table
            # *after* successfull upload. But this would open a window without
            # de-duplication just to handle the special case of an upload
            # failing.
            #
            # On the other hand, we also want to prevent future deduplication
            # against this block: otherwise the next attempt to upload the same
            # cache element (by a different upload thread that has not
            # encountered problems yet) is guaranteed to link against the
            # non-existing block, and the data will be lost.
            #
            # Therefore, we just set the hash of the missing block to NULL,
            # and rely on fsck to pick up the pieces. Note that we cannot
            # delete the row from the blocks table, because the id will get
            # assigned to a new block, so the inode_blocks entries will
            # refer to incorrect data.
            #

            with lock:
                self.db.execute('UPDATE blocks SET hash=NULL WHERE obj_id=?',
                                (obj_id,))
            raise

        finally:
            self.in_transit.remove(el)
            self._unlock_obj(obj_id)
            self._unlock_entry(el.inode, el.blockno)
            self.transfer_completed.notify_all()


    def wait(self):
        '''Wait until an object has been uploaded

        If there are no objects in transit, return immediately. This method
        releases the global lock.
        '''

        if not self.transfer_in_progress():
            return

        with lock_released:
            self.transfer_completed.wait()

    def upload(self, el):
        '''Upload cache entry `el` asynchronously

        This method releases the global lock.
        '''

        log.debug('upload(%s): start', el)

        # Calculate checksum
        with lock_released:
            try:
                self._lock_entry(el.inode, el.blockno)
                assert el not in self.in_transit
                if not el.dirty:
                    log.debug('upload(%s): not dirty, returning', el)
                    self._unlock_entry(el.inode, el.blockno)
                    return
                if (el.inode, el.blockno) not in self.cache:
                    log.debug('%s removed while waiting for lock', el)
                    self._unlock_entry(el.inode, el.blockno)
                    return

                self.in_transit.add(el)
                sha = hashlib.sha256()
                el.seek(0)
                while True:
                    buf = el.read(BUFSIZE)
                    if not buf:
                        break
                    sha.update(buf)
                hash_ = sha.digest()
            except:
                self.in_transit.discard(el)
                self._unlock_entry(el.inode, el.blockno)
                raise

        try:
            try:
                old_block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
                                               'WHERE inode=? AND blockno=?',
                                               (el.inode, el.blockno))
            except NoSuchRowError:
                old_block_id = None

            try:
                block_id = self.db.get_val('SELECT id FROM blocks WHERE hash=?', (hash_,))

            # No block with same hash
            except NoSuchRowError:
                obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, -1)')
                log.debug('upload(%s): created new object %d', el, obj_id)
                block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, hash, size) '
                                         'VALUES(?,?,?,?)', (1, obj_id, hash_, el.size))
                log.debug('upload(%s): created new block %d', el, block_id)
                log.debug('upload(%s): adding to upload queue', el)

                # Note: we must finish all db transactions before adding to
                # in_transit, otherwise commit() may return before all blocks
                # are available in db.
                self.db.execute('INSERT OR REPLACE INTO inode_blocks (block_id, inode, blockno) '
                                'VALUES(?,?,?)', (block_id, el.inode, el.blockno))

                with lock_released:
                    self._lock_obj(obj_id)
                    self._queue_upload((el, obj_id))

            # There is a block with the same hash
            else:
                if old_block_id != block_id:
                    log.debug('upload(%s): (re)linking to %d', el, block_id)
                    self.db.execute('UPDATE blocks SET refcount=refcount+1 WHERE id=?',
                                    (block_id,))
                    self.db.execute('INSERT OR REPLACE INTO inode_blocks (block_id, inode, blockno) '
                                    'VALUES(?,?,?)', (block_id, el.inode, el.blockno))

                el.dirty = False
                self.in_transit.remove(el)
                self._unlock_entry(el.inode, el.blockno, release_global=True)

                if old_block_id == block_id:
                    log.debug('upload(%s): unchanged, block_id=%d', el, block_id)
                    return

        except:
            self.in_transit.discard(el)
            with lock_released:
                self._unlock_entry(el.inode, el.blockno, noerror=True)
                self._unlock_obj(obj_id, noerror=True)
            raise

        # Check if we have to remove an old block
        if not old_block_id:
            log.debug('upload(%s): no old block, returning', el)
            return

        self._deref_block(old_block_id)

    def _queue_upload(self, obj):
        '''Put *obj* into upload queue'''

        while True:
            if self.to_upload.put(obj, timeout=5):
                return
            for t in self.upload_threads:
                if t.is_alive():
                    break
            else:
                raise NoWorkerThreads('no upload threads')

    def _queue_removal(self, obj):
        '''Put *obj* into removal queue'''

        while True:
            try:
                self.to_remove.put(obj, timeout=5)
            except QueueFull:
                pass
            else:
                return

            for t in self.removal_threads:
                if t.is_alive():
                    break
            else:
                raise NoWorkerThreads('no removal threads')

    def _deref_block(self, block_id):
        '''Decrease reference count for *block_id*

        If reference counter drops to zero, remove block and propagate
        to objects table (possibly removing the referenced object
        as well).

        This method releases the global lock.
        '''

        refcount = self.db.get_val('SELECT refcount FROM blocks WHERE id=?', (block_id,))
        if refcount > 1:
            log.debug('decreased refcount for block: %d', block_id)
            self.db.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?', (block_id,))
            return

        log.debug('removing block %d', block_id)
        obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
        self.db.execute('DELETE FROM blocks WHERE id=?', (block_id,))
        (refcount, size) = self.db.get_row('SELECT refcount, size FROM objects WHERE id=?',
                                           (obj_id,))
        if refcount > 1:
            log.debug('decreased refcount for obj: %d', obj_id)
            self.db.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                            (obj_id,))
            return

        log.debug('removing object %d', obj_id)
        self.db.execute('DELETE FROM objects WHERE id=?', (obj_id,))

        # Taking the lock ensures that the object is no longer in
        # transit itself. We can release it immediately after, because
        # the object is no longer in the database.
        log.debug('adding %d to removal queue', obj_id)

        with lock_released:
            self._lock_obj(obj_id)
            self._unlock_obj(obj_id)

            if size == -1:
                # size == -1 indicates that object has not yet been uploaded.
                # However, since we just acquired a lock on the object, we know
                # that the upload must have failed. Therefore, trying to remove
                # this object would just give us another error.
                return

            self._queue_removal(obj_id)


    def transfer_in_progress(self):
        '''Return True if there are any cache entries being uploaded'''

        return len(self.in_transit) > 0

    def _removal_loop(self):
        '''Process removal queue'''

        # This method may look more complicated than necessary, but
        # it ensures that we read as many objects from the queue
        # as we can without blocking, and then hand them over to
        # the backend all at once.
        ids = []
        while True:
            try:
                log.debug('reading from queue (blocking=%s)', len(ids)==0)
                tmp = self.to_remove.get(block=len(ids)==0)
            except QueueEmpty:
                tmp = FlushSentinel

            if (tmp is FlushSentinel or tmp is QuitSentinel) and ids:
                log.debug('removing: %s', ids)
                with self.backend_pool() as backend:
                    backend.delete_multi(['s3ql_data_%d' % i for i in ids])
                ids = []
            else:
                ids.append(tmp)

            if tmp is QuitSentinel:
                break


    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for block `blockno` of `inode`

        This method releases the global lock. The managed block, however,
        is executed with the global lock acquired and MUST NOT release
        it. This ensures that only one thread is accessing a given block
        at a time.

        Note: if `get` and `remove` are called concurrently, then it is
        possible that a block that has been requested with `get` and
        passed to `remove` for deletion will not be deleted.
        """

        #log.debug('get(inode=%d, block=%d): start', inode, blockno)

        if self.cache.is_full():
            self.expire()

        self._lock_entry(inode, blockno, release_global=True)
        try:
            el = self._get_entry(inode, blockno)
            el.last_access = time.time()
            oldsize = el.size
            try:
                yield el
            finally:
                # Update cachesize. NOTE: this requires that at most one
                # thread has access to a cache entry at any time.
                self.cache.size += el.size - oldsize
        finally:
            self._unlock_entry(inode, blockno, release_global=True)

        #log.debug('get(inode=%d, block=%d): end', inode, blockno)

    def _get_entry(self, inode, blockno):
        '''Get cache entry for `blockno` of `inode`

        Assume that cache entry lock has been acquired.
        '''

        try:
            el = self.cache[(inode, blockno)]

        # Not in cache
        except KeyError:
            filename = os.path.join(self.path, '%d-%d' % (inode, blockno))
            try:
                block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
                                           'WHERE inode=? AND blockno=?', (inode, blockno))

            # No corresponding object
            except NoSuchRowError:
                log.debug('_get_entry(inode=%d, block=%d): creating new block', inode, blockno)
                el = CacheEntry(inode, blockno, filename)
                self.cache[(inode, blockno)] = el
                return el

            # Need to download corresponding object
            obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
            log.debug('_get_entry(inode=%d, block=%d): downloading object %d..',
                      inode, blockno, obj_id)
            el = CacheEntry(inode, blockno, filename)
            try:
                def do_read(fh):
                    el.seek(0)
                    el.truncate()
                    shutil.copyfileobj(fh, el, BUFSIZE)

                with lock_released:
                    # Lock object. This ensures that we wait until the object
                    # is uploaded. We don't have to worry about deletion, because
                    # as long as the current cache entry exists, there will always be
                    # a reference to the object (and we already have a lock on the
                    # cache entry).
                    self._lock_obj(obj_id)
                    self._unlock_obj(obj_id)
                    with self.backend_pool() as backend:
                        backend.perform_read(do_read, 's3ql_data_%d' % obj_id)
            except:
                el.unlink()
                el.close()
                raise

            self.cache[(inode, blockno)] = el
            el.dirty = False # (writing will have set dirty flag)
            self.cache.size += el.size

        # In Cache
        else:
            #log.debug('_get_entry(inode=%d, block=%d): in cache', inode, blockno)
            self.cache.move_to_end((inode, blockno), last=True) # move to head

        return el

    def expire(self):
        """Perform cache expiry

        This method releases the global lock.
        """

        # Note that we have to make sure that the cache entry is written into
        # the database before we remove it from the cache!

        log.debug('expire: start')

        while True:
            need_size = self.cache.size - self.cache.max_size
            need_entries = len(self.cache) - self.cache.max_entries

            if need_size <= 0 and need_entries <= 0:
                break

            # Need to make copy, since we aren't allowed to change dict while
            # iterating through it. Look at the comments in CommitThread.run()
            # (mount.py) for an estimate of the resulting performance hit.
            sth_in_transit = False
            for el in list(self.cache.values()):
                if need_size <= 0 and need_entries <= 0:
                    break

                need_entries -= 1
                need_size -= el.size

                if el.dirty:
                    if el not in self.in_transit:
                        log.debug('expire: uploading %s..', el)
                        self.upload(el) # Releases global lock
                    sth_in_transit = True
                    continue

                log.debug('removing inode %d, block %d from cache', el.inode, el.blockno)
                self._lock_entry(el.inode, el.blockno, release_global=True)
                try:
                    # May have changed while we were waiting for lock
                    if el.dirty:
                        log.debug('%s got dirty while waiting for lock', el)
                        continue
                    if (el.inode, el.blockno) not in self.cache:
                        log.debug('%s removed while waiting for lock', el)
                        continue
                    self.cache.remove((el.inode, el.blockno))
                finally:
                    self._unlock_entry(el.inode, el.blockno, release_global=True)

            if sth_in_transit:
                log.debug('expire: waiting for transfer threads..')
                self.wait() # Releases global lock

        log.debug('expire: end')


    def remove(self, inode, start_no, end_no=None):
        """Remove blocks for `inode`

        If `end_no` is not specified, remove just the `start_no` block.
        Otherwise removes all blocks from `start_no` to, but not including,
         `end_no`.

        This method releases the global lock.
        """

        log.debug('remove(inode=%d, start=%d, end=%s): start', inode, start_no, end_no)

        if end_no is None:
            end_no = start_no + 1

        for blockno in range(start_no, end_no):
            self._lock_entry(inode, blockno, release_global=True)
            try:
                if (inode, blockno) in self.cache:
                    log.debug('remove(inode=%d, blockno=%d): removing from cache',
                              inode, blockno)
                    self.cache.remove((inode, blockno))

                try:
                    block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
                                               'WHERE inode=? AND blockno=?', (inode, blockno))
                except NoSuchRowError:
                    log.debug('remove(inode=%d, blockno=%d): block not in db', inode, blockno)
                    continue

                # Detach inode from block
                self.db.execute('DELETE FROM inode_blocks WHERE inode=? AND blockno=?',
                                (inode, blockno))

            finally:
                self._unlock_entry(inode, blockno, release_global=True)

            # Decrease block refcount
            self._deref_block(block_id)

        log.debug('remove(inode=%d, start=%d, end=%s): end', inode, start_no, end_no)

    def flush(self, inode, blockno):
        """Flush buffers for given block"""

        try:
            el = self.cache[(inode, blockno)]
        except KeyError:
            return

        el.flush()

    def commit(self):
        """Initiate upload of all dirty blocks

        When the method returns, all blocks have been registered
        in the database (but the actual uploads may still be
        in progress).

        This method releases the global lock.
        """

        # Need to make copy, since we aren't allowed to change dict while
        # iterating through it. Look at the comments in CommitThread.run()
        # (mount.py) for an estimate of the resulting performance hit.
        for el in list(self.cache.values()):
            if not el.dirty or el in self.in_transit:
                continue

            self.upload(el) # Releases global lock

    def clear(self):
        """Clear cache

        This method releases the global lock.
        """

        log.debug('clear: start')
        bak = self.cache.max_entries
        self.cache.max_entries = 0
        self.expire() # Releases global lock
        self.cache.max_entries = bak

        log.debug('clear: end')

    def get_usage(self):
        '''Return cache size and dirty cache size

        This method is O(n) in the number of cache entries.
        '''

        used = self.cache.size
        dirty = 0
        for el in self.cache.values():
            if el.dirty:
                dirty += el.size

        return (used, dirty)
    
    def __del__(self):
        if len(self.cache) > 0:
            raise RuntimeError("BlockManager instance was destroyed without calling destroy()!")
