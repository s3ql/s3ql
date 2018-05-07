#!/usr/bin/env python3
'''
t2_block_cache.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys
    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from contextlib import contextmanager
from s3ql.backends import local
from s3ql.backends.common import AbstractBackend
from s3ql.backends.pool import BackendPool
from s3ql.block_cache import BlockCache, QuitSentinel
from s3ql.mkfs import init_tables
from s3ql.metadata import create_tables
from s3ql.database import Connection
from s3ql.common import AsyncFn, time_ns
import s3ql.block_cache
from argparse import Namespace
from common import safe_sleep
from pytest_checklogs import assert_logs
from unittest.mock import patch
import errno
import os
import logging
import shutil
import stat
import tempfile
import threading
import unittest
import queue
import pytest

log = logging.getLogger(__name__)

# A dummy removal queue to monkeypatch around the need for removal and upload
# threads
class DummyQueue:
    def __init__(self, cache):
        self.obj = None
        self.cache = cache

    def get_nowait(self):
        return self.get(block=False)

    def put(self, obj, timeout=None):
        self.obj = obj
        self.cache._removal_loop()
        return True

    def get(self, block=True):
        if self.obj is None:
            raise queue.Empty()
        elif self.obj is QuitSentinel:
            self.obj = None
            return QuitSentinel
        else:
            tmp = self.obj
            self.obj = QuitSentinel
            return tmp

    def qsize(self):
        return 0

class cache_tests(unittest.TestCase):

    def setUp(self):

        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.backend_pool = BackendPool(lambda: local.Backend(
            Namespace(storage_url='local://' + self.backend_dir)))

        self.cachedir = tempfile.mkdtemp(prefix='s3ql-cache-')
        self.max_obj_size = 1024

        # Destructors are not guaranteed to run, and we can't unlink
        # the file immediately because apsw refers to it by name.
        # Therefore, we unlink the file manually in tearDown()
        self.dbfile = tempfile.NamedTemporaryFile(delete=False)
        self.db = Connection(self.dbfile.name)
        create_tables(self.db)
        init_tables(self.db)

        # Create an inode we can work with
        self.inode = 42
        now_ns = time_ns()
        self.db.execute("INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount,size) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                         | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                         os.getuid(), os.getgid(), now_ns, now_ns, now_ns, 1, 32))

        cache = BlockCache(self.backend_pool, self.db, self.cachedir + "/cache",
                           self.max_obj_size * 100)
        self.cache = cache

        # Monkeypatch around the need for removal and upload threads
        cache.to_remove = DummyQueue(cache)

        class DummyDistributor:
            def put(self, arg, timeout=None):
                cache._do_upload(*arg)
                return True
        cache.to_upload = DummyDistributor()

        # Tested methods assume that they are called from
        # file system request handler
        s3ql.block_cache.lock = MockLock()
        s3ql.block_cache.lock_released = MockLock()

    def tearDown(self):
        self.cache.backend_pool = self.backend_pool
        self.cache.destroy()
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.backend_dir)
        self.dbfile.close()
        os.unlink(self.dbfile.name)

    def test_thread_hang(self):
        # Make sure that we don't deadlock if uploads threads or removal
        # threads have died and we try to expire or terminate

        # Monkeypatch to avoid error messages about uncaught exceptions
        # in other threads
        upload_exc = False
        removal_exc = False
        def _upload_loop(*a, fn=self.cache._upload_loop):
            try:
                return fn(*a)
            except NotADirectoryError:
                nonlocal upload_exc
                upload_exc = True
        def _removal_loop(*a, fn=self.cache._removal_loop):
            try:
                return fn(*a)
            except NotADirectoryError:
                nonlocal removal_exc
                removal_exc = True
        self.cache._upload_loop = _upload_loop
        self.cache._removal_loop = _removal_loop

        # Start threads
        self.cache.init(threads=3)

        # Create first object (we'll try to remove that)
        with self.cache.get(self.inode, 0) as fh:
            fh.write(b'bar wurfz!')
        self.cache.start_flush()
        self.cache.wait()

        # Make sure that upload and removal will fail
        os.rename(self.backend_dir, self.backend_dir + '-tmp')
        open(self.backend_dir, 'w').close()

        # Create second object (we'll try to upload that)
        with self.cache.get(self.inode, 1) as fh:
            fh.write(b'bar wurfz number two!')

        # Schedule a removal
        self.cache.remove(self.inode, 0)

        try:
            # Try to clean-up (implicitly calls expire)
            with assert_logs('Unable to drop cache, no upload threads left alive',
                              level=logging.ERROR, count=1):
                with pytest.raises(OSError) as exc_info:
                     self.cache.destroy()
                assert exc_info.value.errno == errno.ENOTEMPTY
            assert upload_exc
            assert removal_exc
        finally:
            # Fix backend dir
            os.unlink(self.backend_dir)
            os.rename(self.backend_dir + '-tmp', self.backend_dir)

            # Remove objects from cache and make final destroy
            # call into no-op.
            self.cache.remove(self.inode, 1)
            self.cache.destroy = lambda: None


    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fh:
            return fh.read(len_)

    def test_get(self):
        inode = self.inode
        blockno = 11
        data = self.random_data(int(0.5 * self.max_obj_size))

        # Case 1: Object does not exist yet
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            fh.write(data)

        # Case 2: Object is in cache
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))

        # Case 3: Object needs to be downloaded
        self.cache.drop()
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))

    def test_expire(self):
        inode = self.inode

        # Define the 4 most recently accessed ones
        most_recent = [7, 11, 10, 8]
        for i in most_recent:
            safe_sleep(0.2)
            with self.cache.get(inode, i) as fh:
                fh.write(('%d' % i).encode())

        # And some others
        for i in range(20):
            if i in most_recent:
                continue
            with self.cache.get(inode, i) as fh:
                fh.write(('%d' % i).encode())

        # Flush the 2 most recently accessed ones
        start_flush(self.cache, inode, most_recent[-2])
        start_flush(self.cache, inode, most_recent[-3])

        # We want to expire 4 entries, 2 of which are already flushed
        self.cache.cache.max_entries = 16
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_write=2)
        self.cache.expire()
        self.cache.backend_pool.verify()
        self.assertEqual(len(self.cache.cache), 16)

        for i in range(20):
            if i in most_recent:
                self.assertTrue((inode, i) not in self.cache.cache)
            else:
                self.assertTrue((inode, i) in self.cache.cache)

    def test_upload(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.cache.max_size)
        blockno1 = 21
        blockno2 = 25
        blockno3 = 7

        data1 = self.random_data(datalen)
        data2 = self.random_data(datalen)
        data3 = self.random_data(datalen)

        # Case 1: create new object
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
            el1 = fh
        assert self.cache.upload_if_dirty(el1)
        self.cache.backend_pool.verify()

        # Case 2: Link new object
        self.cache.backend_pool = MockBackendPool(self.backend_pool)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data1)
            el2 = fh
        assert not self.cache.upload_if_dirty(el2)
        self.cache.backend_pool.verify()

        # Case 3: Upload old object, still has references
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data2)
        assert self.cache.upload_if_dirty(el1)
        self.cache.backend_pool.verify()

        # Case 4: Upload old object, no references left
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_del=1, no_write=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data3)
        assert self.cache.upload_if_dirty(el2)
        self.cache.backend_pool.verify()

        # Case 5: Link old object, no references left
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_del=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data2)
        assert not self.cache.upload_if_dirty(el2)
        self.cache.backend_pool.verify()

        # Case 6: Link old object, still has references
        # (Need to create another object first)
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno3) as fh:
            fh.seek(0)
            fh.write(data1)
            el3 = fh
        assert self.cache.upload_if_dirty(el3)
        self.cache.backend_pool.verify()

        self.cache.backend_pool = MockBackendPool(self.backend_pool)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
        assert not self.cache.upload_if_dirty(el1)
        self.cache.drop()
        self.cache.backend_pool.verify()

    def test_remove_referenced(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.cache.max_size)
        blockno1 = 21
        blockno2 = 24
        data = self.random_data(datalen)

        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data)
        self.cache.drop()
        self.cache.backend_pool.verify()

        self.cache.backend_pool = MockBackendPool(self.backend_pool)
        self.cache.remove(inode, blockno1)
        self.cache.backend_pool.verify()

    def test_remove_cache(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.max_obj_size))

        # Case 1: Elements only in cache
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.remove(inode, 1)
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertEqual(fh.read(42), b'')

    def test_upload_race(self):
        inode = self.inode
        blockno = 1
        data1 = self.random_data(int(0.4 * self.max_obj_size))
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            fh.write(data1)

        # Remove it
        self.cache.remove(inode, blockno)

        # Try to upload it, may happen if CommitThread is interrupted
        self.cache.upload_if_dirty(fh)

    def test_expire_race(self):
        # Create element
        inode = self.inode
        blockno = 1
        data1 = self.random_data(int(0.4 * self.max_obj_size))
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            fh.write(data1)
        assert self.cache.upload_if_dirty(fh)

        # Make sure entry will be expired
        self.cache.cache.max_entries = 0

        # Lock it
        self.cache._lock_entry(inode, blockno, release_global=True)

        try:
            # Start expiration, will block on lock
            t1 = AsyncFn(self.cache.expire)
            t1.start()

            # Start second expiration, will block
            t2 = AsyncFn(self.cache.expire)
            t2.start()

            # Release lock
            self.cache._unlock_entry(inode, blockno)
            t1.join_and_raise()
            t2.join_and_raise()

            assert len(self.cache.cache) == 0
        finally:
                self.cache._unlock_entry(inode, blockno, release_global=True,
                                         noerror=True)


    def test_parallel_expire(self):
        # Create elements
        inode = self.inode
        for i in range(5):
            data1 = self.random_data(int(0.4 * self.max_obj_size))
            with self.cache.get(inode, i) as fh:
                fh.write(data1)

        # We want to expire just one element, but have
        # several threads running expire() simultaneously
        self.cache.cache.max_entries = 4

        # Lock first element so that we have time to start threads
        self.cache._lock_entry(inode, 0, release_global=True)

        try:
            # Start expiration, will block on lock
            t1 = AsyncFn(self.cache.expire)
            t1.start()

            # Start second expiration, will block
            t2 = AsyncFn(self.cache.expire)
            t2.start()

            # Release lock
            self.cache._unlock_entry(inode, 0)
            t1.join_and_raise()
            t2.join_and_raise()

            assert len(self.cache.cache) == 4
        finally:
                self.cache._unlock_entry(inode, 0, release_global=True,
                                         noerror=True)


    def test_remove_cache_db(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.max_obj_size))

        # Case 2: Element in cache and db
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_write=1)
        start_flush(self.cache, inode)
        self.cache.backend_pool.verify()
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_del=1)
        self.cache.remove(inode, 1)
        self.cache.backend_pool.verify()

        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertEqual(fh.read(42), b'')


    def test_remove_db(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.max_obj_size))

        # Case 3: Element only in DB
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_write=1)
        self.cache.drop()
        self.cache.backend_pool.verify()
        self.cache.backend_pool = MockBackendPool(self.backend_pool, no_del=1)
        self.cache.remove(inode, 1)
        self.cache.backend_pool.verify()
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertEqual(fh.read(42), b'')

    def test_issue_241(self):

        inode = self.inode

        # Create block
        with self.cache.get(inode, 0) as fh:
            fh.write(self.random_data(500))

        # "Fill" cache
        self.cache.cache.max_entries = 0

        # Mock locking to reproduce race condition
        mlock = MockMultiLock(self.cache.mlock)
        with patch.object(self.cache, 'mlock', mlock):
            # Start first expiration run, will block in upload
            thread1 = AsyncFn(self.cache.expire)
            thread1.start()

            # Remove the object while the expiration thread waits
            # for it to become available.
            thread2 = AsyncFn(self.cache.remove, inode, 0, 1)
            thread2.start()
            mlock.yield_to(thread2)
            thread2.join_and_raise(timeout=10)
            assert not thread2.is_alive()

        # Create a new object for the same block
        with self.cache.get(inode, 0) as fh:
            fh.write(self.random_data(500))

        # Continue first expiration run
        mlock.yield_to(thread1, block=False)
        thread1.join_and_raise(timeout=10)
        assert not thread1.is_alive()


class MockMultiLock:
    def __init__(self, real_mlock):
        self.cond = real_mlock.cond
        self.cleared = set()
        self.real_mlock = real_mlock

    def yield_to(self, thread, block=True):
        '''Allow *thread* to proceed'''

        me = threading.current_thread()
        log.debug('%s blocked in yield_to(), phase 1', me.name)
        with self.cond:
            self.cleared.add(thread)
            self.cond.notify_all()

        if not block:
            return
        log.debug('%s blocked in yield_to(), phase 2', me.name)
        with self.cond:
            if not self.cond.wait_for(lambda: thread not in self.cleared, 10):
                pytest.fail('Timeout waiting for lock')
        log.debug('%s completed yield_to()', me.name)

    @contextmanager
    def __call__(self, *key):
        self.acquire(*key)
        try:
            yield
        finally:
            self.release(*key)

    def acquire(self, *key, timeout=None):
        me = threading.current_thread()
        log.debug('%s blocked in acquire()', me.name)
        with self.cond:
            if not self.cond.wait_for(lambda: me in self.cleared, 10):
                pytest.fail('Timeout waiting for lock')
            self.real_mlock.locked_keys.add(key)
        log.debug('%s got lock', me.name)

    def release(self, *key, noerror=False):
        me = threading.current_thread()
        log.debug('%s blocked in release()', me.name)
        with self.cond:
            self.cleared.remove(me)
            self.cond.notify_all()
            if noerror:
                self.real_mlock.locked_keys.discard(key)
            else:
                self.real_mlock.locked_keys.remove(key)
        log.debug('%s released lock', me.name)


class MockBackendPool(AbstractBackend):
    has_native_rename = False

    def __init__(self, backend_pool, no_read=0, no_write=0, no_del=0):
        super().__init__()
        self.no_read = no_read
        self.no_write = no_write
        self.no_del = no_del
        self.backend_pool = backend_pool
        self.backend = backend_pool.pop_conn()
        self.lock = threading.Lock()

    def __del__(self):
        self.backend_pool.push_conn(self.backend)

    def verify(self):
        if self.no_read != 0:
            raise RuntimeError('Got too few open_read calls')
        if self.no_write != 0:
            raise RuntimeError('Got too few open_write calls')
        if self.no_del != 0:
            raise RuntimeError('Got too few delete calls')

    @contextmanager
    def __call__(self):
        '''Provide connection from pool (context manager)'''

        with self.lock:
            yield self

    def lookup(self, key):
        return self.backend.lookup(key)

    def open_read(self, key):
        self.no_read -= 1
        if self.no_read < 0:
            raise RuntimeError('Got too many open_read calls')

        return self.backend.open_read(key)

    def open_write(self, key, metadata=None, is_compressed=False):
        self.no_write -= 1
        if self.no_write < 0:
            raise RuntimeError('Got too many open_write calls')

        return self.backend.open_write(key, metadata, is_compressed)

    def is_temp_failure(self, exc):
        return self.backend.is_temp_failure(exc)

    def clear(self):
        return self.backend.clear()

    def contains(self, key):
        return self.backend.contains(key)

    def delete(self, key, force=False):
        self.no_del -= 1
        if self.no_del < 0:
            raise RuntimeError('Got too many delete calls')

        return self.backend.delete(key, force)

    def list(self, prefix=''):
        '''List keys in backend

        Returns an iterator over all keys in the backend.
        '''
        return self.backend.list(prefix)

    def copy(self, src, dest, metadata=None):
        return self.backend.copy(src, dest, metadata)

    def rename(self, src, dest, metadata=None):
        return self.backend.rename(src, dest, metadata)

    def update_meta(self, key, metadata):
        return self.backend.update_meta(key, metadata)

    def get_size(self, key):
        '''Return size of object stored under *key*'''

        return self.backend.get_size(key)

def start_flush(cache, inode, block=None):
    """Upload data for `inode`

    This is only for testing purposes, since the method blocks until all current
    uploads have been completed.
    """

    for el in cache.cache.values():
        if el.inode != inode:
            continue
        if not el.dirty:
            continue

        if block is not None and el.blockno != block:
            continue

        cache.upload_if_dirty(el)


class MockLock():
    def __call__(self):
        return self

    def acquire(self, timeout=None):
        pass

    def release(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass
