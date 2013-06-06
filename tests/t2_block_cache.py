'''
t2_block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from contextlib import contextmanager
from s3ql.backends import local
from s3ql.backends.common import BackendPool, AbstractBackend
from s3ql.block_cache import BlockCache
from s3ql.mkfs import init_tables
from s3ql.metadata import create_tables
from s3ql.database import Connection
import llfuse
import os
import shutil
import stat
import tempfile
import threading
import time
import unittest

class cache_tests(unittest.TestCase):

    def setUp(self):

        self.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
        self.backend_pool = BackendPool(lambda: local.Backend('local://' + self.backend_dir, 
                                                           None, None))

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
        self.db.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount,size) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                         | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                         os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 32))

        cache = BlockCache(self.backend_pool, self.db, self.cachedir + "/cache",
                           self.max_obj_size * 100)
        self.cache = cache
        
        # Monkeypatch around the need for removal and upload threads
        class DummyQueue:
            def put(self, obj):
                cache._do_removal(obj)
        cache.to_remove = DummyQueue()

        class DummyDistributor:
            def put(self, arg):
                cache._do_upload(*arg)
        cache.to_upload = DummyDistributor()
        
        # Tested methods assume that they are called from
        # file system request handler
        llfuse.lock.acquire()

    def tearDown(self):
        llfuse.lock.release()
        self.cache.backend_pool = self.backend_pool
        self.cache.destroy()
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.backend_dir)
        os.unlink(self.dbfile.name)
        
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
        self.cache.clear()
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))


    def test_expire(self):
        inode = self.inode

        # Define the 4 most recently accessed ones
        most_recent = [7, 11, 10, 8]
        for i in most_recent:
            time.sleep(0.2)
            with self.cache.get(inode, i) as fh:
                fh.write(('%d' % i).encode())

        # And some others
        for i in range(20):
            if i in most_recent:
                continue
            with self.cache.get(inode, i) as fh:
                fh.write(('%d' % i).encode())

        # Flush the 2 most recently accessed ones
        commit(self.cache, inode, most_recent[-2])
        commit(self.cache, inode, most_recent[-3])

        # We want to expire 4 entries, 2 of which are already flushed
        self.cache.max_entries = 16
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_write=2)
        self.cache.expire()
        self.cache.backend_pool.verify()
        self.assertEqual(len(self.cache.entries), 16)

        for i in range(20):
            if i in most_recent:
                self.assertTrue((inode, i) not in self.cache.entries)
            else:
                self.assertTrue((inode, i) in self.cache.entries)

    def test_upload(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.max_size)
        blockno1 = 21
        blockno2 = 25
        blockno3 = 7

        data1 = self.random_data(datalen)
        data2 = self.random_data(datalen)
        data3 = self.random_data(datalen)

        # Case 1: create new object
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
            el1 = fh
        self.cache.upload(el1)
        self.cache.backend_pool.verify()

        # Case 2: Link new object
        self.cache.backend_pool = TestBackendPool(self.backend_pool)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data1)
            el2 = fh
        self.cache.upload(el2)
        self.cache.backend_pool.verify()

        # Case 3: Upload old object, still has references
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data2)
        self.cache.upload(el1)
        self.cache.backend_pool.verify()

        # Case 4: Upload old object, no references left
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_del=1, no_write=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data3)
        self.cache.upload(el2)
        self.cache.backend_pool.verify()

        # Case 5: Link old object, no references left
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_del=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data2)
        self.cache.upload(el2)
        self.cache.backend_pool.verify()

        # Case 6: Link old object, still has references
        # (Need to create another object first)
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno3) as fh:
            fh.seek(0)
            fh.write(data1)
            el3 = fh
        self.cache.upload(el3)
        self.cache.backend_pool.verify()

        self.cache.backend_pool = TestBackendPool(self.backend_pool)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.upload(el1)
        self.cache.clear()
        self.cache.backend_pool.verify()

    def test_remove_referenced(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.max_size)
        blockno1 = 21
        blockno2 = 24
        data = self.random_data(datalen)

        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data)
        self.cache.clear()
        self.cache.backend_pool.verify()

        self.cache.backend_pool = TestBackendPool(self.backend_pool)
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

    def test_remove_cache_db(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.max_obj_size))

        # Case 2: Element in cache and db 
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_write=1)
        commit(self.cache, inode)
        self.cache.backend_pool.verify()
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_del=1)
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
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_write=1)
        self.cache.clear()
        self.cache.backend_pool.verify()
        self.cache.backend_pool = TestBackendPool(self.backend_pool, no_del=1)
        self.cache.remove(inode, 1)
        self.cache.backend_pool.verify()
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertEqual(fh.read(42), b'')

class TestBackendPool(AbstractBackend):
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

    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying
        is done on the remote side. 
        """
        return self.backend.copy(src, dest)

    def rename(self, src, dest):
        """Rename key `src` to `dest`
        
        If `dest` already exists, it will be overwritten. The rename
        is done on the remote side.
        """
        return self.backend.rename(src, dest)

    def get_size(self, key):
        '''Return size of object stored under *key*'''

        return self.backend.get_size(key)

def commit(cache, inode, block=None):
    """Upload data for `inode`
    
    This is only for testing purposes, since the method blocks until all current
    uploads have been completed.
    """

    for el in cache.entries.values():
        if el.inode != inode:
            continue
        if not el.dirty:
            continue

        if block is not None and el.blockno != block:
            continue

        cache.upload(el)
