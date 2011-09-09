'''
t2_block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function
from _common import TestCase
from contextlib import contextmanager
from s3ql.backends import local
from s3ql.backends.common import BucketPool, AbstractBucket
from s3ql.block_cache import BlockCache
from s3ql.common import create_tables, init_tables
from s3ql.database import Connection
import llfuse
import os
import shutil
import stat
import tempfile
import threading
import time
import unittest2 as unittest


class cache_tests(TestCase):

    def setUp(self):

        self.bucket_dir = tempfile.mkdtemp()
        self.bucket_pool = BucketPool(lambda: local.Bucket(self.bucket_dir, None, None))

        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024
        
        self.dbfile = tempfile.NamedTemporaryFile()
        self.db =  Connection(self.dbfile.name)
        create_tables(self.db)
        init_tables(self.db)

        # Create an inode we can work with
        self.inode = 42
        self.db.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount,size) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                         | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                         os.getuid(), os.getgid(), time.time(), time.time(), time.time(), 1, 32))

        self.cache = BlockCache(BucketPool(lambda: local.Bucket(self.bucket_dir, None, None)), 
                                self.db, self.cachedir, 100 * self.blocksize)
        self.cache.init()
        
        # Tested methods assume that they are called from
        # file system request handler
        llfuse.lock.acquire()
        
        # We do not want background threads
        self.cache.commit_thread.stop()


    def tearDown(self):
        self.cache.upload_manager.bucket_pool = self.bucket_pool
        self.cache.destroy()
        if os.path.exists(self.cachedir):
            shutil.rmtree(self.cachedir)
        shutil.rmtree(self.bucket_dir)
        
        llfuse.lock.release()

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fh:
            return fh.read(len_)

    def test_get(self):
        inode = self.inode
        blockno = 11
        data = self.random_data(int(0.5 * self.blocksize))

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
        self.cache.upload_manager.join_all()
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))


    def test_expire(self):
        inode = self.inode

        # Define the 4 most recently accessed ones
        most_recent = [7,11,10,8]
        for i in most_recent:
            time.sleep(0.2)
            with self.cache.get(inode, i) as fh:
                fh.write('%d' % i)
                
        # And some others
        for i in range(20):
            if i in most_recent:
                continue
            with self.cache.get(inode, i) as fh:
                fh.write('%d' % i)
            
        # Flush the 2 most recently accessed ones
        commit(self.cache, inode, most_recent[-2])
        commit(self.cache, inode, most_recent[-3])
        
        # We want to expire 4 entries, 2 of which are already flushed
        self.cache.max_entries = 16
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_write=2)
        self.cache.expire()
        self.cache.upload_manager.join_all()
        self.cache.upload_manager.bucket_pool.verify()
        self.assertEqual(len(self.cache.cache), 16)    
        
        for i in range(20):
            if i in most_recent:
                self.assertTrue((inode, i) not in self.cache.cache)
            else:
                self.assertTrue((inode, i) in self.cache.cache)
        
    def test_upload(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.max_size)
        blockno1 = 21
        blockno2 = 25
        blockno3 = 7

        data1 = self.random_data(datalen)
        data2 = self.random_data(datalen)
        data3 = self.random_data(datalen)
        
        mngr = self.cache.upload_manager

        # Case 1: create new object
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
            el1 = fh
        mngr.add(el1)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket_pool.verify()

        # Case 2: Link new object
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data1)
            el2 = fh
        mngr.add(el2)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket_pool.verify()

        # Case 3: Upload old object, still has references
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data2)
        mngr.add(el1)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket_pool.verify()


        # Case 4: Upload old object, no references left
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_del=1, no_write=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data3)
        mngr.add(el2)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket_pool.verify()

        # Case 5: Link old object, no references left
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_del=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data2)
        mngr.add(el2)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket_pool.verify()

        # Case 6: Link old object, still has references
        # (Need to create another object first)
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_write=1)
        with self.cache.get(inode, blockno3) as fh:
            fh.seek(0)
            fh.write(data1)
            el3 = fh
        mngr.add(el3)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket_pool.verify()

        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
        mngr.add(el1)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket_pool.verify()



    def test_remove_referenced(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.max_size)
        blockno1 = 21
        blockno2 = 24
        data = self.random_data(datalen)
        
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_write=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data)
        self.cache.clear()
        self.cache.upload_manager.join_all()
        self.cache.upload_manager.bucket_pool.verify()

        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool)
        self.cache.remove(inode, blockno1)
        self.cache.upload_manager.bucket_pool.verify()

    def test_remove_cache(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))

        # Case 1: Elements only in cache
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.remove(inode, 1)
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertTrue(fh.read(42) == '')

    def test_remove_cache_db(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))

        # Case 2: Element in cache and db 
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_write=1)
        commit(self.cache, inode)
        self.cache.upload_manager.bucket_pool.verify()
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_del=1)
        self.cache.remove(inode, 1)
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertTrue(fh.read(42) == '')

    def test_remove_db(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))

        # Case 3: Element only in DB
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_write=1)
        self.cache.clear()
        self.cache.upload_manager.join_all()
        self.cache.upload_manager.bucket_pool.verify()
        self.cache.upload_manager.bucket_pool = TestBucketPool(self.bucket_pool, no_del=1)
        self.cache.remove(inode, 1)
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertTrue(fh.read(42) == '')


class TestBucketPool(AbstractBucket):
    def __init__(self, bucket_pool, no_read=0, no_write=0, no_del=0):
        super(TestBucketPool, self).__init__()
        self.no_read = no_read
        self.no_write = no_write
        self.no_del = no_del
        self.bucket_pool = bucket_pool
        self.bucket = bucket_pool.pop_conn()
        self.lock = threading.Lock()

    def __del__(self):
        self.bucket_pool.push_conn(self.bucket)
        
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
        return self.bucket.lookup(key)
        
    def open_read(self, key):
        self.no_read -= 1
        if self.no_read < 0:
            raise RuntimeError('Got too many open_read calls')
        
        return self.bucket.open_read(key)

    def open_write(self, key, metadata=None):
        self.no_write -= 1
        if self.no_write < 0:
            raise RuntimeError('Got too many open_write calls')

        return self.bucket.open_write(key, metadata)
            
    def is_get_consistent(self):
        return self.bucket.is_get_consistent()
                    
    def is_list_create_consistent(self):
        return self.bucket.is_get_consistent()
    
    def clear(self):
        return self.bucket.clear()

    def contains(self, key):
        return self.bucket.contains(key)

    def delete(self, key, force=False):
        self.no_del -= 1
        if self.no_del < 0:
            raise RuntimeError('Got too many delete calls')
            
        return self.bucket.delete(key, force)

    def list(self, prefix=''):
        '''List keys in bucket

        Returns an iterator over all keys in the bucket.
        '''
        return self.bucket.list(prefix)

    def copy(self, src, dest):
        """Copy data stored under key `src` to key `dest`
        
        If `dest` already exists, it will be overwritten. The copying
        is done on the remote side. 
        """
        return self.bucket.copy(src, dest)

    def rename(self, src, dest):
        """Rename key `src` to `dest`
        
        If `dest` already exists, it will be overwritten. The rename
        is done on the remote side.
        """
        return self.bucket.rename(src, dest)


def commit(self, inode, block=None):
    """Upload data for `inode`
    
    This is only for testing purposes, since the method blocks
    until all current uploads have been completed. 
    """

    for el in self.cache.itervalues():
        if el.inode != inode:
            continue
        if not el.dirty:
            continue
        
        if block is not None and el.blockno != block:
            continue
        
        self.upload_manager.add(el)
        
    self.upload_manager.join_all()

        
def suite():
    return unittest.makeSuite(cache_tests)

if __name__ == "__main__":
    unittest.main()
