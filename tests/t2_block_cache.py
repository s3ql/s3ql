'''
t2_block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from s3ql import mkfs
from s3ql.block_cache import BlockCache, UploadQueue
from s3ql.backends import local
from s3ql.database import ConnectionManager
import os
import tempfile
from _common import TestCase
import unittest2 as unittest
import stat
import threading
from time import time
import shutil

class cache_tests(TestCase):

    def setUp(self):

        self.bucket_dir = tempfile.mkdtemp()
        self.bucket = local.Connection().get_bucket(self.bucket_dir)

        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024
        cachesize = int(1.5 * self.blocksize)

        self.dbfile = tempfile.NamedTemporaryFile()
        self.dbcm = ConnectionManager(self.dbfile.name)
        with self.dbcm() as conn:
            mkfs.setup_tables(conn)
            mkfs.init_tables(conn)

        # Create an inode we can work with
        self.inode = 42
        self.dbcm.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount,size) "
                          "VALUES (?,?,?,?,?,?,?,?,?)",
                          (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                           | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                           os.getuid(), os.getgid(), time(), time(), time(), 1, 32))

        self.cache = BlockCache(self.bucket, self.cachedir, cachesize, self.dbcm)
        self.lock = threading.Lock()
        self.lock.acquire()

    def tearDown(self):
        self.cache.bucket = self.bucket
        self.cache.close()
        shutil.rmtree(self.cachedir)
        shutil.rmtree(self.bucket_dir)

    @staticmethod
    def random_data(len_):
        with open("/dev/urandom", "rb") as fh:
            return fh.read(len_)

    def test_get(self):
        inode = self.inode
        blockno = 11
        data = self.random_data(int(0.5 * self.cache.maxsize))

        # Case 1: Object does not exist yet
        with self.cache.get(inode, blockno, self.lock) as fh:
            fh.seek(0)
            fh.write(data)

        # Case 2: Object is in cache
        with self.cache.get(inode, blockno, self.lock) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))

        # Case 3: Object needs to be downloaded
        self.cache.clear()
        with self.cache.get(inode, blockno, self.lock) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))


    def test_flush(self):
        inode = self.inode
        blockno = 11
        data = self.random_data(int(0.5 * self.cache.maxsize))

        self.cache.bucket = TestBucket(self.bucket)
        self.assertEqual(len(self.cache), 0)
        with self.cache.get(inode, blockno, self.lock) as fh:
            fh.seek(0)
            fh.write(data)
        self.assertEqual(len(self.cache), 1)
        self.cache.flush(inode + 1)
        self.assertEqual(len(self.cache), 1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.flush(inode)
        self.assertEqual(len(self.cache), 1)
        self.cache.bucket.verify()

    def test_flush_all(self):
        inode = self.inode
        blockno = 11
        data = self.random_data(int(0.5 * self.cache.maxsize))

        self.cache.bucket = TestBucket(self.bucket)
        self.assertEqual(len(self.cache), 0)
        with self.cache.get(inode, blockno, self.lock) as fh:
            fh.seek(0)
            fh.write(data)
        self.assertEqual(len(self.cache), 1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.flush_all()
        self.assertEqual(len(self.cache), 1)
        self.cache.bucket.verify()

    def test_expire(self):
        inode = self.inode
        data1 = self.random_data(int(0.5 * self.blocksize))
        data2 = self.random_data(int(0.5 * self.blocksize))

        # This object will not be dirty
        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, 1, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.flush(inode)
        self.cache.bucket.verify()

        # This one will be
        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, 2, self.lock) as fh:
            fh.seek(0)
            fh.write(data2)

        self.assertEqual(len(self.cache), 2)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.clear()
        self.assertEquals(len(self.cache), 0)
        self.cache.bucket.verify()


    def test_prepare_upload(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.maxsize)
        blockno1 = 21
        blockno2 = 25
        blockno3 = 7

        data1 = self.random_data(datalen)
        data2 = self.random_data(datalen)
        data3 = self.random_data(datalen)
        
        queue = UploadQueue(self.cache)

        # Case 1: create new object
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
            el1 = fh
        queue._prepare_upload(el1)()
        self.cache.bucket.verify()

        # Case 2: Link new object
        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, blockno2, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
            el2 = fh
        self.assertIsNone(queue._prepare_upload(el2))

        # Case 3: Upload old object, still has references
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1, self.lock) as fh:
            fh.seek(0)
            fh.write(data2)
        queue._prepare_upload(el1)()
        self.cache.bucket.verify()

        # Case 4: Upload old object, no references left
        self.cache.bucket = TestBucket(self.bucket, no_del=1, no_store=1)
        with self.cache.get(inode, blockno2, self.lock) as fh:
            fh.seek(0)
            fh.write(data3)
        queue._prepare_upload(el2)()
        self.cache.bucket.verify()

        # Case 5: Link old object, no references left
        self.cache.bucket = TestBucket(self.bucket, no_del=1)
        with self.cache.get(inode, blockno2, self.lock) as fh:
            fh.seek(0)
            fh.write(data2)
        queue._prepare_upload(el2)()
        self.cache.bucket.verify()

        # Case 6: Link old object, still has references
        # (Need to create another object first)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno3, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
            el3 = fh
        queue._prepare_upload(el3)()
        self.cache.bucket.verify()

        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, blockno1, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
        self.assertIsNone(queue._prepare_upload(el1))
        self.cache.bucket.verify()


    def test_remove_referenced(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.maxsize)
        blockno1 = 21
        blockno2 = 24
        data = self.random_data(datalen)
        
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1, self.lock) as fh:
            fh.seek(0)
            fh.write(data)
        with self.cache.get(inode, blockno2, self.lock) as fh:
            fh.seek(0)
            fh.write(data)
        self.cache.clear()
        self.cache.bucket.verify()

        self.cache.bucket = TestBucket(self.bucket)
        self.cache.remove(inode, self.lock, blockno1)
        self.cache.bucket.verify()

    def test_remove_cache(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))

        # Case 1: Elements only in cache
        with self.cache.get(inode, 1, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.remove(inode, self.lock, 1)
        with self.cache.get(inode, 1, self.lock) as fh:
            fh.seek(0)
            self.assertTrue(fh.read(42) == '')

    def test_remove_cache_db(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))

        # Case 2: Element in cache and db 
        with self.cache.get(inode, 1, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.flush(inode)
        self.cache.bucket.verify()
        self.cache.bucket = TestBucket(self.bucket, no_del=1)
        self.cache.remove(inode, self.lock, 1)
        with self.cache.get(inode, 1, self.lock) as fh:
            fh.seek(0)
            self.assertTrue(fh.read(42) == '')

    def test_remove_db(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))

        # Case 3: Element only in DB
        with self.cache.get(inode, 1, self.lock) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.clear()
        self.cache.bucket.verify()
        self.cache.bucket = TestBucket(self.bucket, no_del=1)
        self.cache.remove(inode, self.lock, 1)
        with self.cache.get(inode, 1, self.lock) as fh:
            fh.seek(0)
            self.assertTrue(fh.read(42) == '')


class TestBucket(object):
    def __init__(self, bucket, no_fetch=0, no_store=0, no_del=0):
        self.no_fetch = no_fetch
        self.no_store = no_store
        self.no_del = no_del
        self.bucket = bucket

    def verify(self):
        if self.no_fetch != 0:
            raise RuntimeError('Got too few fetch calls')
        if self.no_store != 0:
            raise RuntimeError('Got too few store calls')
        if self.no_del != 0:
            raise RuntimeError('Got too few delete calls')

    def prep_store_fh(self, *a, **kw):
        # Lambda is required here
        #pylint: disable-msg=W0108
        return lambda: self.store_fh(*a, **kw)

    def store_fh(self, *a, **kw):
        self.no_store -= 1

        if self.no_store < 0:
            raise RuntimeError('Got too many store calls')

        return self.bucket.store_fh(*a, **kw)

    def fetch_fh(self, *a, **kw):
        self.no_fetch -= 1

        if self.no_fetch < 0:
            raise RuntimeError('Got too many fetch calls')

        return self.bucket.fetch_fh(*a, **kw)

    def delete(self, *a, **kw):
        self.no_del -= 1

        if self.no_del < 0:
            raise RuntimeError('Got too many delete calls')

        try:
            return self.bucket.delete(*a, **kw)
        except KeyError:
            # Don't count key errors
            self.no_del += 1
            raise


    def __delitem__(self, key):
        self.delete(key)

    def __iter__(self):
        return self.bucket.list()

    def  __contains__(self, key):
        return self.bucket.contains(key)

def suite():
    return unittest.makeSuite(cache_tests)

if __name__ == "__main__":
    unittest.main()
