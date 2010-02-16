'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

from s3ql import mkfs, s3cache
from s3ql.backends import local
from s3ql.database import ConnectionManager
import os
import tempfile
from _common import TestCase
import unittest
import stat
from time import time, sleep
import shutil

# Each test should correspond to exactly one function in the tested
# module, and testing should be done under the assumption that any
# other functions that are called by the tested function work perfectly.
class s3cache_tests(TestCase):

    def setUp(self):

        self.bucket_dir = tempfile.mkdtemp()
        self.passphrase = 'schnupp'
        self.bucket = local.Connection().get_bucket(self.bucket_dir, self.passphrase)

        self.dbfile = tempfile.NamedTemporaryFile()
        self.cachedir = tempfile.mkdtemp() + "/"
        self.blocksize = 1024
        cachesize = int(1.5 * self.blocksize)

        self.dbcm = ConnectionManager(self.dbfile.name)
        with self.dbcm() as conn:
            mkfs.setup_db(conn, self.blocksize)

        # Create an inode we can work with
        self.inode = 42
        self.dbcm.execute("INSERT INTO inodes (id, mode,uid,gid,mtime,atime,ctime,refcount, size) "
                   "VALUES (?,?,?,?,?,?,?,?,?)",
                   (self.inode, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), time(), time(), time(), 1, 32))

        self.cache = s3cache.S3Cache(self.bucket, self.cachedir, cachesize, self.dbcm)

    def tearDown(self):
        self.cache.clear()
        self.dbfile.close()
        sleep(local.LOCAL_PROP_DELAY * 1.1)
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
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            fh.write(data)

        # Case 2: Object is in cache
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))

        # Case 3: Object needs to be downloaded
        self.cache._expire_parallel()
        sleep(local.LOCAL_PROP_DELAY * 1.1)
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            self.assertEqual(data, fh.read(len(data)))


    def test_flush(self):
        inode = self.inode
        blockno = 11
        data = self.random_data(int(0.5 * self.cache.maxsize))

        self.cache.bucket = TestBucket(self.bucket)
        self.assertEqual(len(self.cache), 0)
        with self.cache.get(inode, blockno) as fh:
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
        with self.cache.get(inode, blockno) as fh:
            fh.seek(0)
            fh.write(data)
        self.assertEqual(len(self.cache), 1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.flush_all()
        self.assertEqual(len(self.cache), 1)
        self.cache.bucket.verify()

    def test_expire_parallel(self):
        inode = self.inode
        data1 = self.random_data(int(0.5 * self.blocksize))
        data2 = self.random_data(int(0.5 * self.blocksize))

        # This object will not be dirty
        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.flush(inode)
        self.cache.bucket.verify()

        # This one will be
        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, 2) as fh:
            fh.seek(0)
            fh.write(data2)

        self.assertEqual(len(self.cache), 2)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache._expire_parallel()
        self.assertEquals(len(self.cache), 0)
        self.cache.bucket.verify()

    def test_upload_object(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.maxsize)
        blockno1 = 21
        blockno2 = 25
        blockno3 = 7

        data1 = self.random_data(datalen)
        data2 = self.random_data(datalen)
        data3 = self.random_data(datalen)

        # Case 1: Upload new object
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
            el1 = fh
        self.cache._upload_object(el1)
        self.cache.bucket.verify()

        # Case 2: Link new object
        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data1)
            el2 = fh
        self.cache._upload_object(el2)

        # Case 3: Upload old object, still has references
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data2)
        self.cache._upload_object(el1)
        self.cache.bucket.verify()

        # Case 4: Upload old object, no references left
        self.cache.bucket = TestBucket(self.bucket, no_del=1, no_store=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data3)
        self.cache._upload_object(el2)
        self.cache.bucket.verify()

        # Case 5: Link old object, no references left
        self.cache.bucket = TestBucket(self.bucket, no_del=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data2)
        self.cache._upload_object(el2)
        self.cache.bucket.verify()

        # Case 6: Link old object, still has references
        # (Need to create another object first)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno3) as fh:
            fh.seek(0)
            fh.write(data1)
            el3 = fh
        self.cache._upload_object(el3)
        self.cache.bucket.verify()

        self.cache.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache._upload_object(el1)
        self.cache.bucket.verify()

    def test_recover(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))
        data2 = self.random_data(int(0.4 * self.blocksize))

        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        with self.cache.get(inode, 2) as fh:
            fh.seek(0)
            fh.write(data2)

        # Fake cache crash
        self.cache.cache.clear()
        self.cache = s3cache.S3Cache(self.bucket, self.cachedir, self.cache.maxsize, self.dbcm)
        self.cache.recover()

        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertTrue(data1 == fh.read(len(data1)))

        with self.cache.get(inode, 2) as fh:
            fh.seek(0)
            self.assertTrue(data2 == fh.read(len(data2)))

    def test_remove(self):
        inode = self.inode
        data1 = self.random_data(int(0.4 * self.blocksize))
        data2 = self.random_data(int(0.4 * self.blocksize))

        # Case 1: Elements only in cache
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        with self.cache.get(inode, 2) as fh:
            fh.seek(0)
            fh.write(data2)
        self.cache.bucket = TestBucket(self.bucket)
        self.cache.remove(inode)
        self.cache.bucket.verify()

        # Case 2: Elements in cache and db and not referenced
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        with self.cache.get(inode, 2) as fh:
            fh.seek(0)
            fh.write(data2)
        self.cache.bucket = TestBucket(self.bucket, no_store=2)
        self.cache.flush(inode)
        self.cache.bucket.verify()
        self.cache.bucket = TestBucket(self.bucket, no_del=2)
        # Key errors would cause multiple delete calls
        sleep(local.LOCAL_PROP_DELAY * 1.1)
        self.cache.remove(inode)
        self.cache.bucket.verify()

        # Case 3: Elements not in cache and still referenced
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            fh.write(data1)
        with self.cache.get(inode, 2) as fh:
            fh.seek(0)
            fh.write(data1)
        self.cache.bucket = TestBucket(self.bucket, no_store=1)
        self.cache._expire_parallel()
        self.cache.remove(inode, 2)
        self.cache.bucket.verify()


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
        return self.bucket.keys()

    def  __contains__(self, key):
        return self.bucket.contains(key)

def suite():
    return unittest.makeSuite(s3cache_tests)

if __name__ == "__main__":
    unittest.main()
