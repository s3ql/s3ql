'''
t2_block_cache.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function

from s3ql.block_cache import BlockCache
from s3ql.backends import local
from s3ql.backends.common import NoSuchObject
from s3ql.common import create_tables, init_tables
from s3ql.database import Connection
import os
import tempfile
from _common import TestCase
import unittest2 as unittest
import stat
import time
import llfuse
import shutil

class cache_tests(TestCase):

    def setUp(self):

        self.bucket_dir = tempfile.mkdtemp()
        self.bucket = local.Connection().get_bucket(self.bucket_dir)

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

        self.cache = BlockCache(self.bucket, self.db, self.cachedir, 
                                100 * self.blocksize)
        self.cache.init()
        
        # Tested methods assume that they are called from
        # file system request handler
        llfuse.lock.acquire()
        
        # We do not want background threads
        self.cache.commit_thread.stop()


    def tearDown(self):
        self.cache.upload_manager.bucket = self.bucket
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
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_store=2)
        self.cache.expire()
        self.cache.upload_manager.join_all()
        self.cache.upload_manager.bucket.verify()
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
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
            el1 = fh
        mngr.add(el1)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket.verify()

        # Case 2: Link new object
        self.cache.upload_manager.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data1)
            el2 = fh
        mngr.add(el2)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket.verify()

        # Case 3: Upload old object, still has references
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data2)
        mngr.add(el1)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket.verify()


        # Case 4: Upload old object, no references left
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_del=1, no_store=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data3)
        mngr.add(el2)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket.verify()

        # Case 5: Link old object, no references left
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_del=1)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data2)
        mngr.add(el2)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket.verify()


        # Case 6: Link old object, still has references
        # (Need to create another object first)
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno3) as fh:
            fh.seek(0)
            fh.write(data1)
            el3 = fh
        mngr.add(el3)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket.verify()

        self.cache.upload_manager.bucket = TestBucket(self.bucket)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data1)
        mngr.add(el1)
        mngr.join_all()
        self.cache.removal_queue.join_all()
        self.cache.upload_manager.bucket.verify()



    def test_remove_referenced(self):
        inode = self.inode
        datalen = int(0.1 * self.cache.max_size)
        blockno1 = 21
        blockno2 = 24
        data = self.random_data(datalen)
        
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_store=1)
        with self.cache.get(inode, blockno1) as fh:
            fh.seek(0)
            fh.write(data)
        with self.cache.get(inode, blockno2) as fh:
            fh.seek(0)
            fh.write(data)
        self.cache.clear()
        self.cache.upload_manager.join_all()
        self.cache.upload_manager.bucket.verify()

        self.cache.upload_manager.bucket = TestBucket(self.bucket)
        self.cache.remove(inode, blockno1)
        self.cache.upload_manager.bucket.verify()

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
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_store=1)
        commit(self.cache, inode)
        self.cache.upload_manager.bucket.verify()
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_del=1)
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
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_store=1)
        self.cache.clear()
        self.cache.upload_manager.join_all()
        self.cache.upload_manager.bucket.verify()
        self.cache.upload_manager.bucket = TestBucket(self.bucket, no_del=1)
        self.cache.remove(inode, 1)
        with self.cache.get(inode, 1) as fh:
            fh.seek(0)
            self.assertTrue(fh.read(42) == '')


class TestBucket(object):
    def __init__(self, bucket, no_fetch=0, no_store=0, no_del=0):
        self.no_fetch = no_fetch
        self.no_store = no_store
        self.no_del = no_del
        self.bucket = bucket

    def read_after_create_consistent(self):
        return self.bucket.read_after_create_consistent()
    
    def read_after_write_consistent(self):
        return self.bucket.read_after_write_consistent()    
    
    def verify(self):
        if self.no_fetch != 0:
            raise RuntimeError('Got too few fetch calls')
        if self.no_store != 0:
            raise RuntimeError('Got too few store calls')
        if self.no_del != 0:
            raise RuntimeError('Got too few delete calls')

    def prep_store_fh(self, *a, **kw):
        (size, fn) = self.bucket.prep_store_fh(*a, **kw)
        def fn2():
            self.no_store -= 1
            if self.no_store < 0:
                raise RuntimeError('Got too many store calls')
            return fn()
        
        return (size, fn2)

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
        except NoSuchObject:
            # Don't count key errors
            self.no_del += 1
            raise


    def __delitem__(self, key):
        self.delete(key)

    def __iter__(self):
        return self.bucket.list()

    def  __contains__(self, key):
        return self.bucket.contains(key)


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
